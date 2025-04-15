import base64
import sys
import asyncio
import functools
import gzip
import hashlib
import io
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime

import aiohttp
import backoff
import boto3
import botocore.exceptions
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

from arguments import Arguments
from config import REGION_NAME

# ---------------------------------------------------------------------------
# Logging Configuration (captures both stdout and stderr)
# ---------------------------------------------------------------------------
root_logger = logging.getLogger()
root_logger.setLevel(logging.DEBUG)

# Clear any pre-existing handlers
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

log_format = '%(asctime)s [%(levelname)s] [%(name)s] (%(module)s:%(lineno)d, %(funcName)s): %(message)s'

# Handler for INFO and below messages to stdout
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
stdout_formatter = logging.Formatter(log_format)
stdout_handler.setFormatter(stdout_formatter)
root_logger.addHandler(stdout_handler)

# Handler for ERROR and above messages to stderr
stderr_handler = logging.StreamHandler(sys.stderr)
stderr_handler.setLevel(logging.ERROR)
stderr_formatter = logging.Formatter(log_format)
stderr_handler.setFormatter(stderr_formatter)
root_logger.addHandler(stderr_handler)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
CREDENTIALS_PARAM_NAME = "google_access_key"
RETRY_TRIES = 3
RETRY_DELAY = 5
RETRY_BACKOFF = 3

CHUNK_SIZE = 1024 * 1024  # 1 MB
CHUNK_SIZE_FOR_MULTIPART = 5 * 1024 * 1024  # 5 MB
MULTIPART_THRESHOLD = 10 * 1024 * 1024  # 10 MB
STORAGE_CLASS = "INTELLIGENT_TIERING"


# ---------------------------------------------------------------------------
# Retry Decorator
# ---------------------------------------------------------------------------
def retry(exceptions, tries=RETRY_TRIES, delay=RETRY_DELAY, backoff=RETRY_BACKOFF):
    """
    Retry decorator for asynchronous functions.
    """

    def deco_retry(func):
        @functools.wraps(func)
        async def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    logger.warning(f"{str(e)}, Retrying in {mdelay} seconds...")
                    await asyncio.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            try:
                return await func(*args, **kwargs)
            except exceptions as e:
                logger.error(f"Retry limit exceeded. Last exception: {str(e)}")
                raise

        return f_retry

    return deco_retry


# ---------------------------------------------------------------------------
# Abstract Interfaces (SOLID: Dependency Inversion, Interface Segregation)
# ---------------------------------------------------------------------------
class IDriveService(ABC):
    @abstractmethod
    async def get_folder_id(self, folder_name: str) -> str:
        pass

    @abstractmethod
    async def get_spreadsheets(self, folder_id: str) -> list:
        pass

    @abstractmethod
    async def download_spreadsheet(self, file_id: str, file_name: str, mime_type: str) -> io.BytesIO:
        pass


class ICloudStorageUploader(ABC):
    @abstractmethod
    async def upload(self, file_obj: io.BytesIO, file_name: str):
        pass


class IParameterStore(ABC):
    @abstractmethod
    async def get_parameter(self, param_name: str) -> str:
        pass


# ---------------------------------------------------------------------------
# Utility: Compute SHA256 Checksum
# ---------------------------------------------------------------------------
def compute_sha256_checksum(file_obj: io.BytesIO) -> str:
    """
    Cmpute the SHA256 checksum using the full content
    obtained from file_obj.getvalue(). The result is base64–encoded.

    Note: This works well for in–memory streams like BytesIO. If the file is
    very large, you may need to use an iterative approach to avoid high memory usage.
    """
    file_obj.seek(0)
    # Get all the bytes from the file object at once.
    data = file_obj.getvalue()

    # Compute the SHA256 digest on the complete data.
    digest = hashlib.sha256(data).digest()

    # Reset the pointer for subsequent operations.
    file_obj.seek(0)

    # Return the base64-encoded result.
    return base64.b64encode(digest).decode('utf-8')


# ---------------------------------------------------------------------------
# Concrete Implementations
# ---------------------------------------------------------------------------
class GoogleDriveService(IDriveService):
    """
    Handles all operations related to Google Drive.
    """

    def __init__(self, session: aiohttp.ClientSession, credentials):
        self.credentials = credentials
        self.session = session
        self.service = build("drive", "v3", credentials=self.credentials)
        self.loop = asyncio.get_running_loop()
        logger.info("Google Drive Service initialized.")

    async def close(self):
        await self.session.close()
        logger.info("Google Drive session closed.")

    @retry(HttpError, tries=RETRY_TRIES, delay=RETRY_DELAY, backoff=RETRY_BACKOFF)
    async def get_folder_id(self, folder_name: str) -> str:
        logger.info(f"Searching folder: {folder_name}")
        query = f"mimeType='application/vnd.google-apps.folder' and name='{folder_name}' and trashed = false"
        request = self.service.files().list(q=query, fields="files(id)")
        results = await self.loop.run_in_executor(None, request.execute)
        items = results.get("files", [])
        if not items:
            raise ValueError(f"Folder not found: {folder_name}")
        folder_id = items[0]["id"]
        logger.info(f"Found folder ID: {folder_id}")
        return folder_id

    @retry(HttpError, tries=RETRY_TRIES, delay=RETRY_DELAY, backoff=RETRY_BACKOFF)
    async def get_spreadsheets(self, folder_id: str) -> list:
        logger.info(f"Retrieving spreadsheets from folder ID: {folder_id}")
        query = (f"'{folder_id}' in parents and "
                 "(mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' or "
                 "mimeType='application/vnd.google-apps.spreadsheet') and trashed = false")
        request = self.service.files().list(q=query, fields="files(id, name, mimeType)")
        results = await self.loop.run_in_executor(None, request.execute)
        spreadsheets = results.get("files", [])
        logger.info(f"Found spreadsheets: {spreadsheets}")
        return spreadsheets

    @retry(HttpError, tries=RETRY_TRIES, delay=RETRY_DELAY, backoff=RETRY_BACKOFF)
    async def download_spreadsheet(self, file_id: str, file_name: str, mime_type: str) -> io.BytesIO:
        logger.info(f"Downloading spreadsheet: {file_name} (ID: {file_id})")
        if mime_type == "application/vnd.google-apps.spreadsheet":
            request = self.service.files().export(
                fileId=file_id,
                mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            )
        else:
            request = self.service.files().get_media(fileId=file_id)

        byte_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(byte_stream, request, chunksize=CHUNK_SIZE)
        done = False
        while not done:
            status, done = downloader.next_chunk()
        logger.info(f"Downloaded {file_name} (size: {byte_stream.tell()} bytes)")
        return byte_stream


class S3Uploader(ICloudStorageUploader):
    """
    Handles S3 file uploads including compression and multipart uploads.
    Integrity verification is performed by comparing a locally computed SHA256 checksum
    with the value computed by S3 (returned as ChecksumSHA256).
    """

    def __init__(self, boto3_session, bucket_name: str):
        self.boto3_session = boto3_session
        self.bucket_name = bucket_name
        self.loop = asyncio.get_event_loop()
        logger.info("S3Uploader initialized.")

    @staticmethod
    def compress_file(file_obj: io.BytesIO) -> io.BytesIO:
        file_obj.seek(0)
        compressed_obj = io.BytesIO()
        with gzip.GzipFile(fileobj=compressed_obj, mode='w', compresslevel=9) as gz:
            gz.write(file_obj.read())
        compressed_obj.seek(0)
        return compressed_obj

    @retry((botocore.exceptions.BotoCoreError,), tries=RETRY_TRIES, delay=RETRY_DELAY, backoff=RETRY_BACKOFF)
    async def upload(self, file_obj: io.BytesIO, file_name: str):
        logger.info(f"Uploading file: {file_name}")
        # Compress the file.
        compressed_file = self.compress_file(file_obj)
        file_size = compressed_file.getbuffer().nbytes
        is_multipart = file_size >= MULTIPART_THRESHOLD
        compressed_file.seek(0)

        # Compute the SHA256 checksum for the compressed content.
        expected_checksum = compute_sha256_checksum(compressed_file)
        compressed_file.seek(0)

        s3 = self.boto3_session.client('s3')
        date_directory = datetime.utcnow().strftime('%Y/%m/%d')
        s3_key = f"{date_directory}/{file_name}.gz"

        # Set extra args so that S3 calculates the SHA256 checksum.
        # Passing 'ChecksumAlgorithm': 'SHA256' directs S3 to compute this checksum.
        extra_args = {
            "StorageClass": STORAGE_CLASS,
            "ChecksumAlgorithm": "SHA256"
        }

        if is_multipart:
            await self._multipart_upload(s3, compressed_file, s3_key, extra_args)
        else:
            await self._upload_fileobj(s3, compressed_file, s3_key, extra_args)

        await self.check_checksum(s3, s3_key, expected_checksum)
        logger.info(f"Uploaded {file_name} to bucket {self.bucket_name}")

    async def _upload_fileobj(self, s3, file_obj: io.BytesIO, s3_key: str, extra_args: dict):
        await self.loop.run_in_executor(
            None, s3.upload_fileobj, file_obj, self.bucket_name, s3_key, extra_args
        )

    async def _multipart_upload(self, s3, file_obj: io.BytesIO, s3_key: str, extra_args: dict):
        # Initiate multipart upload with the checksum algorithm.
        response = await self.loop.run_in_executor(
            None,
            lambda: s3.create_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                StorageClass=STORAGE_CLASS,
                ChecksumAlgorithm=extra_args.get("ChecksumAlgorithm")
            )
        )
        upload_id = response['UploadId']
        parts = []
        part_number = 0
        while chunk := file_obj.read(CHUNK_SIZE_FOR_MULTIPART):
            part_number += 1
            part_response = await self.loop.run_in_executor(
                None,
                lambda: s3.upload_part(
                    Bucket=self.bucket_name,
                    Key=s3_key,
                    PartNumber=part_number,
                    UploadId=upload_id,
                    Body=chunk
                )
            )
            parts.append({"PartNumber": part_number, "ETag": part_response["ETag"]})
        await self._complete_multipart_upload(s3, s3_key, upload_id, parts)

    async def _complete_multipart_upload(self, s3, s3_key: str, upload_id: str, parts: list):
        parts = sorted(parts, key=lambda part: part['PartNumber'])
        await self.loop.run_in_executor(
            None,
            lambda: s3.complete_multipart_upload(
                Bucket=self.bucket_name,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
        )

    @retry((botocore.exceptions.BotoCoreError,), tries=RETRY_TRIES, delay=RETRY_DELAY, backoff=RETRY_BACKOFF)
    async def check_checksum(self, s3, s3_key: str, expected_checksum: str):
        try:
            response = await self.loop.run_in_executor(
                None, lambda: s3.head_object(Bucket=self.bucket_name, Key=s3_key, ChecksumMode='ENABLED')
            )
            # When using ChecksumAlgorithm 'SHA256', S3 returns the computed checksum
            actual_checksum = response.get("ChecksumSHA256", "")
            if actual_checksum != expected_checksum:
                raise ValueError(
                    f"Checksum mismatch for {s3_key}. Expected: {expected_checksum}, got: {actual_checksum}")
        except botocore.exceptions.ClientError as e:
            error_code = e.response.get('Error', {}).get('Code')
            if error_code == '404':
                raise ValueError(f"File not found: {s3_key}")
            else:
                raise


class SSMParameterStore(IParameterStore):
    """
    Retrieves parameters (such as credentials) using AWS SSM.
    """

    def __init__(self, boto3_session):
        self.boto3_session = boto3_session

    @backoff.on_exception(backoff.expo, (botocore.exceptions.BotoCoreError,), max_tries=10, max_time=30)
    async def get_parameter(self, param_name: str) -> str:
        loop = asyncio.get_running_loop()
        ssm = self.boto3_session.client("ssm")
        response = await loop.run_in_executor(
            None,
            functools.partial(ssm.get_parameter, Name=param_name, WithDecryption=True)
        )
        return response["Parameter"]["Value"]


# ---------------------------------------------------------------------------
# Application Service (Spreadsheet Transfer Orchestration)
# ---------------------------------------------------------------------------
class SpreadsheetTransferService:
    """
    Orchestrates the download of spreadsheets from Google Drive and uploads them to S3.
    """

    def __init__(self, drive_service: IDriveService, uploader: ICloudStorageUploader):
        self.drive_service = drive_service
        self.uploader = uploader

    async def transfer_spreadsheets(self, folder_name: str):
        folder_id = await self.drive_service.get_folder_id(folder_name)
        spreadsheets = await self.drive_service.get_spreadsheets(folder_id)
        tasks = [self._process_spreadsheet(spreadsheet) for spreadsheet in spreadsheets]
        await asyncio.gather(*tasks)

    async def _process_spreadsheet(self, spreadsheet: dict):
        file_id = spreadsheet["id"]
        file_name = spreadsheet["name"]
        mime_type = spreadsheet["mimeType"]
        logger.info(f"Processing file: {file_name}")

        try:
            xlsx_file = await self.drive_service.download_spreadsheet(file_id, file_name, mime_type)
        except HttpError as e:
            if e.resp.status in [404]:
                logger.error(f"File not found: {file_name}")
            elif e.resp.status in [500, 502, 503, 504]:
                logger.error(f"Server error during download: {file_name}")
            else:
                logger.error(f"HTTP error for file {file_name}")
            raise

        object_name = f"{file_name}.xlsx"  # if mime_type == "application/vnd.google-apps.spreadsheet" else file_name

        try:
            await self.uploader.upload(xlsx_file, object_name)
        except botocore.exceptions.BotoCoreError as e:
            logger.error(f"Error uploading {file_name}: {e}")
            raise

        logger.info(f"Successfully processed and uploaded: {file_name}")


# ---------------------------------------------------------------------------
# Main Orchestration Function
# ---------------------------------------------------------------------------
async def main():
    boto3_session = boto3.Session(region_name=REGION_NAME)
    parameter_store: IParameterStore = SSMParameterStore(boto3_session)

    # Retrieve and parse credentials.
    credentials_data = await parameter_store.get_parameter(CREDENTIALS_PARAM_NAME)
    credentials_info = json.loads(credentials_data)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)

    args = Arguments()
    folder_name = args.parse("folder_name", required=True, help_text="Path to source folder")
    s3_bucket_name = args.parse("s3_bucket_name", required=True, help_text="Name of bucket for backup")

    async with aiohttp.ClientSession() as session:
        drive_service: IDriveService = GoogleDriveService(session, credentials)
        uploader: ICloudStorageUploader = S3Uploader(boto3_session, s3_bucket_name)
        transfer_service = SpreadsheetTransferService(drive_service, uploader)

        try:
            await transfer_service.transfer_spreadsheets(folder_name)
        except (botocore.exceptions.BotoCoreError, HttpError) as error:
            logger.error(f"An error occurred during transfer: {error}")
            raise


if __name__ == "__main__":
    asyncio.run(main())
