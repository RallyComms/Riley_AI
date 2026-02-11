"""Google Cloud Storage service for file uploads and management."""

import logging
import os
from datetime import timedelta
from typing import List, Optional
from urllib.parse import urlparse

import google.auth
import google.auth.transport.requests
from fastapi import HTTPException, UploadFile
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

from app.core.config import get_settings

# Configure logging
logger = logging.getLogger(__name__)


class StorageService:
    """Service for managing file uploads to Google Cloud Storage."""

    _client: Optional[storage.Client] = None

    @classmethod
    def _get_client(cls) -> storage.Client:
        """Get or create GCS client instance."""
        if cls._client is None:
            settings = get_settings()
            
            # Initialize client with credentials if provided
            if settings.GOOGLE_APPLICATION_CREDENTIALS:
                # Set environment variable for google-cloud-storage to use
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = settings.GOOGLE_APPLICATION_CREDENTIALS
                cls._client = storage.Client()
            else:
                # Try to use default credentials (e.g., in GCP environment)
                try:
                    cls._client = storage.Client()
                except Exception as exc:
                    raise RuntimeError(
                        "GCS credentials not configured. Set GOOGLE_APPLICATION_CREDENTIALS "
                        "or ensure default credentials are available."
                    ) from exc
        
        return cls._client

    @classmethod
    async def upload_file(cls, file: UploadFile, filename: str) -> str:
        """Upload a file to GCS and return the public URL.
        
        Args:
            file: The uploaded file object
            filename: The filename to use in GCS (should be unique)
            
        Returns:
            Public URL to access the file (e.g., https://storage.googleapis.com/...)
            
        Raises:
            HTTPException: If upload fails
        """
        settings = get_settings()
        client = cls._get_client()
        bucket = client.bucket(settings.GCS_BUCKET_NAME)
        
        try:
            # Create blob
            blob = bucket.blob(filename)
            
            # Reset file pointer to beginning to ensure we read it all, 
            # in case it was read previously for text extraction.
            await file.seek(0)
            
            # Read file content
            file_content = await file.read()
            
            # Upload to GCS
            blob.upload_from_string(
                file_content,
                content_type=file.content_type or "application/octet-stream"
            )
            
            # REMOVED: blob.make_public() 
            # Reason: Buckets with 'Uniform Bucket-Level Access' do not allow per-file ACLs.
            # Access must be granted via the Google Cloud Console (IAM -> Grant 'allUsers' role 'Storage Object Viewer').
            
            # Return public URL
            return blob.public_url
            
        except GoogleCloudError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to upload file to GCS: {exc}"
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error during file upload: {exc}"
            ) from exc

    @classmethod
    async def upload_bytes(
        cls,
        object_name: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        """Upload raw bytes to GCS and return the public URL."""
        settings = get_settings()
        client = cls._get_client()
        bucket = client.bucket(settings.GCS_BUCKET_NAME)

        try:
            blob = bucket.blob(object_name)
            blob.upload_from_string(data, content_type=content_type)
            return blob.public_url
        except GoogleCloudError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to upload bytes to GCS: {exc}",
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error during bytes upload: {exc}",
            ) from exc

    @classmethod
    async def generate_signed_url(cls, object_name: str, ttl_seconds: int) -> str:
        """Generate a V4 signed URL for a GCS object using IAM-based signing.
        
        Uses token-based credentials (suitable for Cloud Run) instead of private key files.
        Requires SIGNING_SERVICE_ACCOUNT_EMAIL to be set in environment or config.
        
        Args:
            object_name: The GCS object name (path within bucket)
            ttl_seconds: Time-to-live for the signed URL in seconds
            
        Returns:
            Signed URL string
            
        Raises:
            HTTPException: If signing fails or service account email is not configured
        """
        settings = get_settings()
        client = cls._get_client()
        bucket = client.bucket(settings.GCS_BUCKET_NAME)
        blob = bucket.blob(object_name)

        try:
            # Get default credentials and project
            credentials, project = google.auth.default()
            
            # Refresh credentials to ensure token is available
            request = google.auth.transport.requests.Request()
            credentials.refresh(request)
            
            # Determine service account email for signing
            # Priority: 1) env var, 2) credentials.service_account_email, 3) raise error
            service_account_email = settings.SIGNING_SERVICE_ACCOUNT_EMAIL
            
            if not service_account_email:
                # Try to get from credentials if available
                if hasattr(credentials, 'service_account_email') and credentials.service_account_email:
                    service_account_email = credentials.service_account_email
                    logger.info(
                        f"Using service account email from credentials: {service_account_email[:10]}..."
                    )
            
            if not service_account_email:
                error_msg = (
                    "SIGNING_SERVICE_ACCOUNT_EMAIL not configured. "
                    "Set SIGNING_SERVICE_ACCOUNT_EMAIL environment variable to the service account "
                    "email that has 'Service Account Token Creator' role for IAM-based signed URL generation."
                )
                logger.error(f"Signed URL generation failed for object '{object_name}': {error_msg}")
                raise HTTPException(
                    status_code=500,
                    detail=error_msg,
                )
            
            # Generate V4 signed URL using IAM signBlob API
            # This works with token-based credentials (Cloud Run, GCE, etc.)
            url = blob.generate_signed_url(
                version="v4",
                expiration=timedelta(seconds=ttl_seconds),
                method="GET",
                service_account_email=service_account_email,
                access_token=credentials.token,
            )
            
            logger.debug(
                f"Generated signed URL for object '{object_name}' "
                f"(TTL: {ttl_seconds}s, SA: {service_account_email[:10]}...)"
            )
            
            return url
            
        except HTTPException:
            # Re-raise HTTP exceptions as-is
            raise
        except GoogleCloudError as exc:
            error_msg = f"GCS error generating signed URL for object '{object_name}': {type(exc).__name__}"
            logger.error(f"{error_msg}: {str(exc)}")
            raise HTTPException(
                status_code=500,
                detail=error_msg,
            ) from exc
        except Exception as exc:
            error_msg = f"Unexpected error generating signed URL for object '{object_name}': {type(exc).__name__}"
            logger.error(f"{error_msg}: {str(exc)}")
            raise HTTPException(
                status_code=500,
                detail=error_msg,
            ) from exc

    @classmethod
    async def download_file(cls, file_url: str) -> bytes:
        """Download a file from GCS and return its content as bytes.
        
        Args:
            file_url: The public URL of the file in GCS
            
        Returns:
            File content as bytes
            
        Raises:
            HTTPException: If download fails
        """
        settings = get_settings()
        client = cls._get_client()
        
        try:
            # Parse the GCS URL to extract bucket and blob name
            # Format: https://storage.googleapis.com/bucket-name/blob-name
            # or: gs://bucket-name/blob-name
            from urllib.parse import urlparse
            
            parsed = urlparse(file_url)
            
            if parsed.scheme == "gs":
                # gs://bucket/path format
                bucket_name = parsed.netloc
                blob_name = parsed.path.lstrip("/")
            else:
                # https://storage.googleapis.com/bucket/path format
                # Extract bucket name from hostname or path
                if "storage.googleapis.com" in parsed.netloc:
                    # Path format: /bucket-name/blob-path
                    path_parts = parsed.path.lstrip("/").split("/", 1)
                    bucket_name = path_parts[0]
                    blob_name = path_parts[1] if len(path_parts) > 1 else ""
                else:
                    raise ValueError(f"Unsupported URL format: {file_url}")
            
            bucket = client.bucket(bucket_name)
            blob = bucket.blob(blob_name)
            
            # Download blob content
            file_content = blob.download_as_bytes()
            
            return file_content
            
        except GoogleCloudError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to download file from GCS: {exc}"
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error during file download: {exc}"
            ) from exc

    @classmethod
    async def delete_file(cls, file_url: str) -> None:
        """Delete a file from GCS by parsing the URL.
        
        Args:
            file_url: The public URL of the file to delete
            (e.g., https://storage.googleapis.com/bucket-name/filename.pdf)
            
        Raises:
            HTTPException: If deletion fails or file not found
        """
        settings = get_settings()
        client = cls._get_client()
        bucket = client.bucket(settings.GCS_BUCKET_NAME)
        
        try:
            # Parse filename from URL
            # URL format: https://storage.googleapis.com/bucket-name/filename.pdf
            parsed_url = urlparse(file_url)
            # Extract filename from path (remove leading slash)
            filename = parsed_url.path.lstrip("/")
            
            # Remove bucket name prefix if present in path
            if filename.startswith(settings.GCS_BUCKET_NAME + "/"):
                filename = filename[len(settings.GCS_BUCKET_NAME) + 1:]
            
            # Get blob and delete
            blob = bucket.blob(filename)
            
            if not blob.exists():
                raise HTTPException(
                    status_code=404,
                    detail=f"File not found in GCS: {filename}"
                )
            
            blob.delete()
            
        except HTTPException:
            raise
        except GoogleCloudError as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to delete file from GCS: {exc}"
            ) from exc
        except Exception as exc:
            raise HTTPException(
                status_code=500,
                detail=f"Unexpected error during file deletion: {exc}"
            ) from exc

    @classmethod
    async def delete_batch(cls, file_urls: List[str]) -> None:
        """Delete multiple files from GCS in batch.
        
        Args:
            file_urls: List of public URLs of files to delete
            
        Raises:
            HTTPException: If any deletion fails (but continues with others)
        """
        errors: List[str] = []
        
        for file_url in file_urls:
            try:
                await cls.delete_file(file_url)
            except HTTPException as exc:
                # Collect errors but continue with other files
                errors.append(f"Failed to delete {file_url}: {exc.detail}")
            except Exception as exc:
                errors.append(f"Unexpected error deleting {file_url}: {str(exc)}")
        
        # If any errors occurred, raise an exception with all errors
        if errors:
            raise HTTPException(
                status_code=500,
                detail=f"Some files failed to delete: {'; '.join(errors)}"
            )