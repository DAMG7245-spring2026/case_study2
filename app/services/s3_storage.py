"""AWS S3 storage service."""
import logging
import mimetypes
from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from app.config import get_settings

logger = logging.getLogger(__name__)


class S3Storage:
    """Service for AWS S3 document storage."""
    
    def __init__(self):
        self.settings = get_settings()
        self._client = None
    
    @property
    def client(self):
        """Lazy initialization of S3 client."""
        if self._client is None:
            self._client = boto3.client(
                "s3",
                aws_access_key_id=self.settings.aws_access_key_id,
                aws_secret_access_key=self.settings.aws_secret_access_key,
                region_name=self.settings.aws_region,
            )
        return self._client
    
    @property
    def bucket(self) -> str:
        """Get configured bucket name."""
        return self.settings.s3_bucket
    
    async def health_check(self) -> tuple[bool, Optional[str]]:
        """Check if S3 connection is healthy."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            return True, None
        except NoCredentialsError:
            return False, "No AWS credentials configured"
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code == "404":
                return False, f"Bucket '{self.bucket}' not found"
            return False, str(e)
        except Exception as e:
            return False, str(e)
    
    def _s3_configured(self) -> bool:
        """Return True if S3 is configured (bucket and credentials)."""
        s = self.settings
        return bool(
            (s.aws_access_key_id or "").strip()
            and (s.aws_secret_access_key or "").strip()
            and (s.s3_bucket or "").strip()
        )

    def upload_document(
        self, 
        key: str, 
        content: bytes, 
        content_type: str = "application/octet-stream",
        metadata: Optional[dict] = None
    ) -> bool:
        """Upload a document to S3. Skips upload if S3 is not configured."""
        if not self._s3_configured():
            logger.debug("S3 upload skipped (AWS credentials or bucket not configured)")
            return False
        try:
            extra_args = {"ContentType": content_type}
            if metadata:
                extra_args["Metadata"] = {k: str(v) for k, v in metadata.items()}
            
            self.client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content,
                **extra_args
            )
            logger.info(f"Uploaded document: {key}")
            return True
        except ClientError as e:
            err = e.response.get("Error", {})
            if err.get("Code") == "SignatureDoesNotMatch":
                logger.error(
                    "Failed to upload %s: %s. Check AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY "
                    "(no trailing spaces/newlines), AWS_REGION, and S3 bucket permissions.",
                    key, e,
                )
            else:
                logger.error(f"Failed to upload {key}: {e}")
            return False
        except Exception as e:
            logger.error(f"Failed to upload {key}: {e}")
            return False
    
    def download_document(self, key: str) -> Optional[bytes]:
        """Download a document from S3."""
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=key)
            return response["Body"].read()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.warning(f"Document not found: {key}")
            else:
                logger.error(f"Failed to download {key}: {e}")
            return None
    
    def delete_document(self, key: str) -> bool:
        """Delete a document from S3."""
        try:
            self.client.delete_object(Bucket=self.bucket, Key=key)
            logger.info(f"Deleted document: {key}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete {key}: {e}")
            return False
    
    def list_documents(self, prefix: str = "") -> list[str]:
        """List documents with optional prefix filter."""
        try:
            response = self.client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=prefix
            )
            return [obj["Key"] for obj in response.get("Contents", [])]
        except Exception as e:
            logger.error(f"Failed to list documents: {e}")
            return []
    
    def generate_presigned_url(
        self, 
        key: str, 
        expiration: int = 3600
    ) -> Optional[str]:
        """Generate a presigned URL for document access."""
        try:
            url = self.client.generate_presigned_url(
                "get_object",
                Params={"Bucket": self.bucket, "Key": key},
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            logger.error(f"Failed to generate presigned URL for {key}: {e}")
            return None

    # ================================================================
    # CS2: SEC Filing Storage Methods
    # ================================================================

    def upload_sec_filing(
        self,
        ticker: str,
        filing_type: str,
        filing_date: str,
        local_path: Path,
        content_hash: Optional[str] = None
    ) -> Optional[str]:
        """
        Upload a SEC filing to S3.
        
        Args:
            ticker: Company ticker symbol
            filing_type: Filing type (10-K, 10-Q, 8-K)
            filing_date: Filing date (YYYY-MM-DD)
            local_path: Path to local file
            content_hash: Optional content hash for deduplication
            
        Returns:
            S3 key if successful, None otherwise
        """
        # Generate S3 key
        # Format: sec-filings/{ticker}/{filing_type}/{date}_{filename}
        filename = local_path.name
        s3_key = f"sec-filings/{ticker.upper()}/{filing_type}/{filing_date}_{filename}"
        
        # Determine content type
        content_type, _ = mimetypes.guess_type(str(local_path))
        if content_type is None:
            if local_path.suffix.lower() in ['.htm', '.html']:
                content_type = 'text/html'
            elif local_path.suffix.lower() == '.pdf':
                content_type = 'application/pdf'
            else:
                content_type = 'text/plain'
        
        # Read file content
        try:
            with open(local_path, 'rb') as f:
                content = f.read()
        except Exception as e:
            logger.error(f"Failed to read local file {local_path}: {e}")
            return None
        
        # Prepare metadata
        metadata = {
            "ticker": ticker.upper(),
            "filing_type": filing_type,
            "filing_date": filing_date,
            "original_filename": filename,
        }
        if content_hash:
            metadata["content_hash"] = content_hash
        
        # Upload to S3
        success = self.upload_document(
            key=s3_key,
            content=content,
            content_type=content_type,
            metadata=metadata
        )
        
        if success:
            logger.info(f"Uploaded SEC filing to S3: {s3_key}")
            return s3_key
        
        return None

    def upload_sec_filing_bytes(
        self,
        ticker: str,
        filing_type: str,
        filing_date: str,
        filename: str,
        content: bytes,
        content_type: str = "text/html",
        content_hash: Optional[str] = None
    ) -> Optional[str]:
        """
        Upload SEC filing content directly from bytes.
        
        Args:
            ticker: Company ticker symbol
            filing_type: Filing type (10-K, 10-Q, 8-K)
            filing_date: Filing date (YYYY-MM-DD)
            filename: Original filename
            content: File content as bytes
            content_type: MIME type
            content_hash: Optional content hash
            
        Returns:
            S3 key if successful, None otherwise
        """
        s3_key = f"sec-filings/{ticker.upper()}/{filing_type}/{filing_date}_{filename}"
        
        metadata = {
            "ticker": ticker.upper(),
            "filing_type": filing_type,
            "filing_date": filing_date,
            "original_filename": filename,
        }
        if content_hash:
            metadata["content_hash"] = content_hash
        
        success = self.upload_document(
            key=s3_key,
            content=content,
            content_type=content_type,
            metadata=metadata
        )
        
        return s3_key if success else None

    def get_sec_filing(self, s3_key: str) -> Optional[bytes]:
        """Download a SEC filing from S3."""
        return self.download_document(s3_key)

    def list_sec_filings(
        self,
        ticker: Optional[str] = None,
        filing_type: Optional[str] = None
    ) -> list[str]:
        """
        List SEC filings in S3.
        
        Args:
            ticker: Filter by ticker (optional)
            filing_type: Filter by filing type (optional)
            
        Returns:
            List of S3 keys
        """
        if ticker and filing_type:
            prefix = f"sec-filings/{ticker.upper()}/{filing_type}/"
        elif ticker:
            prefix = f"sec-filings/{ticker.upper()}/"
        else:
            prefix = "sec-filings/"
        
        return self.list_documents(prefix=prefix)

    def delete_sec_filing(self, s3_key: str) -> bool:
        """Delete a SEC filing from S3."""
        return self.delete_document(s3_key)

    def get_sec_filing_url(self, s3_key: str, expiration: int = 3600) -> Optional[str]:
        """Generate a presigned URL for a SEC filing."""
        return self.generate_presigned_url(s3_key, expiration)


# Singleton instance
_s3_storage: Optional[S3Storage] = None


def get_s3_storage() -> S3Storage:
    """Get or create S3 storage singleton."""
    global _s3_storage
    if _s3_storage is None:
        _s3_storage = S3Storage()
    return _s3_storage