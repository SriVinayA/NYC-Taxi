# NYC Taxi Data Pipeline: Decompress Snappy files and upload to S3
# Purpose: Takes compressed .snz files, decompresses them, and stores in AWS S3

import os
import uuid
import snappy
import boto3
from io import BytesIO
from botocore.exceptions import ClientError

def get_mac_prefix() -> str:
    """Generate unique bucket prefix using MAC address to avoid naming conflicts"""
    return f"{uuid.getnode():012x}"

# Configuration: Where to find files and where to store them
LOCAL_DIR = "./"                    # Look for .snz files in current directory
BASE_BUCKET = "nyc-taxi"           # Base name for S3 bucket
PREFIX = "decompressed"            # Folder name inside S3 bucket
PREFERRED_REGION = "us-east-2"     # AWS region for cost/performance optimization

# AWS S3 client setup
session = boto3.session.Session(region_name=PREFERRED_REGION)
s3 = session.client("s3")
effective_region = s3.meta.region_name

# Create unique bucket name: {mac-address}-nyc-taxi
BUCKET = f"{get_mac_prefix()}-{BASE_BUCKET}".lower()

def bucket_exists_and_region(bucket: str):
    """Check if S3 bucket exists and return its region"""
    try:
        # Try to access the bucket
        s3.head_bucket(Bucket=bucket)
        loc = s3.get_bucket_location(Bucket=bucket).get("LocationConstraint")
        return True, (loc or "us-east-1")  # us-east-1 returns None for location
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        
        # Bucket doesn't exist
        if error_code in ("404", "NoSuchBucket", "NotFound"):
            return False, None
            
        # Bucket exists but in wrong region
        if error_code in ("301", "PermanentRedirect"):
            try:
                loc = s3.get_bucket_location(Bucket=bucket).get("LocationConstraint")
                return True, (loc or "us-east-1")
            except ClientError:
                return True, None
                
        # Bucket exists but no permission to access
        if error_code in ("403",):
            return True, None
            
        raise  # Some other unexpected error

def ensure_bucket(bucket: str, client_region: str):
    """Create S3 bucket if it doesn't exist, or verify region if it does"""
    exists, bucket_region = bucket_exists_and_region(bucket)
    
    if exists:
        # Bucket exists - check if regions match
        if bucket_region and bucket_region != client_region:
            raise RuntimeError(
                f"Bucket '{bucket}' exists in region '{bucket_region}', "
                f"but your client is using '{client_region}'. Use another name or region."
            )
        print(f"[INFO] Bucket {bucket} already exists (region: {bucket_region or client_region})")
        return

    # Create new bucket with proper region configuration
    if client_region == "us-east-1":
        # us-east-1 is special case - don't specify LocationConstraint
        s3.create_bucket(Bucket=bucket)
    else:
        # All other regions need explicit LocationConstraint
        s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": client_region},
        )
    print(f"[INFO] Created bucket: {bucket} in {client_region}")

# Initialize: Create or verify S3 bucket
print(f"[DEBUG] Effective client region: {effective_region}")
ensure_bucket(BUCKET, effective_region)

# Main processing loop: Find, decompress, and upload each .snz file
for filename in os.listdir(LOCAL_DIR):
    # Skip files that aren't compressed with snappy
    if not filename.endswith(".snz"):
        continue

    # Prepare file paths and S3 key
    in_path = os.path.join(LOCAL_DIR, filename)
    s3_key = f"{PREFIX}/{filename[:-4]}" if PREFIX else filename[:-4]  # Remove .snz extension

    # Process file: decompress in memory and upload to S3
    with open(in_path, "rb") as compressed_file:
        # Create in-memory buffer for decompressed data
        decompressed_buffer = BytesIO()
        
        # Decompress snappy stream directly to buffer
        snappy.stream_decompress(src=compressed_file, dst=decompressed_buffer)
        
        # Reset buffer position for reading
        decompressed_buffer.seek(0)
        
        # Upload decompressed data to S3
        s3.upload_fileobj(decompressed_buffer, BUCKET, s3_key)

    print(f"[INFO] Uploaded to s3://{BUCKET}/{s3_key}")