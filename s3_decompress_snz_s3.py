# NYC Taxi Data Pipeline: Download from AWS public dataset, decompress, and upload to personal S3
# Purpose: Fetches compressed .snz files from AWS public bucket, decompresses them, stores in your S3

import uuid
import snappy
import boto3
import io
from botocore.exceptions import ClientError

# Source configuration: AWS public NYC taxi dataset
SRC_REGION = "us-east-1"                                               # AWS public data region
SRC_BUCKET = "aws-bigdata-blog"                                       # AWS public bucket
SRC_PREFIX = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/"       # Path to compressed files

# Source S3 client for downloading from public dataset
src_s3 = boto3.client("s3", region_name=SRC_REGION)

# Destination configuration: Your personal S3 storage
DST_REGION = "us-east-2"                                              # Your preferred region
session_dst = boto3.session.Session(region_name=DST_REGION)
dst_s3 = session_dst.client("s3")
effective_region = dst_s3.meta.region_name

def get_mac_prefix() -> str:
    """Generate unique bucket prefix using MAC address to avoid naming conflicts"""
    return f"{uuid.getnode():012x}"

# Create unique destination bucket name and folder structure
DST_BUCKET = f"{get_mac_prefix()}-nyc-taxi".lower()                   # Your unique bucket
DST_PREFIX = "decompressed"                                           # Folder for processed files

def bucket_exists_and_region(bucket: str):
    """Check if destination S3 bucket exists and return its region"""
    try:
        # Try to access the bucket
        dst_s3.head_bucket(Bucket=bucket)
        loc = dst_s3.get_bucket_location(Bucket=bucket).get("LocationConstraint")
        return True, (loc or "us-east-1")  # us-east-1 returns None for location
    except ClientError as e:
        error_code = e.response.get("Error", {}).get("Code", "")
        
        # Bucket doesn't exist
        if error_code in ("404", "NoSuchBucket", "NotFound"):
            return False, None
            
        # Bucket exists but in wrong region
        if error_code in ("301", "PermanentRedirect"):
            try:
                loc = dst_s3.get_bucket_location(Bucket=bucket).get("LocationConstraint")
                return True, (loc or "us-east-1")
            except ClientError:
                return True, None
                
        # Bucket exists but no permission to access
        if error_code in ("403",):
            return True, None
            
        raise  # Some other unexpected error

def ensure_bucket(bucket: str, client_region: str):
    """Create destination S3 bucket if it doesn't exist, or verify region if it does"""
    exists, bucket_region = bucket_exists_and_region(bucket)
    
    if exists:
        # Bucket exists - check if regions match
        if bucket_region and bucket_region != client_region:
            raise RuntimeError(
                f"Bucket '{bucket}' exists in '{bucket_region}', "
                f"but client region is '{client_region}'."
            )
        print(f"[INFO] Bucket {bucket} already exists (region: {bucket_region or client_region})")
        return
        
    # Create new bucket with proper region configuration
    if client_region == "us-east-1":
        # us-east-1 is special case - don't specify LocationConstraint
        dst_s3.create_bucket(Bucket=bucket)
    else:
        # All other regions need explicit LocationConstraint
        dst_s3.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": client_region},
        )
    print(f"[INFO] Created bucket: {bucket} in {client_region}")

# Initialize: Create or verify destination S3 bucket
print(f"[DEBUG] Destination client region: {effective_region}")
ensure_bucket(DST_BUCKET, effective_region)

# Main processing: Discover and process all .snz files from public dataset
paginator = src_s3.get_paginator("list_objects_v2")
pages = paginator.paginate(Bucket=SRC_BUCKET, Prefix=SRC_PREFIX)

for page in pages:
    for item in page.get("Contents", []):
        key = item["Key"]
        
        # Skip folder placeholders and non-compressed files
        if key.endswith("/") or not key.endswith(".snz"):
            continue

        # Download compressed file from public S3 bucket
        response = src_s3.get_object(Bucket=SRC_BUCKET, Key=key)
        compressed_stream = response["Body"]  # File-like streaming object
        file_size = response.get("ContentLength", 0)

        # Prepare destination filename and S3 key
        filename = key.rsplit("/", 1)[-1]  # Extract filename from full path
        processed_filename = filename.replace(".snz", ".txt")  # Change extension to .txt
        dst_key = f"{DST_PREFIX}/{filename}" if DST_PREFIX else filename

        # Stream processing: Download -> Decompress -> Upload (all in memory)
        decompressed_buffer = io.BytesIO()
        
        # Decompress directly from S3 stream to memory buffer
        snappy.stream_decompress(src=compressed_stream, dst=decompressed_buffer)
        
        # Reset buffer position for uploading
        decompressed_buffer.seek(0)
        
        # Upload decompressed data to your S3 bucket
        dst_s3.upload_fileobj(decompressed_buffer, DST_BUCKET, dst_key)

        print(f"[INFO] {key} ({file_size} bytes) -> s3://{DST_BUCKET}/{dst_key}")