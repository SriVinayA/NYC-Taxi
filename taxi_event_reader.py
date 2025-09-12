#!/usr/bin/env python3
"""
NYC Taxi Data Downloader

This module downloads compressed NYC taxi trip data files from an AWS S3 bucket.
The files are in Snappy compressed format (.snz) and contain taxi trip records.

Author: Vinay
Created: September 11, 2025
Last Modified: September 11, 2025
Version: 1.0
"""

import boto3
from boto3 import session
import os
from typing import Optional


def download_nyc_taxi_data(
    region: str = "us-east-1",
    bucket_name: str = "aws-bigdata-blog", 
    object_prefix: str = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/",
    download_path: Optional[str] = None
) -> None:
    """
    Download NYC taxi data files from AWS S3 bucket.
    
    Args:
        region (str): AWS region where the S3 bucket is located
        bucket_name (str): Name of the S3 bucket containing the data
        object_prefix (str): S3 object prefix to filter files
        download_path (str, optional): Local directory to download files to.
                                     If None, downloads to current directory.
    
    Returns:
        None
    
    Raises:
        boto3.exceptions.Boto3Error: If there's an error accessing S3
        FileNotFoundError: If the download path doesn't exist
    """
    # Create boto3 session for AWS S3 access
    session_ = session.Session()
    s3_resource = session_.resource("s3", region_name=region)
    bucket = s3_resource.Bucket(bucket_name)
    
    # Set download directory
    if download_path and not os.path.exists(download_path):
        os.makedirs(download_path)
    
    print(f"Starting download from s3://{bucket_name}/{object_prefix}")
    
    # Iterate through all objects with the specified prefix
    for obj in bucket.objects.filter(Prefix=object_prefix):
        # Extract filename from the full S3 key path
        target = obj.key.split("/")[-1]
        
        # Skip empty prefixes (directories)
        if target:
            # Determine full local path
            local_path = os.path.join(download_path, target) if download_path else target
            
            print(f"Downloading {obj.key} to {local_path}")
            
            try:
                # Download the file from S3 to local filesystem
                bucket.download_file(obj.key, local_path)
                print(f"✓ Successfully downloaded {obj.key}")
                print(f"  File size: {obj.size / (1024 * 1024):.2f} MB")
                
            except Exception as e:
                print(f"✗ Error downloading {obj.key}: {str(e)}")


def main() -> None:
    """
    Main execution function.
    Downloads NYC taxi data files from the specified S3 bucket.
    """
    try:
        download_nyc_taxi_data()
        print("\n🎉 All downloads completed successfully!")
        
    except Exception as e:
        print(f"❌ Error during download process: {str(e)}")


if __name__ == "__main__":
    main()