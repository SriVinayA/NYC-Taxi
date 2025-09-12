#!/usr/bin/env python3
"""
Snappy Decompression to S3 Uploader

This module provides functionality to decompress Snappy compressed files
and upload the decompressed data directly to Amazon S3 without storing
intermediate files locally. This is memory-efficient for processing large
compressed datasets.

Author: Vinay
Created: September 11, 2025
Last Modified: September 11, 2025
Version: 1.0

Dependencies:
    - boto3: pip install boto3
    - python-snappy: pip install python-snappy
"""

import io
import snappy
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from typing import Optional, Dict, Any


class SnappyToS3Processor:
    """
    A class to handle decompression of Snappy files and upload to S3.
    
    Attributes:
        s3_client: Boto3 S3 client instance
        bucket_name: Target S3 bucket name
        region: AWS region for S3 operations
    """
    
    def __init__(self, bucket_name: str, region: str = "us-east-1"):
        """
        Initialize the processor with S3 configuration.
        
        Args:
            bucket_name (str): Name of the target S3 bucket
            region (str): AWS region for S3 operations
            
        Raises:
            NoCredentialsError: If AWS credentials are not configured
            ClientError: If there's an error creating the S3 client
        """
        self.bucket_name = bucket_name
        self.region = region
        
        try:
            self.s3_client = boto3.client('s3', region_name=region)
            print(f"✅ Connected to S3 in region: {region}")
            
        except NoCredentialsError:
            raise NoCredentialsError(
                "AWS credentials not found. Please configure your credentials."
            )
    
    def decompress_and_upload(
        self,
        snappy_file_path: str,
        s3_object_key: str,
        metadata: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Decompress a Snappy file and upload the decompressed data to S3.
        
        Args:
            snappy_file_path (str): Path to the local Snappy compressed file
            s3_object_key (str): S3 object key for the uploaded file
            metadata (dict, optional): Additional metadata for the S3 object
            
        Returns:
            bool: True if successful, False otherwise
            
        Raises:
            FileNotFoundError: If the source file doesn't exist
            snappy.UncompressError: If decompression fails
            ClientError: If S3 upload fails
        """
        print(f"🔧 Processing: {snappy_file_path}")
        
        try:
            # Open the Snappy compressed file
            with open(snappy_file_path, "rb") as snappy_file:
                print("📦 Decompressing data in memory...")
                
                # Create in-memory buffer for decompressed data
                decompressed_data = io.BytesIO()
                
                # Decompress directly to memory buffer
                snappy.stream_decompress(src=snappy_file, dst=decompressed_data)
                
                # Reset buffer position to beginning for reading
                decompressed_data.seek(0)
                
                # Get the size of decompressed data
                decompressed_size = len(decompressed_data.getvalue())
                print(f"📊 Decompressed size: {decompressed_size / (1024 * 1024):.2f} MB")
                
                # Prepare metadata
                upload_metadata = {
                    'source-file': snappy_file_path,
                    'compression': 'snappy',
                    'decompressed-size': str(decompressed_size)
                }
                
                if metadata:
                    upload_metadata.update(metadata)
                
                # Upload to S3
                print(f"☁️  Uploading to s3://{self.bucket_name}/{s3_object_key}")
                
                self.s3_client.upload_fileobj(
                    decompressed_data,
                    self.bucket_name,
                    s3_object_key,
                    ExtraArgs={'Metadata': upload_metadata}
                )
                
                print(f"✅ Successfully uploaded to S3: {s3_object_key}")
                return True
                
        except FileNotFoundError:
            print(f"❌ Source file not found: {snappy_file_path}")
            return False
            
        except snappy.UncompressError as e:
            print(f"❌ Decompression failed: {str(e)}")
            return False
            
        except ClientError as e:
            print(f"❌ S3 upload failed: {str(e)}")
            return False
            
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
            return False
    
    def batch_process(
        self,
        file_mappings: Dict[str, str],
        metadata: Optional[Dict[str, str]] = None
    ) -> Dict[str, bool]:
        """
        Process multiple Snappy files and upload to S3.
        
        Args:
            file_mappings (dict): Mapping of local file paths to S3 object keys
            metadata (dict, optional): Common metadata for all uploads
            
        Returns:
            dict: Results of each file processing (True/False for success/failure)
        """
        results = {}
        total_files = len(file_mappings)
        
        print(f"🚀 Starting batch processing of {total_files} files")
        print("=" * 60)
        
        for i, (local_path, s3_key) in enumerate(file_mappings.items(), 1):
            print(f"\n📝 Processing file {i}/{total_files}")
            
            success = self.decompress_and_upload(
                snappy_file_path=local_path,
                s3_object_key=s3_key,
                metadata=metadata
            )
            
            results[local_path] = success
            
        # Print summary
        successful = sum(results.values())
        failed = total_files - successful
        
        print("\n" + "=" * 60)
        print(f"📊 Batch processing complete:")
        print(f"   ✅ Successful: {successful}")
        print(f"   ❌ Failed: {failed}")
        
        return results


def main() -> None:
    """
    Main execution function demonstrating usage.
    """
    # Configuration
    BUCKET_NAME = "your-s3-bucket-name"
    REGION = "us-east-1"
    
    # Example file mappings (local_path -> s3_object_key)
    file_mappings = {
        "data/taxi_data_1.snz": "decompressed/taxi_data_1.txt",
        "data/taxi_data_2.snz": "decompressed/taxi_data_2.txt",
    }
    
    # Common metadata for all uploads
    common_metadata = {
        "project": "nyc-taxi-analysis",
        "processed-by": "snappy-to-s3-processor",
        "version": "1.0"
    }
    
    try:
        # Initialize processor
        processor = SnappyToS3Processor(BUCKET_NAME, REGION)
        
        # Process files
        results = processor.batch_process(file_mappings, common_metadata)
        
        # Handle results as needed
        for file_path, success in results.items():
            if not success:
                print(f"⚠️  Consider retrying failed file: {file_path}")
                
    except Exception as e:
        print(f"❌ Error in main execution: {str(e)}")


# Example usage for single file processing
def example_single_file():
    """
    Example of processing a single file.
    """
    # Example commented code that was in the original file
    # This demonstrates the core decompression logic:
    
    # decompressed_data = io.BytesIO()
    # snappy.stream_decompress(src=snappyfile, dst=decompressed_data)
    # decompressed_data.seek(0)
    # # store the decompressed data to amazon s3
    
    pass


if __name__ == "__main__":
    main()