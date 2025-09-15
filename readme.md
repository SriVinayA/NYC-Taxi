# NYC Taxi Data Processing Project

A Python-based data processing pipeline for NYC taxi trip data, featuring Snappy compression/decompression and AWS S3 integration.

## Overview

This project provides tools to download, decompress, and process NYC taxi trip data from AWS public datasets. The data is stored in Snappy-compressed format (.snz files) and can be processed both locally and in the cloud using AWS S3.

## Features

- **Data Download**: Download NYC taxi trip data from AWS public bucket
- **Snappy Decompression**: Decompress .snz files locally or stream-decompress to S3
- **AWS S3 Integration**: Upload decompressed data to your own S3 bucket
- **MAC-based Bucket Naming**: Uses MAC address for unique S3 bucket naming
- **Cross-region Support**: Handle data transfer between different AWS regions

## Project Structure

```
├── taxi_event_reader.py          # Downloads .snz files from AWS public bucket
├── snappy_decompress.py          # Local decompression of .snz files
├── local_decompress_snz_s3.py    # Decompresses local .snz files and uploads to S3
├── s3_decompress_snz_s3.py       # Stream decompresses from source S3 to destination S3
├── part-00000.snz to part-00015.snz  # Downloaded NYC taxi data files
├── snappy_decompress/            # Directory containing decompressed files
│   ├── part-00000
│   ├── part-00001
│   └── ... (part-00002 to part-00015)
└── README.md
```

## Prerequisites

### Required Python Packages

```bash
pip install boto3 python-snappy
```

### AWS Configuration

Ensure you have AWS credentials configured:
- AWS CLI configured (`aws configure`)
- Or environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- Or IAM role (if running on EC2)

Required AWS permissions:
- S3 read access to `aws-bigdata-blog` bucket
- S3 full access to your destination bucket
- S3 bucket creation permissions

## Usage

### 1. Download NYC Taxi Data

```bash
python taxi_event_reader.py
```

Downloads Snappy-compressed NYC taxi trip data files from the AWS public dataset:
- **Source**: `s3://aws-bigdata-blog/artifacts/flink-refarch/data/nyc-tlc-trips.snz/`
- **Downloads**: 16 files (part-00000.snz to part-00015.snz)
- **Total Size**: Several GBs of compressed data

### 2. Local Decompression

```bash
python snappy_decompress.py
```

Decompresses .snz files locally:
- **Input**: .snz files in current directory
- **Output**: Decompressed files in `./snappy_decompress/` directory
- **Format**: Removes .snz extension from filenames
- **Note**: Opens Notepad on Windows and displays first 100 lines of each file

### 3. Local to S3 Upload (with decompression)

```bash
python local_decompress_snz_s3.py
```

Decompresses local .snz files and uploads to your S3 bucket:
- **Target Region**: us-east-2 (configurable)
- **Bucket Name**: `{mac-address}-nyc-taxi` (auto-generated)
- **Prefix**: `decompressed/`
- **Process**: Stream decompression → S3 upload

### 4. S3 to S3 Stream Processing

```bash
python s3_decompress_snz_s3.py
```

Stream decompresses from source S3 bucket to destination S3 bucket:
- **Source**: `s3://aws-bigdata-blog` (us-east-1)
- **Destination**: `s3://{mac-address}-nyc-taxi` (us-east-2)
- **Process**: Direct S3-to-S3 streaming with decompression
- **Advantages**: No local storage required, faster processing

## Configuration

### Regions and Buckets

The scripts use the following default configurations:

```python
# Source (NYC Data)
SRC_REGION = "us-east-1"
SRC_BUCKET = "aws-bigdata-blog"
SRC_PREFIX = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/"

# Destination (Your bucket)
DST_REGION = "us-east-2"  # Configurable
DST_BUCKET = f"{mac_address}-nyc-taxi"
DST_PREFIX = "decompressed"
```

### MAC Address-based Naming

The project uses your machine's MAC address to create unique S3 bucket names:
```python
def get_mac_prefix() -> str:
    return f"{uuid.getnode():012x}"
```

## Data Format

The NYC taxi trip data contains fields such as:
- Trip timestamps
- Pickup/dropoff locations
- Trip distances
- Fare amounts
- Payment types
- And more...

Each decompressed file contains multiple records in a structured format suitable for data analysis and processing.

## Error Handling

The scripts include robust error handling for:
- S3 bucket existence and region validation
- AWS credential issues
- Network connectivity problems
- File I/O errors
- Snappy decompression errors

## Performance Notes

- **Stream Processing**: `s3_decompress_snz_s3.py` is the most efficient for large datasets
- **Local Processing**: Use local scripts for smaller datasets or when local analysis is needed
- **Memory Usage**: Stream processing minimizes memory footprint
- **Network**: Cross-region transfers may incur additional AWS charges

## Troubleshooting

### Common Issues

1. **Bucket already exists in different region**
   - Error indicates bucket name collision
   - Solution: The MAC-based naming should prevent this

2. **AWS credentials not found**
   - Configure AWS CLI or set environment variables
   - Ensure proper IAM permissions

3. **Snappy decompression errors**
   - Verify file integrity
   - Check if files are fully downloaded

4. **S3 upload failures**
   - Check network connectivity
   - Verify S3 permissions
   - Ensure bucket exists and is accessible

## Contributing

When modifying the scripts:
1. Maintain error handling patterns
2. Update region configurations as needed
3. Test with small datasets first
4. Consider AWS costs for cross-region transfers

## License

This project is provided as-is for educational and development purposes.

## Data Source

NYC taxi trip data courtesy of:
- NYC Taxi and Limousine Commission (TLC)
- AWS Open Data Program
- Source bucket: `aws-bigdata-blog`
