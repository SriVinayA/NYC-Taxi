# NYC Taxi Data Processing Pipeline

This project provides a high-performance, Python-based data processing pipeline for NYC taxi trip data. It features multi-threaded download and decompression of Snappy-compressed files, seamless integration with AWS S3, and the ability to populate AWS Kinesis Data Streams for real-time analytics.

## Overview

This project provides a suite of tools to download, decompress, and process NYC taxi trip data from a public AWS dataset. The data is stored in Snappy-compressed format (`.snz` files) and can be processed locally or streamed directly to AWS services. The multi-threaded architecture allows for efficient handling of large datasets.

## Features

- **Multi-threaded Processing**: Concurrently download and decompress multiple files for significantly faster processing.
- **AWS Kinesis Integration**: Ingest processed taxi data directly into an AWS Kinesis Data Stream for real-time analysis.
- **Snappy Decompression**: Efficiently decompress `.snz` files.
- **AWS S3 Integration**: Handles data transfer from a source S3 bucket to a local directory or another S3 bucket.
- **Automatic Kinesis Stream Management**: Automatically creates and configures Kinesis streams with appropriate sharding.
- **Flexible Configuration**: Easily configure AWS regions, Kinesis stream names, and other parameters.

## Project Structure

```
├── Taxi_Event_Reader_MT_Kinesis.py # Multi-threaded reader with Kinesis integration
├── Kinesis_Populator.py            # Populates a Kinesis stream from a local file
├── taxi_event_reader.py            # Single-threaded script to download .snz files
├── snappy_decompress.py            # Local decompression of .snz files
├── local_decompress_snz_s3.py      # Decompresses local .snz files and uploads to S3
├── s3_decompress_snz_s3.py         # Stream decompresses from a source S3 to a destination S3
├── snappy_decompressed_events/     # Directory for decompressed NDJSON files
└── README.md
```

## Prerequisites

### Required Python Packages

```bash
pip install boto3 python-snappy
```

### AWS Configuration

Ensure you have AWS credentials configured via one of the following methods:
- AWS CLI (`aws configure`)
- Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
- IAM role (if running on an EC2 instance)

You will need the following AWS permissions:
- S3 read access to the `aws-bigdata-blog` bucket.
- Full S3 access to your destination bucket (if used).
- Permissions to create and write to Kinesis Data Streams.

## Usage

### 1. Multi-threaded Processing with Kinesis Integration

The primary script for most use cases is `Taxi_Event_Reader_MT_Kinesis.py`. It handles multi-threaded download, decompression, and optional upload to Kinesis.

**To download and decompress locally:**

```bash
python Taxi_Event_Reader_MT_Kinesis.py --region us-east-1
```

**To download, decompress, and populate a Kinesis stream:**

```bash
python Taxi_Event_Reader_MT_Kinesis.py --region us-east-1 --stream your-kinesis-stream-name
```

### 2. Populating Kinesis from a Local File

If you have already decompressed the data to a local NDJSON file, you can use `Kinesis_Populator.py` to upload it to a Kinesis stream.

```bash
python Kinesis_Populator.py --file ./snappy_decompressed_events/part-00000.ndjson --stream your-kinesis-stream-name --region us-east-1
```

### 3. Single-threaded and Legacy Scripts

The following scripts are also available for single-threaded operations or specific use cases:

-   `taxi_event_reader.py`: Downloads Snappy-compressed files from the public S3 bucket.
-   `snappy_decompress.py`: Decompresses `.snz` files locally.
-   `local_decompress_snz_s3.py`: Decompresses local `.snz` files and uploads them to an S3 bucket.
-   `s3_decompress_snz_s3.py`: Stream-decompresses files from a source S3 bucket to a destination S3 bucket.

## Configuration

The scripts can be configured using command-line arguments. Key options in `Taxi_Event_Reader_MT_Kinesis.py` include:

-   `--region`: The AWS region for S3 and Kinesis operations.
-   `--stream`: The name of the Kinesis stream to populate. If omitted, Kinesis upload is disabled.
-   `--profile`: The AWS CLI profile to use for authentication.

## Data Format

The NYC taxi trip data is processed into NDJSON format, with each line representing a single trip event. The data includes fields such as trip timestamps, pickup/dropoff locations, trip distances, and fare amounts.

## Error Handling

The scripts include robust error handling for:
- S3 bucket and object access
- AWS credential and permission issues
- Network connectivity problems
- Kinesis stream creation and data ingestion, including throttling
- Snappy decompression errors

## Performance

-   **Multi-threaded Processing**: The `Taxi_Event_Reader_MT_Kinesis.py` script provides the best performance for processing multiple files.
-   **Kinesis Integration**: The scripts manage Kinesis shards and throughput to ensure reliable data ingestion.
-   **Memory Usage**: Stream-based processing minimizes the memory footprint.
