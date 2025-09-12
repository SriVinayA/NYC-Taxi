# NYC Taxi Data Processing Pipeline

A Python-based toolkit for downloading, decompressing, and processing NYC taxi trip data from AWS S3. This project handles Snappy-compressed (.snz) files containing taxi trip records and provides utilities for local processing and cloud storage.

## 🚀 Features

- **Download NYC taxi data** from AWS S3 public datasets
- **Decompress Snappy files** efficiently 
- **Upload decompressed data** directly to S3 without local storage
- **Preview and analyze** decompressed content
- **Batch processing** capabilities for multiple files
- **Memory-efficient** streaming operations

## 📁 Project Structure

```
Nyc-Taxi/
├── .gitignore                 # Git ignore patterns
├── taxi_event_reader.py       # Downloads .snz files from S3
├── snappy_decompress.py       # Decompresses files locally
├── decompressed_to_s3.py     # Decompresses and uploads to S3
└── README.md                 # This file
```

## 🛠️ Installation

### Prerequisites

- Python 3.7+
- AWS CLI configured (for S3 operations)

### Dependencies

Install required packages:

```bash
pip install boto3 python-snappy
```

For Ubuntu/Debian systems, you may need to install system dependencies:

```bash
sudo apt-get install libsnappy-dev
```

For macOS:

```bash
brew install snappy
```

## 📖 Usage

### 1. Download NYC Taxi Data

Download Snappy-compressed taxi data files from the AWS public dataset:

```python
from taxi_event_reader import download_nyc_taxi_data

# Download to current directory
download_nyc_taxi_data()

# Download to specific directory
download_nyc_taxi_data(download_path="./data/")
```

### 2. Decompress Files Locally

Decompress `.snz` files to readable format:

```python
from snappy_decompress import decompress_snappy_files, preview_decompressed_files

# Decompress all .snz files
decompress_snappy_files(source_dir="./", destination_dir="./decompressed/")

# Preview first few lines of decompressed files
preview_decompressed_files("./decompressed/", lines_to_show=5)
```

### 3. Direct S3 Processing

Decompress files and upload directly to S3 without local storage:

```python
from decompressed_to_s3 import SnappyToS3Processor

# Initialize processor
processor = SnappyToS3Processor(bucket_name="your-bucket", region="us-east-1")

# Process single file
processor.decompress_and_upload(
    snappy_file_path="data.snz",
    s3_object_key="processed/data.txt"
)

# Batch processing
file_mappings = {
    "file1.snz": "processed/file1.txt",
    "file2.snz": "processed/file2.txt"
}
results = processor.batch_process(file_mappings)
```

## 🔧 Configuration

### AWS Configuration

Ensure your AWS credentials are configured:

```bash
aws configure
```

Or set environment variables:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
```

### Default Settings

- **Source S3 Bucket**: `aws-bigdata-blog`
- **Data Path**: `artifacts/flink-refarch/data/nyc-tlc-trips.snz/`
- **Default Region**: `us-east-1`
- **Local Decompression Dir**: `./snappy_decompress/`

## 📊 Data Format

The NYC taxi data contains trip records with fields like:

- Trip timestamps (pickup/dropoff)
- Passenger counts
- Trip distances
- Fare amounts
- Payment types
- Location coordinates

## 🚨 Error Handling

The toolkit includes comprehensive error handling for:

- Missing AWS credentials
- Network connectivity issues
- File corruption during download/decompression
- S3 access permissions
- Memory limitations during processing

## 🔍 Monitoring

Each module provides detailed logging with:

- ✅ Success indicators
- ❌ Error messages with details
- 📊 File size and processing statistics
- ⚠️ Warnings for potential issues

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- NYC Taxi & Limousine Commission for providing the dataset
- AWS for hosting the public dataset
- Google's Snappy compression library

## 📞 Support

For questions or issues:

1. Check the existing issues in the repository
2. Create a new issue with detailed description
3. Include error logs and system information

## 🔮 Future Enhancements

- [ ] Add data validation and schema checking
- [ ] Implement parallel processing for large datasets
- [ ] Add support for other compression formats
- [ ] Create data analysis utilities
- [ ] Add Docker containerization
- [ ] Implement data pipeline orchestration