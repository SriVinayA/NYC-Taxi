"""
NYC Taxi Events Reader - Multi-Threaded Version
===============================================

This script downloads NYC taxi trip data from AWS S3, decompresses it using Snappy compression,
and processes the events to extract timing information. It uses multi-threading to process
multiple S3 objects concurrently for improved performance.

Key Features:
- Downloads compressed taxi trip data from a public S3 bucket
- Decompresses Snappy-compressed files in memory
- Extracts timing information from each trip event
- Processes multiple files concurrently using threading
- Outputs decompressed NDJSON files with timing statistics

Author: kavyasripunna2020
Date:   2025-09-15T21:32:10Z
Version: a030626-dirty
License: MIT
"""

# AWS SDK imports for S3 access
import boto3
from boto3 import session
from botocore import UNSIGNED
from botocore.client import Config
from botocore.exceptions import NoCredentialsError

# Standard library imports for file operations and concurrency
import os
import io
import threading
import time
import re

# Third-party imports for compression and file format detection
import snappy
from concurrent.futures import ThreadPoolExecutor, as_completed
import FileFormatDetection

# Local application imports
from TripEvent import TripEvent
import AdaptTimeOption

# ----------------------
# Configuration Constants
# ----------------------
REGION = "us-east-1"                                              # AWS region where the S3 bucket is located
BUCKET_NAME = "aws-bigdata-blog"                                  # Public S3 bucket containing NYC taxi data
OBJECT_PREFIX = "artifacts/flink-refarch/data/nyc-tlc-trips.snz/" # S3 path prefix to taxi trip files
MAX_FILES = 20                                                    # Maximum number of files to download and process
OUTPUT_DIR = "./snappy_decompressed_events"                      # Local directory for decompressed output files

# ----------------------
# Global Variables for Thread-Safe Statistics
# ----------------------
stats_lock = threading.Lock()    # Thread synchronization lock for shared statistics
total_events = 0                 # Total number of events processed across all threads
total_processing_time = 0.0      # Cumulative processing time across all threads
earliest_time = None             # Earliest timestamp found in any event (milliseconds since epoch)
latest_time = None               # Latest timestamp found in any event (milliseconds since epoch)

# ----------------------
# AWS S3 Session Setup
# ----------------------
session_ = session.Session()                           # Create a new AWS session
s3_resource = session_.resource('s3', region_name=REGION)  # Initialize S3 resource for the specified region

bucket = s3_resource.Bucket(BUCKET_NAME)               # Get reference to the S3 bucket
print("Files in S3 bucket:")

# ----------------------
# Helper Functions
# ----------------------
def safe_filename_from_key(key: str, ext: str = ".ndjson") -> str:
    """
    Convert an S3 object key into a filesystem-safe filename.
    
    This function transforms S3 object paths into valid local filenames by removing
    forward slashes and appending a file extension. This preserves the original
    structure information while creating a flat filename structure.
    
    Args:
        key (str): The S3 object key/path (e.g., "artifacts/flink-refarch/data/file.snz/part-0000.snz")
        ext (str): File extension to append (default: ".ndjson")
    
    Returns:
        str: Filesystem-safe filename
        
    Examples:
        artifacts/flink-refarch/data/file.snz/part-0000.snz -> 
        artifactsflink-refarchdatafile.snzpart-0000.snz.ndjson
    """
    # Remove all forward slashes to create a flat filename
    fname = key.replace("/", "")
    return fname + ext

def list_s3_objects(bucket_name: str, prefix: str, max_files: int):
    """
    List S3 objects from a public bucket using unsigned requests.
    
    This function connects to S3 without credentials (unsigned access) to list
    objects from a public bucket. It's designed for accessing publicly available
    datasets like the NYC taxi data.
    
    Args:
        bucket_name (str): Name of the S3 bucket to access
        prefix (str): Object key prefix to filter results
        max_files (int): Maximum number of objects to return
    
    Returns:
        list: List of S3 ObjectSummary instances up to max_files limit
    """
    # Create S3 resource with unsigned config for public bucket access
    s3_res = session.Session().resource("s3", config=Config(signature_version=UNSIGNED))
    bucket = s3_res.Bucket(bucket_name)
    
    out = []
    # Iterate through objects matching the prefix
    for i, obj in enumerate(bucket.objects.filter(Prefix=prefix)):
        print(f"Found: {obj.key}")
        out.append(obj)
        # Stop when we reach the maximum file limit
        if i + 1 >= max_files:
            break
    return out

def download_and_decompress(s3_client, obj_summary):
    """
    Download an S3 object and decompress it using Snappy compression.
    
    This function downloads a Snappy-compressed file from S3, reads it into memory,
    and decompresses it using the Snappy streaming decompression algorithm.
    
    Args:
        s3_client: Boto3 S3 client instance for making requests
        obj_summary: S3 ObjectSummary containing bucket name and key
    
    Returns:
        tuple: (decompressed_stream, download_time_seconds, original_file_size)
            - decompressed_stream (BytesIO): In-memory stream of decompressed data
            - download_time_seconds (float): Time taken to download and decompress
            - original_file_size (int): Size of the compressed file in bytes
    """
    start = time.time()
    
    # Download the object from S3
    resp = s3_client.get_object(Bucket=obj_summary.bucket_name, Key=obj_summary.key)
    payload = resp["Body"].read()  # Read entire file into memory
    size = resp["ContentLength"]   # Get original compressed file size
    
    # Set up source and destination streams for decompression
    src = io.BytesIO(payload)      # Source: compressed data in memory
    dst = io.BytesIO()             # Destination: decompressed data
    
    # Perform Snappy stream decompression
    snappy.stream_decompress(src=src, dst=dst)
    dst.seek(0)  # Reset stream position to beginning for reading
    
    # Calculate total processing time
    read_time = max(0.0, time.time() - start)
    return dst, read_time, size

def process_object(obj_summary):
    """
    Process a single S3 object in a dedicated thread.
    
    This function is the core worker function that handles:
    1. Downloading and decompressing the S3 object
    2. Detecting the file format
    3. Parsing each line as a TripEvent to extract timestamps
    4. Writing decompressed data to a local NDJSON file
    5. Updating global statistics in a thread-safe manner
    
    Args:
        obj_summary: S3 ObjectSummary containing bucket name and key information
    
    Returns:
        tuple: (output_file_path, events_processed, processing_time, thread_name)
    """
    print(f"Thread Name: {threading.current_thread().name} - Starting processing for {obj_summary.key}")

    # Create S3 client for this thread (each thread needs its own client)
    sess = session.Session()
    s3_client = sess.client("s3", region_name=REGION)

    # Access global statistics variables
    global total_events, total_processing_time, earliest_time, latest_time

    key = obj_summary.key
    
    # Download and decompress the S3 object
    stream, read_time, size = download_and_decompress(s3_client, obj_summary)
    bps = (size / read_time) if read_time > 0 else float("inf")
    print(f"Started thread {threading.current_thread()} Read {key}: {size} bytes in {read_time:.2f}s ({bps:.2f} B/s)")

    # Detect the file format of the decompressed stream
    file_format = FileFormatDetection.sniff_stream(stream)
    print(f"Started thread {threading.current_thread()}  Detected file format for {key}: {file_format}")
    stream.seek(0)  # Rewind stream after format detection

    # Prepare output directory and file path
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, safe_filename_from_key(key, ".ndjson"))

    # Initialize counters and timing
    events = 0
    start_proc = time.time()

    # Process each line in the decompressed stream
    with open(out_path, "w", encoding="utf-8", buffering=1024 * 1024) as fh:
        for raw in stream:
            try:
                # Decode the raw bytes to string
                line = raw.decode("unicode_escape")
                
                # Parse as TripEvent to extract timestamp
                ev = TripEvent(line)
                ts_ms = ev.timestamp  # Keep raw milliseconds since epoch

                # Update global timestamp bounds (thread-safe)
                if ts_ms is not None:
                    with stats_lock:
                        if earliest_time is None or ts_ms < earliest_time:
                            earliest_time = ts_ms
                        if latest_time is None or ts_ms > latest_time:
                            latest_time = ts_ms

                # Write the original line to output file
                if not line.endswith("\n"):
                    line += "\n"
                fh.write(line)

                events += 1
                # Flush output buffer periodically for large files
                if events % 50000 == 0:
                    fh.flush()
                    
            except ValueError:
                print(f"{key}: Ignoring malformed line.")
            except Exception as e:
                print(f"{key}: Error processing line: {e}")
        
        # Ensure all data is written to disk
        fh.flush()
        fh.close()

    # Calculate processing time and update global statistics
    proc_time = max(0.0, time.time() - start_proc)
    with stats_lock:
        total_events += events
        total_processing_time += proc_time

    # Calculate and display throughput
    thr = (events / proc_time) if proc_time > 0 else 0.0
    print(f"Started thread {threading.current_thread()} Wrote {events} events to {out_path} | ProcTime {proc_time:.2f}s | {thr:.2f} ev/s")

    # Clean up S3 client resources
    s3_client.close()

    return out_path, events, proc_time, threading.current_thread().name

def main():
    """
    Main function that orchestrates the multi-threaded processing of NYC taxi data.
    
    This function:
    1. Lists S3 objects to process based on configuration
    2. Creates and starts worker threads for parallel processing
    3. Waits for all threads to complete
    4. Displays comprehensive processing statistics
    
    The function uses native Python threading rather than ThreadPoolExecutor
    to provide more control over thread naming and lifecycle management.
    """
    
    # ----------------------
    # Phase 1: Discovery - List S3 objects to process
    # ----------------------
    objs = list_s3_objects(BUCKET_NAME, OBJECT_PREFIX, MAX_FILES)
    if not objs:
        print("No S3 objects found for given prefix.")
        return

    # Start overall timing
    t0 = time.time()
    outputs = []

    # Get reference to main thread for debugging
    current_thread = threading.current_thread()
    print(f"Thread Name: {current_thread.name}")
    
    # ----------------------
    # Alternative ThreadPoolExecutor approach (commented out)
    # ----------------------
    # This shows an alternative implementation using ThreadPoolExecutor
    # which would provide automatic thread pool management but less control
    # over individual thread naming and lifecycle
    # 
    # with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    #     futures = {pool.submit(process_object, obj): obj.key for obj in objs}
    #     for fut in as_completed(futures):
    #         key = futures[fut]
    #         try:
    #             out_path, events, proc_time, threadname = fut.result()
    #             print(f"Thread Name: {threadname} | Completed processing {key}: {events} events in {proc_time:.2f}s")
    #             outputs.append(out_path)
    #         except Exception as e:
    #             print(f"Failed processing {key}: {e}")

    # ----------------------
    # Phase 2: Thread Creation and Startup
    # ----------------------
    i = 0
    threads = []
    
    for obj in objs:
        i = i + 1
        
        # Extract filename from S3 key for thread naming
        # Uses regex to get the last part of the path (filename with extension)
        match = re.search(r'([^/]+\.[^/]+)$', obj.key)
        if not match:
            continue  # Skip if it's a directory (no file extension)
        obj_name = match.group(0)  # Get filename with extension
                
        # Create and start worker thread with descriptive name
        thread = threading.Thread(
            target=process_object, 
            args=(obj,), 
            name=f"Thread {obj_name}-{i}"
        )
        threads.append(thread)
        thread.start()
        print(f"Started thread {thread.name} for {obj.key}")

    # ----------------------
    # Phase 3: Thread Synchronization - Wait for completion
    # ----------------------
    for thread in threads:
        thread.join()  # Block until this thread completes
        print(f"Thread {thread.name} has finished execution.")
        outputs.append(thread.name)  # Track completed threads

    print(f"Thread Name: {current_thread.name}")

    # ----------------------
    # Phase 4: Results and Statistics
    # ----------------------
    total_time = max(0.0, time.time() - t0)

    # Display comprehensive processing statistics
    if total_events > 0:
        # Calculate overall throughput across all threads
        overall_thr = (total_events / total_processing_time) if total_processing_time > 0 else 0.0
        
        print(f"----- SUMMARY -----")
        print(f"Objects processed: {len(outputs)}")
        print(f"Total events: {total_events}")
        print(f"Overall processing throughput: {overall_thr:.2f} ev/s")
        
        # Display timestamp range found in the data
        if earliest_time is not None:
            print(f"Earliest Event Time (ms since epoch): {earliest_time}")
        if latest_time is not None:
            print(f"Latest Event Time (ms since epoch): {latest_time}")
            
        print(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
        print(f"Total wall time: {total_time:.2f}s")

if __name__ == "__main__":
    # Entry point - execute main function when script is run directly
    main()