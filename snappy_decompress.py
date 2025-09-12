#!/usr/bin/env python3
"""
Snappy File Decompressor and Reader

This module decompresses Snappy compressed (.snz) files and provides utilities
to read and preview the decompressed content. It's specifically designed for
processing NYC taxi data files but can handle any Snappy compressed files.

Author: Vinay
Created: September 11, 2025
Last Modified: September 11, 2025
Version: 1.0

Dependencies:
    - python-snappy: pip install python-snappy
"""

import os
import snappy
from typing import Optional


def decompress_snappy_files(
    source_dir: str = "./",
    destination_dir: str = "./snappy_decompress/",
    file_extension: str = ".snz"
) -> None:
    """
    Decompress all Snappy compressed files in the source directory.
    
    Args:
        source_dir (str): Directory containing .snz files to decompress
        destination_dir (str): Directory to store decompressed files
        file_extension (str): File extension of compressed files
    
    Returns:
        None
    
    Raises:
        OSError: If source directory doesn't exist or destination can't be created
        snappy.UncompressError: If decompression fails
    """
    # Create destination directory if it doesn't exist
    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)
        print(f"📁 Created destination directory: {destination_dir}")
    
    # Get list of compressed files
    compressed_files = [f for f in os.listdir(source_dir) if f.endswith(file_extension)]
    
    if not compressed_files:
        print(f"⚠️  No {file_extension} files found in {source_dir}")
        return
    
    print(f"🔧 Found {len(compressed_files)} compressed files to process")
    
    # Process each compressed file
    for filename in compressed_files:
        # Remove the compression extension for output filename
        output_filename = filename[:-len(file_extension)]
        source_path = os.path.join(source_dir, filename)
        output_path = os.path.join(destination_dir, output_filename)
        
        try:
            # Decompress the file using Snappy stream decompression
            with open(source_path, "rb") as src, open(output_path, "wb") as dst:
                snappy.stream_decompress(src=src, dst=dst)
            
            print(f"✓ Decompressed {filename} → {output_filename}")
            
        except Exception as e:
            print(f"✗ Failed to decompress {filename}: {str(e)}")


def preview_decompressed_files(
    directory: str = "./snappy_decompress/",
    lines_to_show: int = 2,
    skip_extensions: tuple = (".snz",)
) -> None:
    """
    Read and preview the first few lines of decompressed files.
    
    Args:
        directory (str): Directory containing decompressed files
        lines_to_show (int): Number of lines to preview from each file
        skip_extensions (tuple): File extensions to skip during preview
    
    Returns:
        None
    
    Raises:
        FileNotFoundError: If the directory doesn't exist
        UnicodeDecodeError: If file contains non-text content
    """
    if not os.path.exists(directory):
        print(f"❌ Directory not found: {directory}")
        return
    
    # Get list of files to preview
    files_to_preview = [
        f for f in os.listdir(directory) 
        if not any(f.endswith(ext) for ext in skip_extensions)
    ]
    
    if not files_to_preview:
        print(f"⚠️  No files to preview in {directory}")
        return
    
    print(f"👀 Previewing {len(files_to_preview)} decompressed files")
    print("=" * 60)
    
    # Preview each file
    for filename in files_to_preview:
        file_path = os.path.join(directory, filename)
        
        print(f"\n📄 Reading {file_path}:")
        print("-" * 40)
        
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                for i, line in enumerate(f):
                    if i >= lines_to_show:
                        break
                    print(f"Line {i+1}: {line.strip()}")
                        
        except UnicodeDecodeError:
            print("⚠️  File appears to be binary - skipping text preview")
            
        except Exception as e:
            print(f"❌ Error reading file: {str(e)}")
            
        print("\n" + "=" * 60)


def process_snappy_files(
    source_dir: str = "./",
    destination_dir: str = "./snappy_decompress/",
    preview_lines: int = 2
) -> None:
    """
    Complete pipeline: decompress Snappy files and preview content.
    
    Args:
        source_dir (str): Directory containing .snz files
        destination_dir (str): Directory for decompressed files  
        preview_lines (int): Number of lines to preview from each file
    
    Returns:
        None
    """
    print("🚀 Starting Snappy file processing pipeline")
    
    # Step 1: Decompress files
    print("\n📦 Step 1: Decompressing files...")
    decompress_snappy_files(source_dir, destination_dir)
    
    # Step 2: Preview decompressed content
    print("\n📖 Step 2: Previewing decompressed content...")
    preview_decompressed_files(destination_dir, preview_lines)


def main() -> None:
    """
    Main execution function.
    """
    # Configuration
    LOCAL_SNZ_PATH = "./"
    DESTINATION_DIR = "./snappy_decompress/"
    PREVIEW_LINES = 2
    
    try:
        # Uncomment the following line to decompress files
        # decompress_snappy_files(LOCAL_SNZ_PATH, DESTINATION_DIR)
        
        # Preview existing decompressed files
        preview_decompressed_files(DESTINATION_DIR, PREVIEW_LINES)
        
    except Exception as e:
        print(f"❌ Error in main execution: {str(e)}")


if __name__ == "__main__":
    main()