"""Command-line interface for file-utils package."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pyarrow.parquet as pq
import fastavro

from file_utils.io import open_any, exists, file_size
from file_utils.convert import convert, detect_format
from file_utils.stream import upload, download, list_keys, object_size, delete, s3_client


def main():
    """Main entry point for file-utils CLI."""
    parser = argparse.ArgumentParser(
        prog="file-utils",
        description="Streaming file utilities for CSV, TSV, JSONL, XML, Avro, Arrow, and Parquet",
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Convert command
    convert_parser = subparsers.add_parser("convert", help="Convert between file formats")
    convert_parser.add_argument("src", help="Source file path (local or s3://)")
    convert_parser.add_argument("dst", help="Destination file path (local or s3://)")
    convert_parser.add_argument("--compression", default="zstd", help="Compression for Parquet (default: zstd)")
    convert_parser.add_argument("--avro-codec", default="deflate", help="Codec for Avro (default: deflate)")
    convert_parser.add_argument("--chunk-rows", type=int, default=65536, help="Rows per batch (default: 65536)")
    convert_parser.add_argument("--row-group-rows", type=int, default=1000000, help="Rows per row group for Parquet (default: 1000000)")
    convert_parser.add_argument("--delimiter", help="Delimiter for CSV/TSV (auto-detected by default)")
    convert_parser.add_argument("--record-tag", help="XML record tag name (default: 'record' for writing, inferred for reading)")
    convert_parser.add_argument("--root-tag", default="root", help="XML root tag name (default: 'root')")
    
    # Upload command
    upload_parser = subparsers.add_parser("upload", help="Upload a file to S3")
    upload_parser.add_argument("src", help="Source file path (local or s3://)")
    upload_parser.add_argument("dst", help="Destination S3 URL (s3://bucket/key)")
    upload_parser.add_argument("--threshold-mb", type=int, default=64, help="Multipart threshold in MB (default: 64)")
    upload_parser.add_argument("--chunk-mb", type=int, default=64, help="Chunk size in MB (default: 64)")
    upload_parser.add_argument("--max-concurrency", type=int, default=8, help="Maximum concurrent uploads (default: 8)")
    
    # Download command
    download_parser = subparsers.add_parser("download", help="Download a file from S3")
    download_parser.add_argument("src", help="Source S3 URL (s3://bucket/key)")
    download_parser.add_argument("dst", help="Destination file path (local or s3://)")
    download_parser.add_argument("--threshold-mb", type=int, default=64, help="Multipart threshold in MB (default: 64)")
    download_parser.add_argument("--chunk-mb", type=int, default=64, help="Chunk size in MB (default: 64)")
    download_parser.add_argument("--max-concurrency", type=int, default=8, help="Maximum concurrent downloads (default: 8)")
    
    # List command
    ls_parser = subparsers.add_parser("ls", help="List S3 objects")
    ls_parser.add_argument("url", help="S3 URL (s3://bucket/prefix/)")
    
    # Info command
    info_parser = subparsers.add_parser("info", help="Show file information")
    info_parser.add_argument("file", help="File path (local or s3://)")
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        sys.exit(1)
    
    if args.command == "convert":
        cmd_convert(args)
    elif args.command == "upload":
        cmd_upload(args)
    elif args.command == "download":
        cmd_download(args)
    elif args.command == "ls":
        cmd_ls(args)
    elif args.command == "info":
        cmd_info(args)
    else:
        parser.print_help()
        sys.exit(1)


def cmd_convert(args):
    """Handle convert command."""
    try:
        result = convert(
            args.src,
            args.dst,
            compression=args.compression,
            avro_codec=args.avro_codec,
            chunk_rows=args.chunk_rows,
            row_group_rows=args.row_group_rows,
            delimiter=args.delimiter,
            record_tag=args.record_tag,
            root_tag=args.root_tag,
        )
        print(f"Converted {args.src} -> {args.dst}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_upload(args):
    """Handle upload command."""
    try:
        result = upload(
            args.src,
            args.dst,
            threshold_mb=args.threshold_mb,
            chunk_mb=args.chunk_mb,
            max_concurrency=args.max_concurrency,
        )
        print(f"Uploaded {args.src} -> {args.dst}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_download(args):
    """Handle download command."""
    try:
        result = download(
            args.src,
            args.dst,
            threshold_mb=args.threshold_mb,
            chunk_mb=args.chunk_mb,
            max_concurrency=args.max_concurrency,
        )
        print(f"Downloaded {args.src} -> {args.dst}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_ls(args):
    """Handle ls command."""
    try:
        for key in list_keys(args.url):
            print(key)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def cmd_info(args):
    """Handle info command."""
    try:
        fmt = detect_format(args.file)
        print(f"Format: {fmt}")
        
        if exists(args.file):
            size = file_size(args.file)
            print(f"Size: {size} bytes")
        
        if fmt == "parquet":
            print_parquet_info(args.file)
        elif fmt == "avro":
            print_avro_info(args.file)
        elif fmt in ("csv", "jsonl"):
            print_generic_info(args.file, fmt)
        elif fmt == "arrow":
            print_arrow_info(args.file)
        else:
            print(f"Info not available for format: {fmt}")
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


def print_parquet_info(path):
    """Print Parquet file information."""
    from file_utils.io import open_any
    file_obj = open_any(path, "rb")
    try:
        pf = pq.ParquetFile(file_obj)
        print(f"Schema: {pf.schema_arrow}")
        print(f"Row count: {pf.metadata.num_rows}")
        print(f"Row groups: {pf.num_row_groups}")
        if pf.metadata.num_row_groups > 0:
            compression = pf.metadata.row_group(0).column(0).compression
            print(f"Compression: {compression}")
    finally:
        file_obj.close()


def print_avro_info(path):
    """Print Avro file information."""
    from file_utils.io import open_any
    file_obj = open_any(path, "rb")
    try:
        reader = fastavro.reader(file_obj)
        schema = reader.writer_schema
        if schema is None:
            schema = reader.reader_schema
        if schema is not None:
            print(f"Writer schema: {json.dumps(schema, indent=2)}")
    finally:
        file_obj.close()


def print_arrow_info(path):
    """Print Arrow IPC file information."""
    import pyarrow.ipc as ipc
    from file_utils.io import open_any
    file_obj = open_any(path, "rb")
    try:
        reader = ipc.open_file(file_obj)
        print(f"Schema: {reader.schema}")
        print(f"Record batches: {reader.num_record_batches}")
        total_rows = sum(reader.get_batch(i).num_rows for i in range(reader.num_record_batches))
        print(f"Total rows: {total_rows}")
    except (ValueError, pa.ArrowIOError):
        # Try streaming format
        file_obj.seek(0)
        reader = ipc.open_stream(file_obj)
        print(f"Schema: {reader.schema}")
        print(f"Streaming Arrow IPC")
    finally:
        file_obj.close()


def print_generic_info(path, fmt):
    """Print generic information for CSV/JSONL."""
    import pyarrow.csv as csv
    from file_utils.io import open_any
    
    if fmt == "csv":
        file_obj = open_any(path, "rb")
        try:
            reader = csv.open_csv(file_obj)
            print(f"Schema: {reader.schema}")
        finally:
            file_obj.close()
    elif fmt == "jsonl":
        # Just show we know it's JSONL
        print("JSONL file - schema inferred from data")


if __name__ == "__main__":
    main()
