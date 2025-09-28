#!/usr/bin/env python3
import argparse
import json
import math
import sys
import time
from typing import Iterator

import boto3
from botocore.exceptions import ClientError

RECORDS_PER_SHARD_PER_SEC = 1000  # Kinesis write limit

def iter_ndjson(path: str) -> Iterator[dict]:
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)

def count_events(path: str) -> int:
    c = 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                c += 1
    return c

def ensure_stream(kinesis, stream_name: str, shard_count: int):
    try:
        desc = kinesis.describe_stream_summary(StreamName=stream_name)
        print(f"Stream Description : {desc}")
        print(f"[info] Stream '{stream_name}' already exists. Using it as-is.")
        return
    except ClientError as e:
        if e.response.get("Error", {}).get("Code") != "ResourceNotFoundException":
            print(f"Code =  {e.response.get("Error", {}).get("Code")}")
            print(f"[error] Unexpected error checking stream: {e}")
            raise

    print(f"[info] Creating stream '{stream_name}' with {shard_count} shard(s)...")
    kinesis.create_stream(StreamName=stream_name, ShardCount=shard_count)

    # Wait for ACTIVE
    waiter = kinesis.get_waiter("stream_exists")
    waiter.wait(StreamName=stream_name)
    # Extra: poll status to ACTIVE (stream_exists waiter returns when it exists; may still be CREATING)
    while True:
        desc = kinesis.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]
        status = desc["StreamStatus"]
        if status == "ACTIVE":
            print("[info] Stream is ACTIVE.")
            break

        print(f"[info] Waiting for ACTIVE, current status: {status} ...")
        time.sleep(2)

def send_records_no_batch(kinesis, stream_name: str, path: str, shard_count: int):
    # throttle to shard_count * 1000 rec/s
    max_rate = shard_count * RECORDS_PER_SHARD_PER_SEC
    sent = 0
    window_start = time.perf_counter()

    for rec in iter_ndjson(path):
        # Partition key: trip_id
        pk_val = rec.get("trip_id")
        if pk_val is None:
            # Fallback if trip_id is missing (shouldn't happen based on your file)
            pk_val = str(hash(json.dumps(rec, sort_keys=True)))
            print(f"[warn] trip_id missing in record, using hash as partition key: {pk_val}")

        data_bytes = (json.dumps(rec) + "\n").encode("utf-8")

        # PutRecord (no batch)
        kinesis.put_record(StreamName=stream_name, Data=data_bytes, PartitionKey=str(pk_val))
        sent += 1

        # Simple token bucket: enforce records/sec cap
        elapsed = time.perf_counter() - window_start
        # allowed so far:
        allowed = max_rate * elapsed
        if sent > allowed:
            # sleep enough to get back under the line
            sleep_s = (sent - allowed) / max_rate
            if sleep_s > 0:
                time.sleep(sleep_s)

    # Final gentle wait to avoid abrupt close in environments
    print(f"[done] Sent {sent} records to '{stream_name}' using {shard_count} shard(s).")

def main():
    ap = argparse.ArgumentParser(description="Send NDJSON events to Kinesis (no batching).")
    ap.add_argument("--file", required=True, help="Path to NDJSON file")
    ap.add_argument("--stream", required=True, help="Kinesis stream name")
    ap.add_argument("--region", required=True, help="AWS region, e.g. us-east-1")
    ap.add_argument("--profile", default=None, help="AWS profile (optional)")
    args = ap.parse_args()

    # Session
    if args.profile:
        boto3.setup_default_session(profile_name=args.profile)
    kinesis = boto3.client("kinesis", region_name=args.region)

    total = count_events(args.file)
    if total == 0:
        print("[error] No events found in file.")
        sys.exit(1)

    # Decide shards solely based on total events (ceil(total/1000))
    shard_count = max(1, math.ceil(total / RECORDS_PER_SHARD_PER_SEC))
    print(f"[plan] Total events: {total} -> shard_count = {shard_count}")

    ensure_stream(kinesis, args.stream, shard_count)
    send_records_no_batch(kinesis, args.stream, args.file, shard_count)

if __name__ == "__main__":
    main()
