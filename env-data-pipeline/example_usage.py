#!/usr/bin/env python3
"""
Example usage of the modified batch pipeline that writes to local csv_outputs folder.
"""

from pipelines.batch_pipeline import BatchPipeline

def main():
    # Create pipeline with default output dir "csv_outputs"
    pipeline = BatchPipeline()

    # Run individual sources
    if pipeline.firms_client:
        print("Running FIRMS ingestion...")
        firms_path = pipeline.run_firms(days=1)
        print(f"FIRMS data saved to: {firms_path}")
    else:
        print("Skipping FIRMS - no API key set")

    print("Running Reddit ingestion...")
    reddit_path = pipeline.run_reddit(limit=10)
    print(f"Reddit data saved to: {reddit_path}")

    # Or run both
    print("Running both sources...")
    results = pipeline.run_all(reddit_limit=10, firms_days=1)
    print(f"Results: {results}")

if __name__ == "__main__":
    main()
