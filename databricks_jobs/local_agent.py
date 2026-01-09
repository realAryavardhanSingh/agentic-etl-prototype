import boto3
import json
import time

# CONFIGURATION
BUCKET_NAME = "agentic-etl-landing-dev-01"  # Your bucket
PREFIX = "input/"

# The "Contract" - The schema we expect
EXPECTED_SCHEMA = {"event_id", "event_timestamp", "event_type", "user_id", "amount", "device"}

s3 = boto3.client("s3")

def get_latest_file():
    """Finds the most recent file uploaded to S3."""
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
    if 'Contents' not in response:
        return None
    
    # Sort by last modified time
    files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
    return files[0]['Key']

def scan_for_anomalies():
    print("🕵️  Agent Active: Scanning S3 for schema violations...")
    
    latest_file_key = get_latest_file()
    if not latest_file_key:
        print("No files found.")
        return

    print(f"Checking latest file: {latest_file_key}")
    
    # Download and inspect the file
    obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_file_key)
    data = json.loads(obj['Body'].read().decode('utf-8'))
    
    # 1. Detect Schema Mismatch (Unexpected Columns)
    current_keys = set(data.keys())
    new_cols = current_keys - EXPECTED_SCHEMA
    
    # 2. Detect Data Quality Issues (Bad Types)
    # Check if 'amount' is a valid number
    amount_is_valid = True
    if "amount" in data:
        if not isinstance(data["amount"], (int, float)):
             amount_is_valid = False

    # --- AGENT LOGIC ---
    if new_cols:
        print("\n🚨 ALERT: Schema Mismatch Detected!")
        print(f"   Unexpected Columns: {new_cols}")
        print("   >> Triggering Architect Agent to analyze impact...")
        # HERE we will hook in the LLM later
        
    elif not amount_is_valid:
        print("\n⚠️  ALERT: Data Quality Error!")
        print(f"   Invalid value for 'amount': {data.get('amount')}")
        print("   >> Triggering Ops Agent to quarantine record...")
        
    else:
        print("✅ Data looks good.")

if __name__ == "__main__":
    # Loop to simulate continuous monitoring
    try:
        while True:
            scan_for_anomalies()
            time.sleep(5) # Check every 5 seconds
    except KeyboardInterrupt:
        print("Agent stopped.")