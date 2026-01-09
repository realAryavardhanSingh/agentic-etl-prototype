import boto3
import json
import random
import time
import uuid
from datetime import datetime

# --- CONFIGURATION ---
BUCKET_NAME = "agentic-etl-landing-dev-01"  # Your bucket name
S3_PREFIX = "input/"                        # The folder inside the bucket
NORMAL_PROBABILITY = 0.8                    # 80% chance of normal data
DELAY_SECONDS = 2                           # Speed of data generation

# Initialize S3 Client
s3_client = boto3.client("s3")

def generate_normal_record():
    """Generates a valid record adhering to the 'expected' schema."""
    return {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now().isoformat(),
        "event_type": random.choice(["click", "view", "purchase"]),
        "user_id": random.randint(1000, 9999),
        "amount": round(random.uniform(10.0, 500.0), 2),
        "device": random.choice(["mobile", "desktop", "tablet"])
    }

def generate_chaos_record():
    """Generates a record that VIOLATES the schema (The Poison Pill)."""
    chaos_type = random.choice(["schema_mismatch", "bad_data_type"])
    
    record = {
        "event_id": str(uuid.uuid4()),
        "event_timestamp": datetime.now().isoformat(),
        "event_type": "error_simulation"
    }
    
    if chaos_type == "schema_mismatch":
        # SCENARIO 1: Unexpected Column (Schema Evolution Trigger)
        print(">> CHAOS INJECTED: Unexpected Column 'marketing_campaign'")
        record["marketing_campaign"] = "summer_sale_2026" 
        record["user_id"] = 9999
        
    elif chaos_type == "bad_data_type":
        # SCENARIO 2: Wrong Data Type (Data Quality Error)
        # 'amount' should be float, but we send a string
        print(">> CHAOS INJECTED: Data Type Mismatch for 'amount'")
        record["amount"] = "one_hundred_dollars" 
        record["user_id"] = 8888

    return record

def upload_to_s3(data):
    file_name = f"{S3_PREFIX}event_{int(time.time())}_{str(uuid.uuid4())[:8]}.json"
    try:
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=json.dumps(data)
        )
        print(f"Uploaded: {file_name}")
    except Exception as e:
        print(f"Upload Failed: {e}")

def main():
    print(f"Starting Chaos Monkey... Target: s3://{BUCKET_NAME}/{S3_PREFIX}")
    print("Press CTRL+C to stop.")
    
    try:
        while True:
            if random.random() < NORMAL_PROBABILITY:
                data = generate_normal_record()
                print("Status: Normal")
            else:
                data = generate_chaos_record()
                print("Status: CHAOS !!!")
            
            upload_to_s3(data)
            time.sleep(DELAY_SECONDS)
            
    except KeyboardInterrupt:
        print("\nStopping Chaos Monkey.")

if __name__ == "__main__":
    main()
