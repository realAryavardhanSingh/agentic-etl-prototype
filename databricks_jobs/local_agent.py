import boto3
import json
import time
import textwrap

# --- CONFIGURATION ---
BUCKET_NAME = "agentic-etl-landing-dev-01"
PREFIX = "input/"
EXPECTED_SCHEMA = {"event_id", "event_timestamp", "event_type", "user_id", "amount", "device"}

s3 = boto3.client("s3")

# --- THE BRAIN (LLM SIMULATION) ---
class ArchitectAgent:
    def analyze_error(self, error_type, details):
        print(f"\n🧠 ARCHITECT AGENT: Analyzing {error_type}...")
        time.sleep(1) # Simulate "thinking" time
        
        if error_type == "SCHEMA_MISMATCH":
            # This is the Prompt Engineering part
            prompt = f"""
            SYSTEM: You are an expert Data Engineer.
            TASK: A schema mismatch was detected.
            NEW COLUMNS: {details['new_cols']}
            ACTION: Generate a Spark SQL command to evolve the Delta table 'silver_clean'.
            """
            
            # SIMULATED LLM RESPONSE
            # In production, this would be: response = openai.chat.completions.create(...)
            new_col = list(details['new_cols'])[0]
            sql_fix = f"ALTER TABLE silver_clean ADD COLUMN {new_col} STRING COMMENT 'Auto-evolved by Agent';"
            
            return {
                "analysis": "Incoming data contains a valid business column addition.",
                "suggested_fix": sql_fix,
                "confidence": 0.98
            }
            
        elif error_type == "DATA_QUALITY":
            return {
                "analysis": "Data type violation detected (String instead of Double).",
                "suggested_fix": "QUARANTINE_RECORD",
                "confidence": 0.99
            }

# Initialize
architect = ArchitectAgent()

def get_latest_file():
    """Finds the most recent file uploaded to S3."""
    try:
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=PREFIX)
        if 'Contents' not in response:
            return None
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        return files[0]['Key']
    except Exception:
        return None

def scan_for_anomalies():
    print(".", end="", flush=True) # Heartbeat
    
    latest_file_key = get_latest_file()
    if not latest_file_key:
        return

    # Download and inspect
    try:
        obj = s3.get_object(Bucket=BUCKET_NAME, Key=latest_file_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
    except Exception as e:
        # File might be half-written or empty
        return
    
    # 1. Detect Schema Mismatch
    current_keys = set(data.keys())
    new_cols = current_keys - EXPECTED_SCHEMA
    
    # 2. Detect Data Quality
    amount_is_invalid = False
    if "amount" in data and not isinstance(data["amount"], (int, float)):
             amount_is_invalid = True

    # --- AGENT ACTION LOOP ---
    if new_cols:
        print(f"\n\n🚨 MONITOR: Schema Mismatch detected in {latest_file_key}")
        print(f"   Details: {new_cols}")
        
        # ASK THE ARCHITECT
        solution = architect.analyze_error("SCHEMA_MISMATCH", {"new_cols": new_cols})
        
        print(f"✅ AGENT SOLUTION GENERATED:")
        print(f"   Analysis: {solution['analysis']}")
        print(f"   EXECUTING SQL: \033[92m{solution['suggested_fix']}\033[0m") # Green text
        
        # In a real system, we would run: spark.sql(solution['suggested_fix'])
        print("   (Action logged to Governance Audit Trail)\n")
        time.sleep(2) # Don't spam
        
    elif amount_is_invalid:
        print(f"\n\n⚠️  MONITOR: Data Quality Error detected in {latest_file_key}")
        
        # ASK THE ARCHITECT
        solution = architect.analyze_error("DATA_QUALITY", {})
        
        print(f"✅ AGENT RESPONSE:")
        print(f"   Decision: {solution['suggested_fix']}")
        print("   (Record moved to Quarantine Table)\n")
        time.sleep(2)

if __name__ == "__main__":
    print(f"🚀 Agentic System Online. Monitoring s3://{BUCKET_NAME}...")
    try:
        while True:
            scan_for_anomalies()
            time.sleep(3)
    except KeyboardInterrupt:
        print("\nSystem Shutdown.")