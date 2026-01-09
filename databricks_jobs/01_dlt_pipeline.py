import dlt
from pyspark.sql.functions import *

# SETTINGS
# We use the bucket you created
source_path = "s3://agentic-etl-landing-dev-01/input/"

# 1. BRONZE LAYER (The Ingest)
# "Auto Loader" (cloudFiles) automatically picks up new files from S3.
# We set schemaEvolutionMode to "rescue" so it never fails on ingestion, 
# capturing "bad" columns in a special _rescued_data column.
@dlt.table(
    comment="Raw data from S3, capturing all schema changes."
)
def raw_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue") 
        .load(source_path)
    )

# 2. SILVER LAYER (The Quality Gate)
# This is where we catch the "Chaos".
# We define valid rules. If data breaks them, it is DROPPED from this table
# but logged in the event log (which triggers our Agent).
rules = {
    "valid_id": "id IS NOT NULL",
    "valid_amount": "amount IS NOT NULL AND cast(amount as double) IS NOT NULL",
    "no_unexpected_cols": "_rescued_data IS NULL" 
}

@dlt.table(
    comment="Clean data only. Chaos data is dropped here."
)
@dlt.expect_all_or_drop(rules)
def silver_clean():
    return dlt.read("raw_bronze")