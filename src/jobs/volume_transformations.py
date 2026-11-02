import argparse
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable

parser = argparse.ArgumentParser()
parser.add_argument(
    "--input",
    required=True,
    help="S3 path of the input file"
)
parser.add_argument(
    "--output",
    required=True,
    help="S3 path of Delta table"
)
args = parser.parse_args()

spark = (
    SparkSession.builder
    .appName("NetAppVolumeTransformations")
    .getOrCreate()
)

INPUT_PATH = args.input
OUTPUT_PATH = args.output

print(f"Reading input: {INPUT_PATH}")
print(f"Writing output: {OUTPUT_PATH}")

# Extract file date from filename
# Example: volume_20260102.csv => 20260102
match = re.search(r"(\d{8})", INPUT_PATH)

if not match:
    raise ValueError(f"Could not extract YYYYMMDD date from input path: {INPUT_PATH}")

date_str = match.group(1)
file_date = datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")  # 20260102
df = spark.read.option("header", "true").option("inferSchema", "true").csv(INPUT_PATH)

print("Input schema:")
df.printSchema()

print(f"Input record count: {df.count()}")

# Add ingestion/file date
df = df.withColumn("file_date",lit(file_date))

# Initial load or incremental MERGE
if not DeltaTable.isDeltaTable(spark, OUTPUT_PATH):
    print("Delta table does not exist. Performing initial load.")
    df.write.format("delta").mode("overwrite").partitionBy("file_date").save(OUTPUT_PATH)
    print("Initial Delta load completed.")
else:
    print("Delta table exists. Performing incremental MERGE.")
    delta_table = DeltaTable.forPath(spark, OUTPUT_PATH)

    delta_table.alias("target").merge(df.alias("source"),"target.volume_uuid = source.volume_uuid")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    print("Incremental MERGE completed.")

print("Volume transformation completed successfully.")

def calculate_volume(length, width, height):
    return length * width * height

spark.stop()

