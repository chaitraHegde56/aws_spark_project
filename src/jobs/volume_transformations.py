import argparse
import re
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable


def extract_file_date(input_path):
    """Extract YYYYMMDD from the input filename."""

    match = re.search(r"(\d{8})", input_path)

    if not match:
        raise ValueError(
            f"Could not extract YYYYMMDD date from input path: {input_path}"
        )

    date_str = match.group(1)

    return datetime.strptime(date_str, "%Y%m%d").strftime("%Y-%m-%d")

def transform_data(spark, input_path, output_path):

    """Read input, transform data and write to Delta table."""

    print(f"Reading input: {input_path}")
    print(f"Writing output: {output_path}")

    file_date = extract_file_date(input_path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(input_path)
    )

    print("Input schema:")
    df.printSchema()

    print(f"Input record count: {df.count()}")

    df = df.withColumn("file_date", lit(file_date))

    if not DeltaTable.isDeltaTable(spark, output_path):

        print("Delta table does not exist. Performing initial load.")

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("file_date")
            .save(output_path)
        )

        print("Initial Delta load completed.")

    else:

        print("Delta table exists. Performing incremental MERGE.")

        delta_table = DeltaTable.forPath(spark, output_path)

        (
            delta_table.alias("target")
            .merge(
                df.alias("source"),
                "target.volume_uuid = source.volume_uuid"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

        print("Incremental MERGE completed.")

    print("Volume transformation completed successfully.")

def calculate_volume(length, width, height):
    return length * width * height

def main():
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

    spark = (SparkSession.builder.appName("NetAppVolumeTransformations").getOrCreate())

    try:
        transform_data(
            spark,
            args.input,
            args.output
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()