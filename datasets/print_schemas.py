import sys
import os
from pyspark.sql import SparkSession

def print_parquet_schemas(directory):
    spark = SparkSession.builder \
        .config("spark.driver.host", "localhost") \
        .appName("Print Parquet Schemas") \
        .master("local[*]") \
        .getOrCreate()

    if not os.path.isdir(directory):
        print(f"Error: The directory {directory} does not exist.")
        spark.stop()
        sys.exit(1)

    # List all files in the directory
    files = os.listdir(directory)

    # Filter for Parquet files
    parquet_files = [f for f in files if f.endswith('.parquet')]

    if not parquet_files:
        print(f"No Parquet files found in directory {directory}.")
        spark.stop()
        sys.exit(1)

    # Print the schema for each Parquet file
    for file in parquet_files:
        file_path = os.path.join(directory, file)
        print(f"\nSchema for {file_path}:")
        df = spark.read.parquet(file_path)
        df.printSchema()
        total_records = df.count()
        print(f"Total number of records in data set: {total_records}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python print_schemas.py [DIRECTORY_PATH]")
        sys.exit(1)

    directory_path = sys.argv[1]

    print_parquet_schemas(directory_path)
