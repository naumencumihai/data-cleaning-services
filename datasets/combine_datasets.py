import sys
import os
from pyspark.sql import SparkSession

def combine_parquet_files(input_dir, output_file):
    spark = SparkSession.builder \
        .config("spark.driver.host", "localhost") \
        .appName("Combine Parquet Files") \
        .master("local[*]") \
        .config("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .getOrCreate()

    if not os.path.isdir(input_dir):
        print(f"Error: The directory {input_dir} does not exist.")
        sys.exit(1)

    # Read all Parquet files in the directory into a single DataFrame and save the combined file
    combined_df = spark.read.parquet(input_dir).repartition(10).persist()
    combined_df.write.parquet(output_file)

    print(f"Combined Parquet file saved to {output_file}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python combine_datasets.py [PATH_TO_DATASETS] [OUTPUT_FILE_NAME]")
        sys.exit(1)

    input_dir = sys.argv[1]
    output_file = sys.argv[2]

    combine_parquet_files(input_dir, output_file)
