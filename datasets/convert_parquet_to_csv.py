import sys
from pyspark.sql import SparkSession

def convert_parquet_to_csv(input_parquet_path, output_csv_path, partitions=None):
    spark = SparkSession.builder \
        .config("spark.driver.host", "localhost") \
        .appName("Parquet to CSV Converter") \
        .master("local[*]") \
        .getOrCreate()

    try:
        df = spark.read.parquet(input_parquet_path).persist()

        if partitions:
            df = df.repartition(int(partitions))
        else:
            df = df.coalesce(1)

        df.write.csv(output_csv_path, header=True)

    finally:
        spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python convert_parquet_to_csv.py [INPUT_PARQUET_PATH] [OUTPUT_CSV_PATH] [OPTIONAL_PARTITIONS]")
        sys.exit(1)

    input_parquet_path = sys.argv[1]
    output_csv_path = sys.argv[2]
    partitions = sys.argv[3] if len(sys.argv) == 4 else None

    if not input_parquet_path:
        print("Error: Missing [INPUT_PARQUET_PATH]")
        sys.exit(1)
    if not output_csv_path:
        print("Error: Missing [OUTPUT_CSV_PATH]")
        sys.exit(1)

    # Run the conversion
    try:
        convert_parquet_to_csv(input_parquet_path, output_csv_path, partitions)
        print(f"Conversion successful! CSV saved to {output_csv_path}")
    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        sys.exit(1)
