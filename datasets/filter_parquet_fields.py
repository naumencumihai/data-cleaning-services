import sys
from pyspark.sql import SparkSession

def filter_parquet_fields(input_parquet_path, output_parquet_path, x_field, y_field):
    spark = SparkSession.builder \
        .config("spark.driver.host", "localhost") \
        .appName("Filter Parquet Fields") \
        .master("local[*]") \
        .config("spark.driver.memory", "16g") \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.maxResultSize", "4g") \
        .config("spark.network.timeout", "1200s") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

    df = spark.read.parquet(input_parquet_path)

    # Select only the specified fields
    selected_df = df.select(x_field, y_field)

    selected_df.write.parquet(output_parquet_path)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python filter_parquet_fields.py [INPUT_PARQUET_PATH] [OUTPUT_PARQUET_PATH] [X_FIELD] [Y_FIELD]")
        sys.exit(1)

    input_parquet_path = sys.argv[1]
    output_parquet_path = sys.argv[2]
    x_field = sys.argv[3]
    y_field = sys.argv[4]

    filter_parquet_fields(input_parquet_path, output_parquet_path, x_field, y_field)
