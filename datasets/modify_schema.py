import sys
from pyspark.sql import SparkSession

def modify_schema(first_file, second_file, output_file):
    spark = SparkSession.builder \
        .config("spark.driver.host", "localhost") \
        .appName("Modify Parquet Schema") \
        .master("local[*]") \
        .getOrCreate()

    df1 = spark.read.parquet(first_file)
    df2 = spark.read.parquet(second_file)

    # Print the original schema of the first file
    print(f"Original schema of {first_file}:")
    df1.printSchema()

    # Target schema based on the second file
    target_schema = df2.schema

    # Cast the columns in df1 to match the types in df2
    for field in target_schema:
        df1 = df1.withColumn(field.name, df1[field.name].cast(field.dataType))

    print(f"\nModified schema of {first_file}:")
    df1.printSchema()

    # Save the modified DataFrame to a new Parquet file
    df1.write.parquet(output_file)

    print(f"\nModified Parquet file saved to {output_file}")

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python modify_schema.py [FIRST_FILE_PATH] [SECOND_FILE_PATH] [OUTPUT_FILE_PATH]")
        sys.exit(1)

    first_file = sys.argv[1]
    second_file = sys.argv[2]
    output_file = sys.argv[3]

    modify_schema(first_file, second_file, output_file)
