from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# Initialize a Spark session
# local[*] = run local with as many threads as cores available
spark = SparkSession.builder \
    .config("spark.driver.host", "localhost") \
    .appName("Read Parquet File") \
    .master("local[*]") \
    .getOrCreate()

# Read the Parquet file
parquet_file = "yellow_tripdata_2023-09.parquet"
df = spark.read.parquet(parquet_file)

# # Show the data (e.g., first 20 rows)
# df.show()

# # Show the schema of the DataFrame
# df.printSchema()

pdf: pd.DataFrame = df.toPandas()

# Get the total number of records
total_records = pdf.shape[0]

filtered_df = pdf[pdf['total_amount'] < 0]

# Show the first 100 records
filtered_df.head(100)

print(f"Total number of records: {total_records}")

# x = pdf['trip_distance']
# y = pdf['total_amount']
# plt.figure(figsize=(10, 6))
# plt.scatter(x, y, alpha=0.5, color='orange')
# plt.xlim(0, 100)
# plt.ylim(-1000, 1000)

# plt.title('Total Amount payed per Trip Distance')
# plt.xlabel('Trip Distance (miles)')
# plt.ylabel('Total Amount ($)')
# plt.grid(True)
# plt.show()


# Stop the Spark session
spark.stop()