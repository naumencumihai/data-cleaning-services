# Data Cleaning Services

General Description of the entire project (TODO)

## Table of Contents
- [AVF MapReduce Outlier Detection](#avf-mapreduce-outlier-detection)
  - [outlier_detection.py](#1-outlier_detectionpy-script)
  - [plot_outliers.py](#2-plot_outlierspy-script)
- [Hadoop MapReduce Implementation](#hadoop-mapreduce-implementation)
  - [recompile_java_mapreduce.py](#1-recompile_java_mapreducepy-tool)
- [Data Processing Scripts Overview](#data-processing-scripts-overview)
  - [modify_schema.py](#1-modify_schemapy)
  - [combine_datasets.py](#2-combine_datasetspy)
  - [print_schemas.py](#3-print_schemaspy)
  - [filter_parquet_fields.py](#4-filter_parquet_fieldspy)
  - [convert_parquet_to_csv.py](#5-convert_parquet_to_csvpy)
- [Requirements](#requirements)
- [Installation](#installation)
- [License](#license)
- [Contact](#contact)

## AVF MapReduce Outlier Detection

This project implements an Attribute Value Frequency (AVF) based outlier detection method using the Hadoop MapReduce framework. The AVF method identifies outliers in large datasets by analyzing the frequency of attribute values, considering values with low frequencies as potential outliers.

Outlier detection is a critical task in data analysis, helping to identify anomalies or deviations that may indicate errors, fraud, or novel insights. The AVF method is an effective approach for detecting outliers based on the rarity of attribute values. This implementation leverages Hadoop MapReduce to efficiently process large datasets distributed across multiple nodes.

### 1. `outlier_detection.py` script

**Description**:  
This script orchestrates the detection of outliers in a dataset using a two-phase MapReduce job. The script handles file preparation, uploads data to HDFS, runs the MapReduce jobs, and processes the results to generate a labeled CSV file indicating which data points are outliers.

**Usage**:
```bash
python outlier_detection.py [HDFS_USER_PATH] [HDFS_INPUT_FILE_PATH | OPTIONAL: type SKIP if not available] [LOCAL_INPUT_FILE_PATH] [OUTPUT_DIR_NAME] [X_AXIS_FIELD] [Y_AXIS_FIELD] [FREQ_THRESHOLD | DEFAULT: 1]
```

**Parameters**:
- `HDFS_USER_PATH`: The base path in HDFS where the input data is stored and the output will be saved.
- `HDFS_INPUT_FILE_PATH`: The path to the input file in HDFS. If not available, use `SKIP` to generate the file from the local input.
- `LOCAL_INPUT_FILE_PATH`: The path to the local input CSV file that needs to be processed. The file should be in a format suitable for MapReduce (indexed, no header).
- `OUTPUT_DIR_NAME`: The name of the directory where the output will be stored. This directory will be created under the results folder.
- `X_AXIS_FIELD`: The name of the column in the CSV file that represents the X-axis field for outlier detection.
- `Y_AXIS_FIELD`: The name of the column in the CSV file that represents the Y-axis field for outlier detection.
- `FREQ_THRESHOLD`: (Optional) The frequency threshold to determine whether a data point is an outlier. If not provided, the default value is 1.

**Example**:
```bash
python outlier_detection.py /user/yourusername hdfs_input.csv ./local_input.csv results_directory trip_distance total_amount 2
```

This command will:
1. Check if the input file in HDFS exists or if it needs to be generated from the local file.
2. Upload the local input file to HDFS if necessary, and remove the header if it exists.
3. Run the first MapReduce job to detect outliers for the `trip_distance` field.
4. Run the second MapReduce job to detect outliers for the `total_amount` field.
5. Retrieve the results from HDFS and save them locally under `results_directory`.
6. Merge the results with the original data to create a final labeled CSV file indicating which records are outliers.

**Note**:  
Make sure the Hadoop environment is set up correctly, and the necessary Java MapReduce implementations are compiled and available in the specified paths.

### 2. `plot_outliers.py` script

**Description**:  
This script generates scatter plots to visualize outliers and non-outliers in a dataset. It takes an input CSV file that contains labeled data points (outliers marked with 'OUT') and plots them in separate subplots. The script allows for sampling non-outliers to reduce processing time and save memory when dealing with large datasets.

**Usage**:
```bash
python plot_outliers.py [INPUT_CSV_PATH] [X_FIELD] [Y_FIELD] [SAMPLE_SIZE]
```

**Parameters**:
- `INPUT_CSV_PATH`: The path to the CSV file that contains the data to be plotted. The file should include an index, the X and Y fields, and a label column indicating outliers.
- `X_FIELD`: The name of the column in the CSV file that represents the X-axis field.
- `Y_FIELD`: The name of the column in the CSV file that represents the Y-axis field.
- `SAMPLE_SIZE`: (Optional) The number of non-outlier data points to sample for plotting. If not provided, all non-outliers will be plotted.

**Example**:
```bash
python plot_outliers.py ./results/labeled_result.csv trip_distance total_amount 100000
```

This command will:
1. Read the `labeled_result.csv` file located in the `./results/` directory.
2. Separate the data into outliers (labeled 'OUT') and non-outliers.
3. Sample 10,000 non-outlier data points for plotting (if `SAMPLE_SIZE` is provided).
4. Create two scatter plots:
   - One for non-outliers (in blue).
   - One for outliers (in red).
5. Save the generated plots as `outliers_plot.png` in the same directory as the input CSV file.

**Note**:  
The script is designed to handle large datasets by sampling non-outliers. This approach reduces memory usage and improves plotting performance. The `SAMPLE_SIZE` parameter is optional but recommended for very large datasets.

## Hadoop MapReduce Implementation

### 1. `recompile_java_mapreduce.py` tool 

**Description**:  
This script automates the process of recompiling Java MapReduce implementations. It compiles the Mapper, Reducer, and Driver classes for a given implementation and packages them into a JAR file. The script takes the implementation name as an argument, ensures it is in camelCase or PascalCase format, and then generates a directory in snake_case where the compiled files are stored.

**Usage**:
```bash
python recompile_java_mapreducer_implementation.py [IMPLEMENTATION_NAME]
```

**Parameters**:
- `IMPLEMENTATION_NAME`: The name of the MapReduce implementation in camelCase or PascalCase format. This should correspond to the Java files you want to compile (e.g., `OutlierDetection`).

**Example**:
```bash
python recompile_java_mapreducer_implementation.py AvfOutlierDetectionJob1
```
This command will:
1. Verify that `AvfOutlierDetectionJob1` is in camelCase or PascalCase format.
2. Convert `AvfOutlierDetectionJob1` to `avf_outlier_detection_job1` for the directory name.
3. Compile the Java files `AvfOutlierDetectionJob1Mapper.java`, `AvfOutlierDetectionJob1Reducer.java`, and `AvfOutlierDetectionJob1Driver.java` located in the `avf_outlier_detection_job1` directory.
4. Package the compiled classes into `avf_outlier_detection_job1/AvfOutlierDetectionJob1.jar`.

**Note**:  
Ensure that the Hadoop environment is properly set up and the `hadoop classpath` command is correctly configured in your system for the script to work effectively.

## Data Processing Scripts Overview

This repository contains a set of Python scripts designed to manage and process large datasets in Parquet format using PySpark. The scripts included in this repository allow for schema modification, dataset combination, and schema inspection, making it easier to handle and analyze large-scale data.


### 1. `modify_schema.py`

**Description**:  
This script modifies the schema of a Parquet file to match the schema of another Parquet file. This is particularly useful when you need to combine datasets that have the same structure but differing data types.

**Usage**:  
```bash
python modify_schema.py [INPUT_FILE_PATH] [REFERENCE_FILE_PATH] [OUTPUT_FILE_PATH]
```

**Parameters**:
- `INPUT_FILE_PATH`: The path to the Parquet file whose schema you want to modify.
- `REFERENCE_FILE_PATH`: The path to the Parquet file whose schema you want to use as a reference.
- `OUTPUT_FILE_PATH`: The path where the modified Parquet file will be saved.

**Example**:
```bash
python modify_schema.py datasets/yellow_tripdata_2023-01.parquet datasets/yellow_tripdata_2023-02.parquet datasets/yellow_tripdata_2023-01_modified.parquet
```

### 2. `combine_datasets.py`

**Description**:  
This script combines multiple Parquet files into a single dataset. It processes files with similar schemas and merges them into one comprehensive dataset, facilitating easier data management and analysis.

**Usage**:  
```bash
python combine_datasets.py [INPUT_DIRECTORY] [OUTPUT_FILE_NAME]
```

**Parameters**:
- `INPUT_DIRECTORY`: Directory containing the Parquet files to be combined.
- `OUTPUT_FILE_NAME`: Name of the output Parquet file that will store the combined dataset.

**Example**:
```bash
python combine_datasets.py datasets/yellow_tripdata/ combined_datasets/yellow_tripdata_2023.parquet
```

### 3. `print_schemas.py`

**Description**:  
This script prints the schema of all Parquet files in a given directory. It is useful for quickly inspecting the structure of multiple datasets, helping you understand the data before performing further processing.

**Usage**:  
```bash
python print_schemas.py [DIRECTORY_PATH]
```
**Parameters**:
- `DIRECTORY_PATH`: Directory containing the Parquet files whose schemas need to be printed.

**Example**:
```bash
python print_schemas.py datasets/yellow_tripdata/
```

### 4. `filter_parquet_fields.py`

**Description**:  
This script filters a given dataset by 2 mentioned fields (it excludes the rest of them from the resulted Parquet dataset)

**Usage**:  
```bash
python filter_parquet_fields.py [INPUT_PARQUET_PATH] [OUTPUT_PARQUET_PATH] [X_FIELD] [Y_FIELD]
```
**Parameters**:
- `INPUT_PARQUET_PATH`: The path to the input Parquet file. This is the file from which you want to extract specific fields.
- `OUTPUT_PARQUET_PATH`: The path where the output Parquet file will be saved. The output file will contain only the specified fields.
- `X_FIELD`: The name of the first field (column) to be included in the output file.
- `Y_FIELD`: The name of the second field (column) to be included in the output file.

**Example**:
```bash
python filter_parquet_fields.py ./combined_datasets/yellow_tripdata_2023.parquet ./filtered_fields_datasets/yellow_tripdata_2023_distance_amount trip_distance total_amount
```

### 5. `convert_parquet_to_csv.py`

**Description**:  
This script converts a Parquet file to a CSV file.

**Usage**:  
```bash
python convert_parquet_to_csv.py [INPUT_PARQUET_PATH] [OUTPUT_CSV_PATH] [OPTIONAL_PARTITIONS]
```
**Parameters**:
- `INPUT_PARQUET_PATH`: The path to the input Parquet file that you want to convert.
- `OUTPUT_CSV_PATH`: The path where the output CSV file will be saved. If multiple partitions are used, this will be the directory containing the part files.
- `OPTIONAL_PARTITIONS`: (Optional) Number of partitions to use for the conversion. If not provided, the data will be written to a single CSV file.

**Example**:
```bash
python convert_parquet_to_csv.py datasets/yellow_tripdata_2023-06.parquet output/yellow_tripdata_2023-06.csv 4
```

## Requirements

- Python 3.x
- PySpark
- Pandas (for any Pandas-based operations)
- Apache Hadoop 3.x or later
- Java Development Kit (JDK) 8 or later

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/naumencumihai/data-cleaning-services.git
   cd data-processing-services
   ```

2. **Install the required Python packages**:
    ```bash
    pip install pyspark pandas
    ```

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## Contact

For any questions or issues, please open an issue on the GitHub repository or contact the author at [naumencumihai@gmail.com].
