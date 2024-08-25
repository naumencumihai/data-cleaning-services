# Data Processing Scripts

This repository contains a set of Python scripts designed to manage and process large datasets in Parquet format using PySpark. The scripts included in this repository allow for schema modification, dataset combination, and schema inspection, making it easier to handle and analyze large-scale data.

## Table of Contents

- [Scripts Overview](#scripts-overview)
  - [modify_schema.py](#1-modify_schemapy)
  - [combine_datasets.py](#2-combine_datasetspy)
  - [print_schemas.py](#3-print_schemaspy)
  - [filter_parquet_fields.py](#4-filter_parquet_fieldspy)
  - [convert_parquet_to_csv.py](#5-convert_parquet_to_csvpy)
- [Requirements](#requirements)
- [Installation](#installation)
- [License](#license)
- [Contact](#contact)

## Scripts Overview

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
