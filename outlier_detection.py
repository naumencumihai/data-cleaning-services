import os
import sys
import subprocess
import re
import pandas as pd
import uuid

def compute_label(frequency, threshold):
    return 'OUT' if frequency <= threshold else ''

def process_outliers(initial_csv_path, x_col_pos, y_col_pos, x_field, y_field, x_freq_table_path, y_freq_table_path, output_csv_path, threshold):
    print("Processing results...")

    initial_df = pd.read_csv(initial_csv_path, low_memory=False, usecols=[0, x_col_pos, y_col_pos], names=['index', x_field, y_field])
    
    # Convert columns to integers to match the frequency tables
    initial_df['x int value'] = initial_df[x_field].astype(int)
    initial_df['y int value'] = initial_df[y_field].astype(int)

    x_freq_df = pd.read_csv(x_freq_table_path, delimiter='\t', names=['x int value', 'x freq'])
    y_freq_df = pd.read_csv(y_freq_table_path, delimiter='\t', names=['y int value', 'y freq'])

    x_freq_df['label_x'] = x_freq_df['x freq'].apply(lambda freq: compute_label(freq, threshold))
    y_freq_df['label_y'] = y_freq_df['y freq'].apply(lambda freq: compute_label(freq, threshold))

    # Merge the initial data with the frequency tables on the x and y integer values
    merged_df = initial_df.merge(x_freq_df[['x int value', 'label_x']], how='left', on='x int value')
    merged_df = merged_df.merge(y_freq_df[['y int value', 'label_y']], how='left', on='y int value')

    merged_df['label'] = merged_df.apply(
        lambda row: 'OUT' if row['label_x'] == 'OUT' or row['label_y'] == 'OUT' else '', axis=1
    )

    final_df = merged_df[['index', x_field, y_field, 'label']]

    final_df.to_csv(output_csv_path, index=False)

    print(f"Output CSV file created at: {output_csv_path}")

def file_exists_in_directory(directory, filename):
    for existing_filename in os.listdir(directory):
        if existing_filename == filename:
            return True
    return False

def outlier_detection(hdfs_user_path, hdfs_input_file_path, local_input_path, output_dir_name, x_field, y_field, freq_threshold):
    
    ### Setup

    directory_path = os.path.dirname(local_input_path)
    file_name = os.path.basename(local_input_path)

    no_header_file_name = f'NOHEADER_{file_name}'
    no_header_input_path = f'{directory_path}\\NOHEADER_{file_name}'
    
    hdfs_output_dir = f'{hdfs_user_path}/output/{output_dir_name}'

    rand_name = uuid.uuid4()
    x_name = f'x_{rand_name}'
    y_name = f'y_{rand_name}'
    hdfs_output_path_x = f'{hdfs_output_dir}/{x_name}'
    hdfs_output_path_y = f'{hdfs_output_dir}/{y_name}'

    hdfs_generated_input_path = None
    if (hdfs_input_file_path == 'SKIP'):
        hdfs_generated_input_path = f'{hdfs_user_path}/input/{rand_name}'

    # Create the indexed file without header, if it does not exists
    if (not file_exists_in_directory(directory_path, no_header_file_name)):
        df = pd.read_csv(local_input_path, low_memory=False)
        df.to_csv(no_header_input_path, index=True, header=False)

        # Upload source file indexed and without header
        hdfs_put_command = f'hdfs dfs -put {no_header_input_path} {hdfs_generated_input_path if hdfs_generated_input_path else hdfs_input_file_path}'
        print(hdfs_put_command)
        subprocess.run(hdfs_put_command, shell=True)
    else:
        df = pd.read_csv(local_input_path, nrows=0)
    
    x_column = df.columns.get_loc(x_field) + 1
    y_column = df.columns.get_loc(y_field) + 1


    ### Hadoop command execution

    try:
        current_folder = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        current_folder = os.getcwd()

    # run avf mapreduce job 1 for x axis
    hadoop_execute_command_x = f'hadoop jar {current_folder}\\implementations\\avf_outlier_detection_job1\\AvfOutlierDetectionJob1.jar AvfOutlierDetectionJob1Driver {hdfs_generated_input_path if hdfs_generated_input_path else hdfs_input_file_path} {hdfs_output_path_x} {x_column}'
    print(hadoop_execute_command_x)
    subprocess.run(hadoop_execute_command_x, shell=True)

    # run again for y axis
    hadoop_execute_command_y = f'hadoop jar {current_folder}\\implementations\\avf_outlier_detection_job1\\AvfOutlierDetectionJob1.jar AvfOutlierDetectionJob1Driver {hdfs_generated_input_path if hdfs_generated_input_path else hdfs_input_file_path} {hdfs_output_path_y} {y_column}'
    print(hadoop_execute_command_y)
    subprocess.run(hadoop_execute_command_y, shell=True)

    output_dir = f'{current_folder}\\results\\{output_dir_name}'

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    hadoop_get_command = f'hdfs dfs -get -p {hdfs_output_dir} {current_folder}\\results'
    print(hadoop_get_command)
    subprocess.run(hadoop_get_command, shell=True)


    ### Process data

    process_outliers(no_header_input_path, x_column, y_column, x_field, y_field, f'{output_dir}\\{x_name}\\part-r-00000', f'{output_dir}\\{y_name}\\part-r-00000', f'{output_dir}\\labeled_result.csv', freq_threshold)

if __name__ == "__main__":
    if len(sys.argv) < 7 or len(sys.argv) > 8:
        print("Usage: python outlier_detection.py [HDFS USER PATH] [HDFS INPUT FILE PATH | OPTIONAL: type SKIP if not available] [LOCAL INPUT FILE PATH] [OUTPUT DIR NAME] [X AXIS FIELD] [Y AXIS FIELD] [FREQ THRESHOLD | DEFAULT: 1] ")
        sys.exit(1)
    
    hdfs_user_path = sys.argv[1]
    hdfs_input_file_path = sys.argv[2]
    local_input_path = sys.argv[3]
    output_dir_name = sys.argv[4]
    x_field = sys.argv[5]
    y_field = sys.argv[6]
    if len(sys.argv) == 8:
        freq_threshold = int(sys.argv[7])
    else:
        freq_threshold = 1

    outlier_detection(hdfs_user_path, hdfs_input_file_path, local_input_path, output_dir_name, x_field, y_field, freq_threshold) 
