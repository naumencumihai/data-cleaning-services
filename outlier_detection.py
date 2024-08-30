import os
import sys
import subprocess
import re
import pandas as pd
import uuid

def outlier_detection(input_path, output_dir_name, hdfs_path, stddev_threshold, x_field, y_field):
    df = pd.read_csv(input_path, low_memory=False)
    x_column = df.columns.get_loc(x_field) + 1
    y_column = df.columns.get_loc(y_field) + 1

    try:
        current_folder = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        current_folder = os.getcwd()

    directory_path = os.path.dirname(input_path)
    file_name = os.path.basename(input_path)

    no_header_input_path = f'{directory_path}\\NOHEADER_{file_name}'

    df.to_csv(no_header_input_path, index=True, header=False)
    df = pd.read_csv(no_header_input_path, low_memory=False)

    rand_name = uuid.uuid4()
    x_name = f'x_{rand_name}'
    y_name = f'y_{rand_name}'
    hdfs_input_path = f'{hdfs_path}/input/{rand_name}'
    hdfs_output_path_x = f'{hdfs_path}/output/{x_name}'
    hdfs_output_path_y = f'{hdfs_path}/output/{y_name}'

    hdfs_put_command = f'hdfs dfs -put {no_header_input_path} {hdfs_input_path}'
    print(hdfs_put_command)
    subprocess.run(hdfs_put_command, shell=True)

    # run mapreduce for x axis
    hadoop_execute_command_x = f'hadoop jar {current_folder}\implementations\outlier_detection\OutlierDetection.jar OutlierDetectionDriver {hdfs_input_path} {hdfs_output_path_x} {stddev_threshold} {x_column}'
    print(hadoop_execute_command_x)
    subprocess.run(hadoop_execute_command_x, shell=True)

    # run again for y axis
    hadoop_execute_command_y = f'hadoop jar {current_folder}\implementations\outlier_detection\OutlierDetection.jar OutlierDetectionDriver {hdfs_input_path} {hdfs_output_path_y} {stddev_threshold} {y_column}'
    print(hadoop_execute_command_y)
    subprocess.run(hadoop_execute_command_y, shell=True)

    x_output_dir = f'{current_folder}\\results\\{output_dir_name}\\x'
    y_output_dir = f'{current_folder}\\results\\{output_dir_name}\\y'

    if not os.path.exists(x_output_dir):
        os.makedirs(x_output_dir)
    if not os.path.exists(y_output_dir):
        os.makedirs(y_output_dir)

    hadoop_get_command_x = f'hdfs dfs -get -p {hdfs_output_path_x}/part-r-00000 {x_output_dir}'
    subprocess.run(hadoop_get_command_x, shell=True)
    print(hadoop_get_command_x)
    hadoop_get_command_y = f'hdfs dfs -get -p {hdfs_output_path_y}/part-r-00000 {y_output_dir}'
    subprocess.run(hadoop_get_command_y, shell=True)
    print(hadoop_get_command_y)

if __name__ == "__main__":
    if len(sys.argv) != 7:
        print("Usage: python outlier_detection.py [INPUT FILE PATH] [OUTPUT DIR NAME] [HDFS USER PATH] [STDDEV THRESHOLD] [X AXIS FIELD] [Y AXIS FIELD]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_dir_name = sys.argv[2]
    hdfs_path = sys.argv[3]
    stddev_threshold = sys.argv[4]
    x_field = sys.argv[5]
    y_field = sys.argv[6]

    outlier_detection(input_path, output_dir_name, hdfs_path, stddev_threshold, x_field, y_field)
