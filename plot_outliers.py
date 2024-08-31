import os
import pandas as pd
import matplotlib.pyplot as plt
import sys

def plot_outliers(input_path, x_field, y_field, sample_size):
    directory_path = os.path.dirname(input_path)

    # Load the processed data
    data = pd.read_csv(input_path, low_memory=False) 

    # Separate the data into outliers and non-outliers
    outliers = data[data['label'] == 'OUT']

    if (sample_size is not None):
        non_outliers = data[data['label'] != 'OUT'].sample(n=sample_size, random_state=1)
    else:
        non_outliers = data[data['label'] != 'OUT']

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 6))

    # Plot non-outliers on the first subplot
    ax1.scatter(non_outliers[x_field], non_outliers[y_field], color='blue', alpha=0.5)
    ax1.set_title('Non-Outliers')
    ax1.set_xlabel(x_field)
    ax1.set_ylabel(y_field)
    ax1.grid(True)

    # Plot outliers on the second subplot
    ax2.scatter(outliers[x_field], outliers[y_field], color='red', alpha=0.5)
    ax2.set_title('Outliers')
    ax2.set_xlabel(x_field)
    ax2.set_ylabel(y_field)
    ax2.grid(True)
    
    outliers_plot_path = os.path.join(directory_path, 'outliers_plot.png')
    plt.savefig(outliers_plot_path)
    plt.close()

if __name__ == "__main__":
    if len(sys.argv) < 4 or len(sys.argv) > 5:
        print("Usage: python plot_outliers.py [INPUT_CSV_PATH] [X_FIELD] [Y_FIELD] [SAMPLE_SIZE]")
        sys.exit(1)

    input_csv_path = sys.argv[1]
    x_field = sys.argv[2]
    y_field = sys.argv[3]
    if (len(sys.argv) == 5):
        sample_size = int(sys.argv[4])
    else:
        sample_size = None

    plot_outliers(input_csv_path, x_field, y_field, sample_size)