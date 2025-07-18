import os
import pandas as pd

def convert_csv_to_parquet(root_dir='.'):
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.csv'):
                csv_path = os.path.join(subdir, file)
                try:
                    print(f"Processing: {csv_path}")
                    df = pd.read_csv(csv_path)
                    
                    parquet_path = os.path.splitext(csv_path)[0] + '.parquet'
                    df.to_parquet(parquet_path, compression='snappy')
                    print(f"Saved to: {parquet_path}")
                except Exception as e:
                    print(f"Failed to process {csv_path}: {e}")

def convert_parquet_to_csv(root_dir='.'):
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_path = os.path.join(subdir, file)
                try:
                    print(f"Processing: {parquet_path}")
                    df = pd.read_parquet(parquet_path)
                    
                    parquet_path = os.path.splitext(parquet_path)[0] + '.csv'
                    df.to_csv(parquet_path, index=False)
                    print(f"Saved to: {parquet_path}")
                except Exception as e:
                    print(f"Failed to process {parquet_path}: {e}")

# Run the conversion in the current directory
if __name__ == '__main__':
    convert_parquet_to_csv()
