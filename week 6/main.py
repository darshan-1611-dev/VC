import time
import yaml
import dask.dataframe as dd

__author__ = 'Darshan Dhanani'

start_time = time.time()

def column_name_modification(df):
    df.columns.tolist()

    new_column_list = []

    for string  in df.columns.tolist():
        new_column_list.append(''.join(e for e in string if e.isalnum()).lower())

    df.columns = new_column_list
    
    return df


def column_name_validation(df_columns, config_data_columns):
    if len(df_columns) == len(config_data_columns) and list(config_data_columns)  == list(df_columns):
        return 1
    else:
        return 0

# Read YAML file
with open("file.yaml", 'r') as stream:
    data_loaded = yaml.safe_load(stream)
    
    file_name = data_loaded['file_name'] + '.' + data_loaded['file_type']
    config_data_columns = data_loaded['columns']
    
df = dd.read_csv(file_name)  # read file with dask 

df = column_name_modification(df)  # change column name 
    
# validation of column name 
if column_name_validation(df.columns.tolist(), config_data_columns):  
    print("Columns Validation passed")
else:
    print("Columns Validation failed")
    
execution_time = time.time() - start_time
print("---------------------------------------------")
print(f"Execution Time: {execution_time:.4f} seconds")
