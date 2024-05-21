import dask.dataframe as dd
import time
import os

__author__ = 'Darshan Dhanani'

FILE_NAME = "D:/jainam/nifty_data/result/result/option/BANKNIFTY_OPTION.csv"

start_time = time.time()

def read_file_metadata(filepath):
    #dictionary
    firstDict = {}
        
    #main library that holds stats
    stats = os.stat(filepath)
        
    attrs = {
        'File Name': stats.st_name,
        'Size (KB)': sizeFormat(stats.st_size),
        'Creation Date': timeConvert(stats.st_ctime),
        'Modified Date': timeConvert(stats.st_mtime),
        'Last Access Date': timeConvert(stats.st_atime),
        }
  
    firstDict[name] = attrs 

    
    return firstDict 
    
# read_file_metadata(FILE_NAME) 
# exit()
df = dd.read_csv(FILE_NAME, header=None)  # dask 

print(df.head(2))

# print(len(dataframe))

execution_time = time.time() - start_time
print("---------------------------------------------")
print(f"Execution Time: {execution_time:.4f} seconds")
