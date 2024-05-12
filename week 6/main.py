import dask.dataframe as dd
import time
import os

__author__ = 'Darshan Dhanani'

FILE_NAME = "D:/jainam/nifty_data/result/result/option/BANKNIFTY_OPTION.csv"

start_time = time.time()

# dataframe = dd.read_csv(FILE_NAME)  # dask


print(len(dataframe))

execution_time = time.time() - start_time
print("---------------------------------------------")
print(f"Execution Time: {execution_time:.4f} seconds")


