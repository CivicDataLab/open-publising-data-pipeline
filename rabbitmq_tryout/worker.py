import re

import pandas as pd


pattern = re.compile('[/$&()*^%#@!]+')
# data = pd.read_csv('sample.csv')
data = pd.read_csv("https://docs.google.com/spreadsheets/d/1z52IRRcn3Q6AFdVATi0WaLpF8MoxopqU/export?format=csv")
print(data)
columns = ["Major Head Description","Sub Major Head Description","Minor Head Description","Sub Head Description","Sub Sub Head Description","Detail Head Description","Sub Detail Head description"]
output_column = "Head Description in English"
separator = "$"
data[output_column] = data[columns].astype(str).agg(f"""{separator}""".join, axis=1)
print(data)