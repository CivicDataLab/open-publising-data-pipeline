import pandas as pd

# data = pd.read_csv('sample.csv')
data = pd.read_csv("https://docs.google.com/spreadsheets/d/1z52IRRcn3Q6AFdVATi0WaLpF8MoxopqU/export?format=csv")
print(data)
column = 'Head of Account'
sep = '-'
data[["Major Head Code","Sub Major Head Code","Minor Head Code","Sub Head Code","Sub Sub Head Code","Detail Head Code","Sub Detail Head Code"]] = data[column].str.split(sep, expand=True)
print(data)