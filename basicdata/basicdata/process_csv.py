import pandas as pd

df=pd.read_csv('basicdata/basicdata/dataset/SalesTransactions/SalesTransactions.csv',
               sep=',',encoding='utf-8',low_memory=False)
print(df)