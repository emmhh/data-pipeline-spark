import pyspark as ps
import pandas as pd

# print(pd.read_csv('./tmp.csv')[5:7])

spark = ps.sql.SparkSession.builder.master('local').appName('test').getOrCreate()
sc = spark.sparkContext
print(sc)