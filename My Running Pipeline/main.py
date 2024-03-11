import pandas as pd
import glob
import os
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, when, StringType
from pyspark.sql.types import StructType, StructField, IntegerType

data = r'source_data/'

all_files = glob.glob(os.path.join(data, '*.csv'))
df_list = []
for file in all_files:
    data = pd.read_csv(file)
    df_list.append(data)
df = pd.concat(df_list, ignore_index=True)

df = df.applymap(lambda x: x.replace(" km", ''))

df["Total Distance"] = pd.to_numeric(df["Total Distance"], errors="coerce")
df["Year"] = df["Time Period"].str.split(" ").str[1]
df = df[["Year", "Total Distance"]]
df = df.groupby("Year").agg({"Total Distance": "sum"}).sort_values(by="Year", ascending=False)
print(df)