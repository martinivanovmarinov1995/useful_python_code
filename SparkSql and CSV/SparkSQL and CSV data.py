from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, TimestampType

schema = StructType([
    StructField("VendorID", StringType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", StringType(), True),
    StructField("trip_distance", StringType(), True),
    StructField("pickup_longitude", StringType(), True),
    StructField("pickup_latitude", StringType(), True),
    StructField("RatecodeID", StringType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", StringType(), True),
    StructField("payment_type", StringType(), True),
    StructField("fare_amount", StringType(), True),
    StructField("extra", StringType(), True),
    StructField("mta_tax", StringType(), True),
    StructField("tip_amount", StringType(), True),
    StructField("tolls_amount", StringType(), True),
    StructField("improvement_surcharge", StringType(), True),
    StructField("total_amount", StringType(), True)
])


spark = SparkSession.builder.getOrCreate()
url = 'csv_data\\uber_data.csv'

# Use the DataFrameReader to read the CSV file and apply the schema
spark_df_reader = spark.read.format("csv").option("header", True).schema(schema)

# Call load() to actually read the data and create a DataFrame
spark_df = spark_df_reader.load(url)

print(spark_df.printSchema())
print(spark_df.show())