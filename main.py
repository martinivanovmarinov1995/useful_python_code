import pandas as pd
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, ceil
import json

spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

ACCESS_KEY = 'AKIAQXDDMEB35SLU2DTP'
SECRET_KEY = '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'

client = boto3.client('s3',
                      aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY, )

AWS_S3_BUCKET = 'awsdataengineer123'

resource = client.list_objects(Bucket=AWS_S3_BUCKET)

for item in resource.get('Contents'):
    data = client.get_object(Bucket=AWS_S3_BUCKET, Key=item.get('Key'))
    content = data.get('Body').read()
    json_data = json.loads(content)
    pandas_df = pd.json_normalize(json_data)
    spark_df = spark.createDataFrame(pandas_df)
    updated_df = spark_df.withColumn("Year", col("Year").cast("int"))
    updated_df = updated_df.withColumn('Century', ceil(col('Year') / 100))
    #updated_df = updated_df.select('Century').distinct()
    updated_df.show()
