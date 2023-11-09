import pandas as pd
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, ceil
import json
import os
import requests

pd.DataFrame.iteritems = pd.DataFrame.items

AWS_S3_BUCKET = 'awsdataengineer123'
DATASET = 'photos'
URL = f'https://jsonplaceholder.typicode.com/{DATASET}'
ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')

# def http_get(URL):
#     request = requests.get(URL)
#     return json.loads(request.text)


def data_transformation(client, bucket, spark):
    resource = client.list_objects(Bucket=bucket)
    for item in resource.get('Contents'):
        data = client.get_object(Bucket=bucket, Key=item.get('Key'))
        content = data.get('Body').read()
        json_data = json.loads(content)
        pandas_df = pd.json_normalize(json_data)
        spark_df = spark.createDataFrame(pandas_df)
        updated_df = spark_df.withColumn("Year", col("Year").cast("int"))
        updated_df = updated_df.withColumn('Century', ceil(col('Year') / 100))
        # updated_df = updated_df.select('Century').distinct()
        return updated_df


def main():
    # data = http_get(URL)
    # dataframe = pd.DataFrame.from_dict(data)
    # dataframe.to_json('s3://awsdataengineer123/archive/photos.json')

    # conf
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    client = boto3.client('s3',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY, )

    data = data_transformation(client=client, bucket=AWS_S3_BUCKET, spark=spark)

    # create temp view
    data.createOrReplaceTempView('people')
    spark.sql('Select distinct Century from people where Century = "20" ').show()


if __name__ == "__main__":
    main()

