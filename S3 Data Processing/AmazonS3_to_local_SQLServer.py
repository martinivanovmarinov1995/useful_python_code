import pandas as pd
import boto3
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, ceil
import json
import pyodbc
import os
pd.DataFrame.iteritems = pd.DataFrame.items

AWS_S3_BUCKET = 'awsdataengineer123'
DATASET = 'photos'
URL = f'https://jsonplaceholder.typicode.com/{DATASET}'
ACCESS_KEY = os.environ.get('AWS_ACCESS_KEY_ID')
SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')


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


def sql_server_commit(spark_view):
    conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'
                          'Server=(localdb)\MSSQLLocalDB;'
                          'Database=mydb;'
                          'username=localhost;'
                          'TrustedConnection=yes;')
    cursor = conn.cursor()
    cursor.execute("""  DROP TABLE [mydb].[dbo].[employees];
                            CREATE TABLE [mydb].[dbo].[employees]
                            (
                            country_name varchar(max),
                            country_code varchar(max),
                            year integer,
                            value varchar(max),
                            century varchar(max)
                            )
                            """)
    conn.commit()

    insert_query = 'INSERT INTO  [mydb].[dbo].[employees] (country_name, country_code, year, value, century) values (' \
                   '?,?,?,?,?) '

    # Write to SQL Table
    for row in spark_view.rdd.collect():
        cursor.execute(insert_query, row['Country Name'], row['Country Code'], row['Year'], row['Value'],
                       row['Century'])
        conn.commit()


# def move_files_to_archive(spark, updated_df, client, bucket):


def main():
    # configurations
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    client = boto3.client('s3',
                          aws_access_key_id=ACCESS_KEY,
                          aws_secret_access_key=SECRET_KEY, )

    data = data_transformation(client=client, bucket=AWS_S3_BUCKET, spark=spark)

    #data2 = data.repartition(16)

    sql_server_commit(data)


if __name__ == "__main__":
    main()