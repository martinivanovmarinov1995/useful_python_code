from lib.common.utils import get_environment
from lib.common.logger import get_logger
from lib.common.const import INGESTION_BUCKET, INGESTION, DELTA
from lib.aws_utils.glue_utils import get_job_parameter, get_optional_job_parameter, get_optional_boolean_job_parameter
from lib.aws_utils.secrets_manager_client import SecretsManagerClient
from lib.aws_utils.redshift_client import RedshiftClient
from simple_salesforce import Salesforce
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from logging import Logger
import datetime
from typing import Dict, Any
from lib.gdpr.spark_wrapper import GdprWrapper


t = datetime.datetime.now()
print(t)
timestamp = t.strftime("%Y%m%d%H%M%S")
JOB_NAME_SHORT = "extraction"
table = 'Hierarchy'
stg_table = 'stg_table_archive'
MIO_NAME = 'MIO_Name'
SOURCE_NAME = 'sales_territory_riskconnect'
logger: Logger = get_logger(f'extraction_from_salesforce')


class JobParams:
    __slots__ = ["mio_number", "mio_name", "env", "load_type"]
    _boolean = {}
    _optional = {}

    def __init__(self, **kwargs):
        for field in self.__slots__:
            if field in kwargs:
                value = kwargs[field]
            elif field in self._boolean:
                value = get_optional_boolean_job_parameter(field, default=self._boolean[field])
            elif field in self._optional:
                value = get_optional_job_parameter(field, default=self._optional[field])
            else:
                value = get_job_parameter(field)
            setattr(self, field, value)

    def asdict(self):
        return {field: getattr(self, field) for field in self.__slots__}

    @staticmethod
    def parse_list(param_value):
        return [t.strip() for t in param_value.split(",") if t.strip()]


# function to create a DataFrame and drop unwanted attributes
def create_df(sf_data: Dict[str, Any], spark, env: str, gdpr: GdprWrapper, table_name):
    """ Create DF and Drop unwanted attributes """
    if 'records' in sf_data and sf_data['records']:
        for record in sf_data['records']:
            del record['attributes']
        schema = StructType([StructField(field, StringType(), True) for field in sf_data['records'][0]])
        df = spark.createDataFrame(sf_data['records'], schema=schema)
        df.selectExpr(
            [f"STRING(`{col}`)" for col in df.columns]
            + ["STRING(current_timestamp()) as mio_source_datetime"])
        if env != "PROD":
            df = gdpr.wrap_dataframe(df, table_name)
        return df
    else:
        logger.info('DataFrame is empty')


# function to connect to salesforce
def connect_to_salesforce(sm_client):
    """ Get Credentials from Secret Manager and Connect to Salesforce """
    # dda/dev/riskconnect/api user credentials
    secret_manager_key = f'dda/dev/riskconnect/api'
    credentials = sm_client.get_credentials(secret_manager_key)
    return Salesforce(username=credentials['username'], password=credentials['password'],
                      security_token=credentials['security_token'])


def get_predicate(client, table):
    """ Get the date on which the last record has been extracted """
    logger.info(f"Calculating the {table}'s last extracted date for predicate")
    cursor = client.get_cursor()
    cursor.execute(
        f"""
        SELECT CASE  WHEN MAX(NULLIF(LastModifiedDate, '')::TIMESTAMP) IS NULL 
               OR TIMESTAMP_CMP(MAX(NULLIF(createddate, '')::TIMESTAMP), MAX(NULLIF(LastModifiedDate, '')::TIMESTAMP))>=0 
               THEN MAX(NULLIF(createddate, '')) ELSE MAX(NULLIF(LastModifiedDate, '')) END AS dt  
        FROM spectrum_schema.{table} 
            """)
    result = cursor.fetchone()
    if not result or not result[0]:
        raise ValueError(
            f"No latest date found for spectrum_schema.{table}, please check if data is present in archive")
    result = str(result)
    start_index = result.index("'")
    end_index = result.index("'", start_index + 1)
    output_str = result[start_index + 1:end_index]
    return output_str


read_query = f"""
SELECT Id,
       OwnerId,
       IsDeleted,
       Name,
       CreatedDate,
       CreatedById,
       LastModifiedDate,
       LastModifiedById,
FROM table ;
"""


def main(params: JobParams):
    # get value for environment
    env = get_environment(params.env)
    env_dir = env['s3_env_dir']  # DEV

    # initiate clients
    spark = SparkSession.builder.getOrCreate()
    sm_client = SecretsManagerClient(env=env)
    sf_client = connect_to_salesforce(sm_client)
    gdpr = GdprWrapper('MIO_Number', SOURCE_NAME)

    counter = 0
    target_path = f's3://path/{env_dir}/{INGESTION}/{SOURCE_NAME}/Table_Name/{DELTA}/'
    if params.load_type.upper() == 'FULL':
        logger.info(f'Reading query for Hierarchy Node: {read_query}')

        last_mod_filter = f" "
        # read file and replace with to_append
        new_sql_file = read_query.replace(';', last_mod_filter)
        logger.info(f"SOQL Query: {new_sql_file}")

        # query the data from salesforce
        sf_data = sf_client.query_all(new_sql_file)

        # create a DataFrame
        df = create_df(sf_data, spark, env=env_dir, gdpr=gdpr, table_name=table)

        counter += df.count()

        logger.info(f'Total number of records is {counter}')
        logger.info(f'Saving parquet files into {target_path}')

        # write the DataFrame in the output_location
        df.write.parquet(target_path, mode='overwrite')

    if params.load_type.upper() == 'DELTA':

        # create RedshiftClient
        rs_client = RedshiftClient(sm_client=sm_client, s3_client=None)

        logger.info(f'Reading query for Hierarchy Node: {read_query}')

        # get lastmodifieddate from stg_table
        output_str = get_predicate(client=rs_client, table=stg_table)

        last_mod_filter = f"where LastModifiedDate > {output_str} or CreatedDate > {output_str} "

        # read file and replace with to_append
        new_sql_file = read_query.replace(';', last_mod_filter)
        logger.info(f"SOQL Query: {new_sql_file}")

        # query the data from salesforce
        sf_data = sf_client.query_all(new_sql_file)

        # check output row count
        if sf_data['totalSize'] > 0:
            # create a DataFrame
            df = create_df(sf_data, spark, env=env_dir, gdpr=gdpr, table_name=table)

            # write the DataFrame in the output_location
            df.write.parquet(target_path, mode='overwrite')

            counter += df.count()

        logger.info(f'Total number of records is {counter}')
        logger.info(f'Saving parquet files into {target_path}')

    logger.info(f'Extraction completed')


if __name__ == '__main__':
    main(JobParams())
