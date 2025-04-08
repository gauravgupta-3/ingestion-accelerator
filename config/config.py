import configparser
import os
import boto3
from prefect import flow, task, get_run_logger


s3_boto = boto3.client('s3')
configuration_file_bucket = "ingestion-accelerator-prefect"
configuration_file_key = "config/config.ini"
obj = s3_boto.get_object(Bucket=configuration_file_bucket, Key=configuration_file_key)

@task
def read_config():
    logger = get_run_logger()
    try:
        config = configparser.ConfigParser()
        config.read_string(obj['Body'].read().decode())
        #print("Obj", config.read_string(obj['Body'].read().decode()))
        driver = config.get('SQL Server Database', 'driver')
        host = config.get('SQL Server Database', 'host')
        metadata_database = config.get('SQL Server Database', 'metadata_database')
        metadata_rdbms_table = config.get('SQL Server Database', 'metadata_rdbms_table')
        metadata_file_table = config.get('SQL Server Database', 'metadata_file_table')
        metadata_api_table = config.get('SQL Server Database', 'metadata_api_table')
        config_values ={
            'driver': driver,
            'host': host,
            'metadata_database': metadata_database,
            'metadata_rdbms_table': metadata_rdbms_table,
            'metadata_file_table': metadata_file_table,
            'metadata_api_table': metadata_api_table        
        }
        logger.info('Successfully captured configuration details')
        return config_values
    except Exception as e:
        logger.error(e)
        raise e
