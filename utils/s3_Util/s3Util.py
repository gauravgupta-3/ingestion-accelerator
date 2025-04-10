import boto3
import datetime
import pandas as pd
import logging
import pyarrow as pa
import pyarrow.parquet as pq
import io
from prefect import flow, task, get_run_logger
from prefect_aws import AwsCredentials

# logger = logging.getLogger()
# logger.setLevel(level=logging.INFO)
# logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S')

aws_cred_block = AwsCredentials.load("aws-credentials")
session = aws_cred_block.get_boto3_session()
s3 = session.resource("s3")
            
#s3 = boto3.resource('s3')     
             
@task                    
def s3_delete_file(s3_bucket_name,s3_key):
    logger = get_run_logger()
    """ Delete files from a bucket.

        :param bucket_name: Bucket name
        :param file_key: key to be deleted
    """
    logger.info('Deleting file if exists: {}'.format(s3_key))
    s3.Object(s3_bucket_name, s3_key).delete()
    return True

@task
def s3_upload_file_parquet(s3_bucket_name,s3_key,df,extension):
    logger = get_run_logger()
    """ Convert to parquet and upload files to s3 bucket.

        :param bucket_name: Bucket name
        :param file_key: key to be uploaded
        :df: data to be converted to parquet
    """   
    if extension =='.parquet':
        parquet_key = s3_key
    else:
        parquet_key = s3_key.replace(extension, '.parquet')
    table = pa.Table.from_pandas(df)
    parquet_buffer = io.BytesIO()
    pq.write_table(table, parquet_buffer)
    data = parquet_buffer.getvalue()
    parquet_buffer.seek(0)
    #s3 = boto3.resource('s3')
    s3.Object(s3_bucket_name,parquet_key).put(Body=data)
    logger.info('File uploaded successfully to s3: {}'.format(parquet_key))

