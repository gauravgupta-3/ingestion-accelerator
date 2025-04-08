import os 
import glob 
import pandas as pd
import boto3
import pyarrow.parquet as pa
import io
import shutil
import logging
from utils.s3_Util.s3Util import s3_delete_file,s3_upload_file_parquet
from utils.db_Util.dbUtil import sql_server_connect
from config.config import read_config
from prefect import flow, task, get_run_logger

# logger = logging.getLogger()
# logger.setLevel(level=logging.INFO)
# logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S')

@task
def archive_local_src_file(local_file_path,source_archival_path,file_name):
    logger = get_run_logger()
    try:
        shutil.move(local_file_path,source_archival_path)
        logger.info('File Successfully Archived: {}'.format(file_name))
    except Exception as ex:
        logger.error("File cannot be archived")
        logger.error(ex)

@flow(log_prints=True)
def extract_shared_file():
    logger = get_run_logger()
    logger.info('Start execution to capture metadata details')
    ##Read configuration variables
    ##Read configuration variables
    config_data = read_config()
    driver = config_data['driver']
    host = config_data['host']
    metadata_database = config_data['metadata_database']
    metadata_file_table = config_data['metadata_file_table']
    ##Read dbms metadata
    cnxn_metadata = sql_server_connect(driver,host,metadata_database)
    sql_query_metadata = f"select * from {metadata_file_table}"
    metadata_df = pd.read_sql(sql_query_metadata,cnxn_metadata)
    print("Metadata df:\n {}".format(metadata_df.to_string()))
    for row in metadata_df.itertuples():
        if row.active_flag == 1 and row.src_type =='shared_windows_path':
            for root, dirs, files in os.walk(row.src_file_path,topdown=True):
                if files:
                    for file_name in files:
                        local_file_path = os.path.join(root, file_name)
                        basename, extension = os.path.splitext(file_name)
                        source_archive_path_dir = row.src_archive_path
                        source_archival_full_path = os.path.join(source_archive_path_dir, file_name)
                        
                        isExist = os.path.exists(source_archive_path_dir)
                        if not isExist:
                            os.makedirs(source_archive_path_dir)

                        s3_key = row.s3_dest_folder +'/'+row.src_name+'/'+file_name
                        #print("Filename", file_name)
                        #print("S3 key:", s3_key)
                        if extension =='.csv':
                            with open(local_file_path) as f:
                                encode = str(f).split("encoding=",1)[1][:-1]
                                df = pd.read_csv(local_file_path, encoding=encode)
                            logger.info("Started CSV file extraction {}".format(file_name))                         
                            s3_upload_file_parquet(row.s3_dest_bucket_name,s3_key,df,extension)
                            archive_local_src_file(local_file_path,source_archival_full_path,file_name)
                            logger.info("Finshed CSV file extraction {}".format(file_name))  
                                
                        elif extension =='.json':
                            df = pd.read_json(local_file_path)
                            logger.info("Started Json file extraction {}".format(file_name))                          
                            s3_upload_file_parquet(row.s3_dest_bucket_name,s3_key,df,extension)
                            archive_local_src_file(local_file_path,source_archival_full_path,file_name)
                            logger.info("Finished Json file extraction {}".format(file_name))  

                        elif extension =='.txt':
                            df = pd.read_csv(local_file_path,sep=" ")
                            logger.info("Started Text file extraction {}".format(file_name))                      
                            s3_upload_file_parquet(row.s3_dest_bucket_name,s3_key,df,extension)
                            archive_local_src_file(local_file_path,source_archival_full_path,file_name)
                            logger.info("Finished Text file extraction {}".format(file_name))  

                        elif extension =='.parquet':
                            table = pa.read_table(local_file_path)
                            df = table.to_pandas()
                            logger.info("Started Parquet file extraction {}".format(file_name)) 
                            s3_upload_file_parquet(row.s3_dest_bucket_name,s3_key,df,extension)
                            archive_local_src_file(local_file_path,source_archival_full_path,file_name)
                            logger.info("Finished Parquet file extraction {}".format(file_name)) 
                else:
                    logger.info("File does not exist at path {}".format(row.src_file_path))


if __name__ == "__main__":
    extract_shared_file()



    











