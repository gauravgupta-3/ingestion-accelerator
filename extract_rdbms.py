import pyodbc
from sqlalchemy import create_engine, text
import datetime
import pandas as pd
import logging
from dateutil import parser
from utils.db_Util.dbUtil import sql_server_connect
from utils.s3_Util.s3Util import s3_delete_file,s3_upload_file_parquet
from config.config import read_config
from prefect import flow, task, get_run_logger


# logger = logging.getLogger()
# logger.setLevel(level=logging.INFO)
# logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
#                     datefmt='%Y-%m-%d %H:%M:%S')



pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.precision", 3)




@flow(log_prints=True)
def extract_rdbms():
    logger = get_run_logger()
    ##Read configuration variables
    logger.info('Start execution to capture metadata details')
    config_data = read_config()
    driver = config_data['driver']
    host = config_data['host']
    user = config_data['user']
    pwd = config_data['pwd']
    metadata_database = config_data['metadata_database']
    metadata_rdbms_table = config_data['metadata_rdbms_table']
    ##Read dbms metadata
    cnxn_metadata = sql_server_connect(driver,host,user,pwd,metadata_database)
    sql_query_metadata = f"select * from {metadata_rdbms_table}"
    metadata_df = pd.read_sql(sql_query_metadata,cnxn_metadata)
    print("Metadata df:\n {}".format(metadata_df.to_string()))
    metadata_df['delay_in_seconds'] = metadata_df['delay_in_seconds'].fillna(0)
    for row in metadata_df.itertuples():
        if row.active_flag == 1 and row.src_type =='sql_server':
        ##Incremental load
            if row.load_type == 'incremental':
                file_name = row.src_table_name
                r_watermark_value = row.watermark_value - datetime.timedelta(seconds=row.delay_in_seconds)
                sql_query =f"select * from {row.src_table_name} where {row.watermark_column}>cast('{r_watermark_value}' as datetime2)"
                logger.info("Start incremental extraction from SQL server for table: {}".format(file_name))
                cnxn=sql_server_connect(driver,host,user,pwd,row.src_database)
                sql_watermark = f"select max({row.watermark_column}) as c1 from {row.src_table_name}" 
                sql_df = pd.read_sql(sql_query,cnxn)
                if not sql_df.empty:
                    watermark_updated_timestamp = pd.read_sql(sql_watermark,cnxn)
                    watermark_updated = watermark_updated_timestamp.iloc[0]['c1']
                    watermark_updated_n = watermark_updated+datetime.timedelta(milliseconds=5)
                    
                    file_timestamp = datetime.datetime.now().replace(microsecond = 0)  
                    #update statement
                    #cursor = cnxn_metadata.cursor()
                    update_sql = text(f'UPDATE {metadata_rdbms_table} SET watermark_value= :x WHERE object_id= :y')
                    val = {'x': watermark_updated_n, 'y':row.object_id} 
                    logger.info("Updated SQL {}".format(update_sql))
                    logger.info("VAL {}".format(val))
                    cnxn_metadata.execute(update_sql,val)
                    cnxn_metadata.commit()
                    logger.info("Updated watermark value:{} in table {}".format(watermark_updated_n,file_name)) 
                
                    u_file_name = row.src_table_name+'_'+str(file_timestamp)
                    extension = '.parquet'
                    s3_key = row.s3_dest_folder +'/'+row.src_name+'/'+file_name+'/'+u_file_name+extension
                    s3_upload_file_parquet(row.s3_dest_bucket_name,s3_key,sql_df,extension)
                else:
                    logger.info("No data extracted for this run for {} table for watermark value: {}".format(file_name,r_watermark_value))
                logger.info("Finish incremental extraction from SQL server for table: {}".format(file_name))
        ##Full load
            elif row.load_type == 'full':
                file_name = row.src_table_name
                sql_query =f"select * from {row.src_table_name}"
                logger.info("Start Full load extraction from SQL server for table: {}".format(file_name))
                cnxn=sql_server_connect(driver,host,user,pwd,row.src_database)
                sql_df = pd.read_sql(sql_query,cnxn)
                #print(sql_df.to_string())
                if sql_df.empty:
                    logger.info("No data extracted for this run for {} table {}".format(file_name))
                    
                else: 
                    file_name = row.src_table_name
                    extension = '.parquet'
                    s3_key = row.s3_dest_folder +'/'+row.src_name+'/'+file_name+'/'+file_name+extension
                    s3_delete_file(row.s3_dest_bucket_name,s3_key)
                    s3_upload_file_parquet(row.s3_dest_bucket_name,s3_key,sql_df,extension)
                logger.info("Finish Full load extraction from SQL server for table: {}".format(file_name))
        ##Snapshot load
            elif row.load_type == 'snapshot':
                file_name = row.src_table_name
                sql_query =f"select * from {row.src_table_name}"
                snapshot_timestamp = datetime.datetime.now().replace(microsecond = 0)
                logger.info("Start Snapshot extraction from SQL server for table: {}".format(file_name))
                cnxn=sql_server_connect(driver,host,user,pwd,row.src_database)
                sql_df = pd.read_sql(sql_query,cnxn)
                if sql_df.empty:
                    logger.info("No data extracted for this run for {} table {}".format(file_name))
                else: 
                    u_file_name = row.src_table_name+'_'+str(snapshot_timestamp)
                    extension = '.parquet'
                    s3_key = row.s3_dest_folder +'/'+row.src_name+'/'+file_name+'/'+u_file_name+'_'+row.load_type+extension
                    s3_upload_file_parquet(row.s3_dest_bucket_name,s3_key,sql_df,extension)
                logger.info("Finish Snapshot extraction from SQL server for table: {}".format(file_name)) 

if __name__ == "__main__":
    extract_rdbms()