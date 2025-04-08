import requests
import pandas as pd
import logging
from utils.s3_Util.s3Util import s3_delete_file,s3_upload_file_parquet
from utils.db_Util.dbUtil import sql_server_connect
from config.config import read_config
from io import StringIO
import datetime
from prefect import flow, task

logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

pd.set_option("display.max_rows", None)
pd.set_option("display.max_columns", None)
pd.set_option("display.precision", 3)

@flow(log_prints=True)
def extract_api():
    config_data = read_config()
    driver = config_data['driver']
    host = config_data['host']
    metadata_database = config_data['metadata_database']
    metadata_api_table = config_data['metadata_api_table']
    ##Read dbms metadata
    cnxn_metadata = sql_server_connect(driver,host,metadata_database)
    sql_query_metadata = f"select * from {metadata_api_table}"
    metadata_df = pd.read_sql(sql_query_metadata,cnxn_metadata)
    print("Metadata df:\n {}".format(metadata_df.to_string()))
    for row in metadata_df.itertuples():
        if row.active_flag == 1 and row.src_type =='api':

            try:
                # Make a GET request to the API endpoint using requests.get()
                url = row.api_url
                response = requests.get(url)

                # Check if the request was successful (status code 200)
                logger.info("Started API data extraction {}".format(url))
                if response.status_code == 200:
                    #data = response.json()
                    #print(data)
                    #print("response",response.text)
                    df = pd.read_json(StringIO(response.text),typ='dictionary').to_frame()
                    #print("Dataframe",df)
                    extension = 'parquet'
                    increment_time = datetime.datetime.now().replace(microsecond = 0)
                    file_name = row.s3_dest_file_name
                    u_file_name  = file_name+'_'+str(increment_time)
                    s3_key = row.s3_dest_folder +'/'+row.src_name+'/'+file_name+'/'+u_file_name+extension
                    s3_upload_file_parquet(row.s3_dest_bucket_name,s3_key,df,extension)
                else:
                    logger.error(response.status_code)
                    print('Error:', response.status_code)

                logger.info("Finished API data extraction {}".format(url))
            except requests.exceptions.RequestException as e:

                # Handle any network-related errors or exceptions
                logger.error(e)



if __name__ == '__main__':
    extract_api()
