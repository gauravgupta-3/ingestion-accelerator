import pyodbc
import sqlalchemy as sa
from sqlalchemy import create_engine
import datetime
import pandas as pd
import logging

from prefect import flow, task

logger = logging.getLogger()
logger.setLevel(level=logging.INFO)
logging.basicConfig(format='%(asctime)s %(levelname)s:%(name)s:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')

            
    
             
@task                    
def sql_server_connect(driver,host,src_database):
    try:
        #cnxn = pyodbc.connect(driver = driver , host = host, database = src_database, Trusted_Connection='yes')
        url = f'mssql+pyodbc://@' + host + '/' + src_database + '?trusted_connection=yes&driver=ODBC+Driver+17+for+SQL+Server'
        engine = sa.create_engine(url)
        cnxn = engine.connect()
    except Exception as e:
        logger.error(e)
        raise e
    else:
        return cnxn