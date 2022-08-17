from lib2to3.pgen2 import driver
from plistlib import UID
import sqlalchemy 
import create_engine
import pandas as pd
import pyodbc
import os

#get password from environment variable
pwd = os.environ['PWD']
uid = os.environ['UID']

#sql db connection details
driver = '{ODBC Driver 17 for SQL Server}'
server = 'localhost'
database = 'ETL'

#extract data from sql server
def extract():
        src_conn = pyodbc.connect(
        'DRIVER=' + driver + ';SERVER=' + server + '\SQLEXPRESS' + ';DATABASE=' + database + ';UID=' + uid + ';PWD=' + pwd)
        src_cursor = src_conn.cursor()
        src_cursor.execute(""" select  t.name as table_name
                            from sys.tables t 
                            where t.name in ('DimProduct','DimProductSubcategory','DimProductSubcategory','DimProductCategory','DimSalesTerritory','FactInternetSales') """)
        src_tables = src_cursor.fetchall()
        for tbl in src_tables:
            df = pd.read_sql_query(f'select * FROM {tbl[0]}', src_conn)
            load(df, tbl[0])
    except Exception as e:\
    print("Data extract error: " + str(e))
    finally:
src_conn.close()


#load data to postgresql
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/AdventureWorks')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        df.to_sql(f'stg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        print("Data imported successful")
    except Exception as e:
        print("Data load error: " + str(e))