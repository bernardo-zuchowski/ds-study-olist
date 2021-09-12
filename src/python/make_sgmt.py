import os
import sqlalchemy
import argparse
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__) ) ) ) # base directory where python works
DATA_DIR = os.path.join( BASE_DIR, 'data' ) # data directory where is the data
SQL_DIR = os.path.join( BASE_DIR, 'src', 'sql' )

with open( os.path.join( SQL_DIR, 'segments.sql') ) as query_file:
    query = query_file.read()

#open conection with database
str_connection = 'sqlite:///{path}' #define a string of connection
str_connection = str_connection.format( path = os.path.join(DATA_DIR, 'olist.db') )
connection = sqlalchemy.create_engine( str_connection )

create_query = f'''
CREATE TABLE  tb_seller_sgmt AS
{query}
;'''

insert_query = f'''
DELETE FROM tb_seller_sgmt WHERE dt_sgmt = '{date}'
INSERT INTO tb_seller_sgmt SELECT
{query}
;'''

connection.execute( create_query )