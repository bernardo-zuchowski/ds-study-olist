import os
import pandas as pd
import sqlalchemy


BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__) ) ) ) # base directory where python works
DATA_DIR = os.path.join( BASE_DIR, 'data' ) # data directory where is the data

files_names = [ i for i in os.listdir( DATA_DIR ) if i.endswith('.csv') ] # list compression to iterate and indentify only files if they ends .csv

#open conection with database
str_connection = 'sqlite:///{path}' #define a string of connection
str_connection = str_connection.format( path = os.path.join(DATA_DIR, 'olist.db') )
connection = sqlalchemy.create_engine( str_connection )

# each file .csv it has a insertion on database
for i in files_names:
    print(i)
    df_tmp = pd.read_csv( os.path.join( DATA_DIR, i ) ) #read csv tables on DATA_DIR
    table_name= "tb_" + i.strip(".csv").replace("olist_", "").replace("_dataset", "") #rename replacing
    df_tmp.to_sql( table_name,
                   connection,
                   if_exists='replace',
                   index=False )