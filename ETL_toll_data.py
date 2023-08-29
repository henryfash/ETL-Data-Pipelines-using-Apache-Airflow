# import the libraries

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import tarfile
import pandas as pd
import mysql.connector as msql

# define functions
def download_tolldata():
    response = requests.get("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz")
    with open("tolldata.tgz", "wb") as file:
        file.write(response.content)

def unzip_tolldata():
    with tarfile.open("tolldata.tgz", "r:gz") as tar:
        tar.extractall()

def extract_from_csv():
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles','Vehicle code']
    df = pd.read_csv('vehicle-data.csv', names= column_names)
    df.drop(['Number of axles','Vehicle code'], axis=1, inplace=True)
    df.to_csv("csv_data.csv", index=False)

def extract_from_tsv():
    column_names = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles','Tollplaza id', 'Tollplaza code']
    df = pd.read_table('tollplaza-data.tsv', names = column_names)
    df.drop(['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type'], axis=1, inplace=True)
    df.to_csv("tsv_data.csv", index=False)
    
def extract_from_fixed_width_file():
    df = pd.read_fwf('payment-data.txt', delimiter=' ', header=None)
    df = df[[9,10]]
    df.rename(columns = {9:'Type of Payment code', 10:'Vehicle Code'}, inplace = True)
    df.to_csv("fixed_width_data.csv", index=False)

def consolidate_csvs():
    df1 = pd.read_csv('csv_data.csv')
    df2 = pd.read_csv('tsv_data.csv')
    df3 = pd.read_csv('fixed_width_data.csv')
    combined_df = pd.concat([df1, df2, df3], axis=1)
    combined_df.to_csv("extracted_data.csv", index=False)

def transform_data():
    df = pd.read_csv("extracted_data.csv")
    
    # change vehicle type to uppercase 
    df["Vehicle type"] = df["Vehicle type"].apply(lambda x: x.upper())
    
    # rename the columns
    df.rename(columns={'Rowid':'row_id','Timestamp':'timestamp','Anonymized Vehicle number':'anonymized_vehicle_number','Vehicle type':'vehicle_type',
                        'Number of axles':'number_of_axles','Tollplaza id':'tollplaza_id','Tollplaza code':'tollplaza_code',
                        'Type of Payment code':'type_of_payment_code','Vehicle Code':'vehicle_code'}, inplace=True)
    
    # convert to the correct datatype
    df[['anonymized_vehicle_number','tollplaza_id']] = df[['anonymized_vehicle_number','tollplaza_id']].astype(str)
    df['timestamp'] = df.timestamp.astype('datetime64[ns]')
    
    df.to_csv("transformed_data.csv", index=False)

def load_transformed_data():
    try:
        # create MySQL connection
        conn = msql.connect(host="localhost",user ="root",password="<password>")

        # create database
        cursor = conn.cursor()
        cursor.execute("CREATE DATABASE IF NOT EXISTS traffic_data")
        print("Database was created")

        # connect to database
        try:
            db_config = {
            "host": "localhost",
            "user": "root",
            "password": "<password>",
            "database": "traffic_data"
            }
            # Create a MySQL connection
            conn = msql.connect(**db_config)
            cursor = conn.cursor()
            print("Connect to DB successfully")

            # create table
            try:
                cursor.execute("CREATE TABLE IF NOT EXISTS toll_data \
                               (row_id int(10) PRIMARY KEY, timestamp datetime, anonymized_vehicle_number varchar(25),\
                                vehicle_type varchar(10), number_of_axles int(10), tollplaza_id varchar(10), \
                                tollplaza_code varchar(10), type_of_payment_code varchar(10), vehicle_code varchar(10))")
                print("Table was created....")
                
                # read data to dataframe
                df = pd.read_csv("transformed_data.csv")
                
                # loop through the dataframe, insert rows into table
                print("Inserting data into table...")
                for i,row in df.iterrows(): 
                    sql = "INSERT INTO traffic_data.toll_data VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                    
                    # insert rows into table
                    cursor.execute(sql, tuple(row))
                    conn.commit() # commit to save changes
                print(f"{i+1} rows were inserted")
                              
            except msql.Error as err:
                print(f"Error: {err}")

        except msql.Error as err:
            print(f"Error: {err}")

    except msql.Error as err:
        print(f"Error: {err}")
    
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")


# DAG arguments
default_args = {
    'owner': 'Henry Fash',
    'start_date': days_ago(0),
    'email': ['hfash@mymail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow ETL for Traffic data',
    schedule_interval=timedelta(days=1),
)

#*** define the tasks ***#

# task to download data
download_data =  PythonOperator(
    task_id='download_data',
    python_callable=download_tolldata,
    provide_context=True, 
    dag=dag,
)

# task to unzip data
unzip_data =  PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_tolldata,
    provide_context=True, 
    dag=dag,
)

# task to extract data from csv file
extract_data_from_csv = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_from_csv,
    provide_context=True, 
    dag=dag,
)

# task to extract data from tsv file
extract_data_from_tsv =  PythonOperator(
    task_id='extract_tsv',
    python_callable=extract_from_tsv,
    provide_context=True, 
    dag=dag,
)

# task to extract data from fixed width file
extract_data_from_fixed_width =  PythonOperator(
    task_id='extract_fixed_width',
    python_callable=extract_from_fixed_width_file,
    provide_context=True, 
    dag=dag,
)

# task to consolidate data extracted
consolidate_data =  PythonOperator(
    task_id='merge_csvs',
    python_callable=consolidate_csvs,
    provide_context=True, 
    dag=dag,
)

# task to transform data

transform_data =  PythonOperator(
    task_id='transform_merge',
    python_callable=transform_data,
    provide_context=True, 
    dag=dag,
)

# task to load data
load_data =  PythonOperator(
    task_id='load_transform_merge',
    python_callable=load_transformed_data,
    provide_context=True, 
    dag=dag,
)

# task pipeline
download_data >> unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data >> load_data