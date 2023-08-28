# import the libraries

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import tarfile
import pandas as pd

# define functions
def download_tolldata():
    response = requests.get("https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz")
    with open("tolldata.tgz", "wb") as file:
        file.write(response.content)

def unzip_tolldata():
    with tarfile.open("tolldata.tgz", "r:gz") as tar:
        tar.extractall()

def extract_csv():
    df = pd.read_csv('vehicle-data.csv', header = None)
    df.drop([4, 5], axis=1, inplace=True)
    df.to_csv("csv_data.csv", index=False, header=False)

def extract_tsv():
    df = pd.read_table('tollplaza-data.tsv', header = None)
    df.drop([0,1,2,3], axis=1, inplace=True)
    df.to_csv("tsv_data.csv", index=False, header=False)

def extract_fixedwidth():
    df = pd.read_fwf('payment-data.txt', delimiter=' ', header=None)
    df = df[[9,10]]
    df.to_csv("fixed_width_data.csv", index=False, header=False)

def consolidate_csvs():
    df1 = pd.read_csv('csv_data.csv', header = None)
    df2 = pd.read_csv('tsv_data.csv', header = None)
    df3 = pd.read_csv('fixed_width_data.csv', header = None)
    combined_df = pd.concat([df1, df2, df3], axis=1)
    combined_df.to_csv("extracted_data.csv", index=False, header=False)

def transform_data():
    pass

def load_transformed_data():
    pass


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
    description='Apache Airflow Final Assignment',
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
    python_callable=extract_csv,
    provide_context=True, 
    dag=dag,
)

# task to extract data from tsv file
extract_data_from_tsv =  PythonOperator(
    task_id='extract_tsv',
    python_callable=extract_tsv,
    provide_context=True, 
    dag=dag,
)

# task to extract data from fixed width file
extract_data_from_fixed_width =  PythonOperator(
    task_id='extract_fixedwidth',
    python_callable=extract_fixedwidth,
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