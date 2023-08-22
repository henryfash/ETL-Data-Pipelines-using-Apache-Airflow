# import the libraries
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago



# DAG arguments
default_args = {
    'owner': 'Henry Fas',
    'start_date': days_ago(0),
    'email': ['henryfas@somemail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow ETL',
    schedule_interval=timedelta(days=1),
)

#*** define the tasks ***#

# task to download
download_data = BashOperator(
    task_id='download',
    bash_command='wget -P /home/project/airflow/dags https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz',
    dag=dag,
)

# task to unzip data
unzip_data = BashOperator(
    task_id='unzip',
    bash_command='tar -xzf /home/project/airflow/dags/tolldata.tgz -C /home/project/airflow/dags',
    dag=dag,
)

# task to extract data from csv file
extract_data_from_csv = BashOperator(
    task_id='extract_csv',
    bash_command='cut -d"," -f1-4 vehicle-data.csv > /home/project/airflow/dags/csv_data.csv,
    dag=dag,
)

# task to extract data from tsv file
extract_data_from_tsv = BashOperator(
    task_id='extract_tsv',
    bash_command='cut -d $"\t" -f5-7 tollplaza-data.tsv > /home/project/airflow/dags/tsv_data.csv,
    dag=dag,
)

# task to extract data from fixed width file
extract_data_from_fixed_width = BashOperator(
    task_id='extract_txt',
    bash_command='cut -d " " -f6,7 payment-data.txt > /home/project/airflow/dags/fixed_width_data.csv,
    dag=dag,
)

# task to consolidate data extracted
consolidate_data = BashOperator(
    task_id='consolidate_files',
    bash_command='paste -d "," csv_data.csv tsv_data.csv fixed_width_data.csv > /home/project/airflow/dags/extracted_data.csv,
    dag=dag,
)

# task to transform data
transform_data = BashOperator(
    task_id='transform',
    bash_command='paste -d"," <(cut -d"," -f1-3 extracted_data.csv) <(cut -d"," -f4 extracted_data.csv | tr "[:lower:]" "[:upper:]") <(cut -d"," -f5- extracted_data.csv) > transformed_data.csv',
    dag=dag,
)



#**** task pipeline ****#
download_data >> unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data