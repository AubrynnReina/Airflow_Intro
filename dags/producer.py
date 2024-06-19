from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset('./tmp/test.txt')
my_file_2 = Dataset('./tmp/test_2.txt')

with DAG(
    dag_id='producer',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    

    @task(outlets=[my_file])
    def update_file():
        with open(my_file.uri, '+a') as f:
            f.write('Producer update.\n')

    
    @task(outlets=[my_file_2])
    def update_file_2():
        with open(my_file_2.uri, '+a') as f:
            f.write('Producer update.\n')

    update_file() >> update_file_2()