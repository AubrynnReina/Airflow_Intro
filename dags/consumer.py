from airflow import DAG, Dataset
from airflow.decorators import task

from datetime import datetime

my_file = Dataset('./tmp/test.txt')
my_file_2 = Dataset('./tmp/test_2.txt')

with DAG(
    dag_id='consumer',
    schedule=[my_file, my_file_2],
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:
    

    @task()
    def update_file():
        with open(my_file.uri, 'r') as f:
            print(f.read())

    
    @task()
    def update_file_2():
        with open(my_file_2.uri, 'r') as f:
            print(f.read())

    update_file() >> update_file_2()