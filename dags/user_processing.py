import json
import pandas as pd

from airflow import DAG
from datetime import datetime
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = 'postgres'
HTTP_CONN_ID = 'user_api'
HTTP_ENDPOINT = 'api/'
USERS_FILE = './tmp/all_users.csv'
USER_FILE = './tmp/processed_user.csv'


# ti = task instance
def _process_user(ti):
    res_json = ti.xcom_pull(task_ids='extract_user')
    user = res_json['results'][0]
    user_df = pd.json_normalize(
        {
            'first_name': user['name']['first'],
            'last_name': user['name']['last'],
            'country': user['location']['country'],
            'username': user['login']['username'],
            'password': user['login']['password'],
            'email': user['email'],
        }
    )

    # Save for PostgreSQL import
    user_df.to_csv(USER_FILE, index=False, header=False)

    # Save as .csv
    try:
        users_df = pd.read_csv(USERS_FILE)
    except:
        users_df = pd.DataFrame()
    
    users_df = pd.concat([users_df, user_df], axis=0)
    users_df.to_csv(USERS_FILE, index=False, header=True)


def _store_user_to_postgresql():
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.copy_expert(
        sql='COPY users FROM STDIN WITH DELIMITER AS \',\'',
        filename=USER_FILE
    )


with DAG(
    dag_id="user_processing",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql='''
            CREATE TABLE IF NOT EXISTS users (
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            );
        ''')
    

    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id=HTTP_CONN_ID,
        endpoint=HTTP_ENDPOINT
    )


    extract_user = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id=HTTP_CONN_ID,
        endpoint=HTTP_ENDPOINT,
        method='GET',
        response_filter=lambda res: json.loads(res.text),
        log_response=True
    )


    process_user = PythonOperator(
        task_id='process_user',
        python_callable=_process_user, 
    )


    store_user = PythonOperator(
        task_id='store_user',
        python_callable=_store_user_to_postgresql
    )


is_api_available >> extract_user >> process_user >> store_user
create_table >> store_user
