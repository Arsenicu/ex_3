from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonVirtualenvOperator

from datetime import datetime


import os

DAG_ID = "ex_3_etl"

default_args = {
    "owner": "airflow"}

def run_filter_okved():
    from ex3.ex_1_2_egrul import main
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    postgres_hook = PostgresHook()
    #engine = postgres_hook.get_sqlalchemy_engine() 
    return main('/opt/airflow/data/egrul.json.zip', postgres_hook)
    
def run_get_vacancies():
    import asyncio
    from ex3.ex_2_1 import main
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    
    postgres_hook = PostgresHook()
    params={'text': 'python middle developer', 'per_page': 100}


    return asyncio.run(main('api', params, postgres_hook))
    
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2023, 7, 1),
    schedule="@once",
    catchup=False,
) as dag:
    download=BashOperator(
        task_id='download_egrul',
        bash_command="""curl 'https://ofdata.ru/open-data/download/egrul.json.zip' -o '/opt/airflow/data/egrul.json.zip'""",
        )
    
    filter_okved = PythonVirtualenvOperator(
        task_id="filter_okved",
        requirements=["orjson==3.9.1","tqdm"],
        python_callable=run_filter_okved,
    )
    get_vacancies = PythonVirtualenvOperator(
        task_id="get_vacancies",
        requirements=["aiohttp==3.8.1","beautifulsoup4==4.12.2","tqdm"],
        python_callable=run_get_vacancies,
    )
    compare_vacancies = PostgresOperator(
        task_id="compare_vacancies",
        sql="""
            DROP TABLE if EXISTS ex3_result;
            SELECT * INTO ex3_result FROM (
                SELECT t.name, t.okved_code, v.position, v.job_description, v.key_skills  
                FROM  public.vacancies v
                JOIN  public.telecom_companies t ON t.name ILIKE ('% ' || v.company_name || '%')
            ) r;
          """,
    )
    download >> filter_okved >> get_vacancies >> compare_vacancies