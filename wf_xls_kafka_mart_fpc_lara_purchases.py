from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import os
import sys

script_path = os.path.abspath(__file__)
project_path = os.path.dirname(os.path.dirname(script_path))
sys.path.append(os.path.join(project_path, '\libs'))

from file_processing import write_meta_file
from kafka_producer_common_for_xls import call_producer
from extract_from_lara import extract_xls

default_args = {
    'owner': 'artem_s',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email': ['twindt@mail.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

@dag(
    dag_id = 'wf_xls_kafka_mart_fpc_lara_purchases',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['advanced', 'lara']
)
def wf_xls_kafka_mart_fpc_lara_purchases():

    @task
    def check_data_availability() -> bool:
        return True

    @task
    def prepare_parameters(data_ready: bool, **context) -> dict:
        if data_ready:
            dag_run_conf = context["dag_run"].conf if "dag_run" in context else {}
            return dag_run_conf
        else: raise

    @task
    def extract_from_xls(configs:dict, **context):
        loger = LoggingMixin().log
        # Извлекаем ключ конфигурации из имени DAG-а, который его запустил
        conf_key = list(configs.keys())[0]
        loger.info(f'Успешно получен ключ конфигурации: {conf_key}!')
        params = configs[conf_key]
        for folder, files in params['files'].items():
            for file in files:
                full_path = os.path.join(params['directory'], folder, file)
                loger.info(f"Обработка файла: {full_path}")
                if call_producer(extract_xls, full_path, params['name_report'], params['name_pharm_chain'], params['prefix_topic']):
                    write_meta_file(params['directory'], folder, file)

    start_flow = check_data_availability()
    parametrs = prepare_parameters(start_flow)
    extract_from_xls(parametrs)

wf_xls_kafka_mart_fpc_lara_purchases()