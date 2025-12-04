from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from file_processing import write_meta_file
import os
import sys

# Добавляем путь к библиотекам в sys.path
script_path = os.path.abspath(__file__)
project_path = os.path.dirname(script_path) + '/libs'
sys.path.append(project_path)

from kafka_producer_common_for_xls import call_producer
from extract_from_vita_tomsk import extract_sale


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
    dag_id='wf_xls_kafka_mart_fpc_vita_tomsk_sale',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['advanced', 'vita_tomsk']
)
def wf_xls_kafka_mart_fpc_vita_tomsk_sale():

    @task
    def prepare_parameters(**context) -> dict:
        """Извлекает параметры, переданные из cf-дага."""
        dag_run_conf = context["dag_run"].conf if "dag_run" in context else {}
        return {**context["params"], **dag_run_conf}

    @task
    def extract_from_xls(configs: dict, **context):
        """Извлекает данные из файлов и отправляет в Kafka."""
        loger = LoggingMixin().log
        for folder, files in configs['files'].items():
            for file in files:
                full_path = os.path.join(configs['directory'], folder, file)
                loger.info(f"Начало обработки файла: {full_path}")
                
                # Вызываем парсер и отправляем данные в Kafka
                if call_producer(extract_sale,
                                 full_path,
                                 configs['name_report'],
                                 configs['name_pharm_chain'],
                                 configs['prefix_topic']):
                    # Если отправка успешна, записываем метаданные
                    write_meta_file(configs['directory'], folder, file)

    parametrs = prepare_parameters()
    extract_from_xls(parametrs)

wf_xls_kafka_mart_fpc_vita_tomsk_sale()