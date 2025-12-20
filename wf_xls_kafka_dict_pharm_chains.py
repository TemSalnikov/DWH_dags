from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from file_processing import write_meta_file
import os
import sys
script_path = os.path.abspath(__file__)
project_path = os.path.dirname(script_path)+'/libs'
sys.path.append(project_path)

from kafka_producer_common_for_xls import call_producer
from extract_from_parm_chains import extract_xls


default_args = {
    'owner': 'artem_s',
    'depends_on_past': False,  # Задачи не зависят от прошлых запусков
    'start_date': datetime(2025, 1, 1),
    'email': ['twindt@mail.ru'],
    'email_on_failure': False,  # Не Отправлять email при ошибке
    'email_on_retry': False,   # Не отправлять при ретрае
    'retries': 0,             # 0 попытки при ошибке
    'retry_delay': timedelta(minutes=5),  # Ждать 5 минут перед ретраем
    'execution_timeout': timedelta(minutes=30),  # Макс. время выполнения задачи
}



@dag(
    dag_id = 'wf_xls_kafka_dict_pharm_chains',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params = {'directory': '/opt/airflow/data/reports/',
            'name_report': 'Прямые сети ИНН 4 кв 2025_СВОД_ФИНАЛ_с ассоциациями_.xlsx',
            'name_pharm_chain': '',
            'prefix_topic': 'dict_ptharm_chain'
        #   'db_config': {'host': 'postgres',
        #                 'database': 'meta',
        #                 'user': 'meta',
        #                 'password': 'meta',
        #                 'port': '5432'
        #                 }
        },
    tags=['advanced']
)

def wf_xls_kafka_dict_pharm_chains():

    @task
    def check_data_availability() -> bool:
        # Проверяет готовность данных (пример реализации).
        # Возвращает True если данные готовы.

        # Здесь может быть проверка файлов, запрос к API или БД
        # Для примера просто возвращаем True
        return True
    @task
    def prepare_parameters(data_ready: bool, **context) -> dict:

        # Получаем параметры DAG
        if data_ready:
            # Получаем параметры из контекста выполнения
            dag_run_conf = context["dag_run"].conf if "dag_run" in context else {}

            # Объединяем с параметрами по умолчанию из DAG
            parametrs = {**context["params"], **dag_run_conf}
            return parametrs
        else: raise
    @task
    def extract_from_xls(configs:dict, **context):
        # pass
        loger = LoggingMixin().log
        _dag_id = context["dag"] if "dag" in context else ''
        algo_id = str(_dag_id).split(':')[1].strip().strip('>')[3:]
        loger.info(f'Успешно получено algo_id {algo_id}!')
        if call_producer(extract_xls,
                                configs[algo_id]['directory']+'/'+configs[algo_id]['name_report'],
                                configs[algo_id]['name_report'],
                                configs[algo_id]['name_pharm_chain'],
                                configs[algo_id]['prefix_topic']):
            # write_meta_file(
            #     # configs[algo_id]['db_config'], 
            #     configs[algo_id]['directory'],folder,file)

    start_flow = check_data_availability()
    parametrs = prepare_parameters(start_flow)
    extract_from_xls(parametrs)


wf_xls_kafka_dict_pharm_chains()
