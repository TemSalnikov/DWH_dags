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
from extract_from_maksavit import extract_xls


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
    dag_id = 'wf_xls_kafka_mart_fpc_maksavit_remain',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['advanced', 'maksavit']
)

def wf_xls_kafka_mart_fpc_maksavit_remain():

    @task
    def check_data_availability() -> bool:
        return True
    @task
    def prepare_parameters(data_ready: bool, **context) -> dict:
        if data_ready:
            dag_run_conf = context["dag_run"].conf if "dag_run" in context else {}
            parametrs = {**context["params"], **dag_run_conf}
            return parametrs
        else: raise
    @task
    def extract_from_xls(configs:dict, **context):
        loger = LoggingMixin().log
        _dag_id = context["dag"].dag_id
        algo_id = _dag_id[3:]
        loger.info(f'Успешно получено algo_id {algo_id}!')
        for folder, files in configs[algo_id]['files'].items():
            for file in files:
                loger.info(f"Параметры записи в Кафка: {configs[algo_id]['directory']+'/'+folder+'/'+file},{configs[algo_id]['name_report']},{configs[algo_id]['name_pharm_chain']},{configs[algo_id]['prefix_topic']}")
                if call_producer(extract_xls,
                                        configs[algo_id]['directory']+'/'+folder+'/'+file,
                                        configs[algo_id]['name_report'],
                                        configs[algo_id]['name_pharm_chain'],
                                        configs[algo_id]['prefix_topic']):
                    write_meta_file(configs[algo_id]['directory'], folder, file)

    start_flow = check_data_availability()
    parametrs = prepare_parameters(start_flow)
    extract_from_xls(parametrs)


wf_xls_kafka_mart_fpc_maksavit_remain()