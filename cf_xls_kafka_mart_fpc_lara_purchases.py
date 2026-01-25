from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param
from airflow.exceptions import AirflowSkipException
from airflow.utils.log.logging_mixin import LoggingMixin
from typing import Optional, Dict
import os
import sys
script_path = os.path.abspath(__file__)
project_path = os.path.dirname(os.path.dirname(script_path))
sys.path.append(os.path.join(project_path, 'libs'))
import file_processing

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
    'provide_context': True
}

@dag(
    dag_id='cf_xls_kafka_mart_fpc_lara_purchases',
    default_args=default_args,
    schedule_interval="@monthly",
    catchup=False,
    params = {'directory': '/opt/airflow/data/reports/ЛАРА/Закуп/',
              'name_report': 'Закупки',
              'name_pharm_chain': 'Лара',
              'prefix_topic': 'fpc_lara'
            },
    tags=['advanced', 'lara']
)
def cf_xls_kafka_mart_fpc_lara_purchases():
    @task
    def check_data_availability() -> bool:
        return True
    @task
    def prepare_parameters(data_ready: bool, **context) -> Optional[Dict]:
        if data_ready:
            dag_run_conf = context["dag_run"].conf if "dag_run" in context else {}
            parametrs = {**context["params"], **dag_run_conf}
            return parametrs
        else: raise


    @task
    def get_folders(parametrs:Dict)-> Optional[list]:
        return file_processing.get_list_folders(parametrs['directory'])

    @task
    def get_files(parametrs:Dict, folders:list)-> Optional[dict]:
        loger = file_processing.LoggingMixin().log
        files = {}
        for folder in folders:
            loger.info(f'Получения списка файлов из папки {folder}')
            files[folder] = file_processing.get_list_files(parametrs['directory'], folder)
        loger.info(f'Получен перечень файлов: {files}')
        return files

    @task
    def get_meta_folders(parametrs:Dict)-> Optional[list]:
        loger = file_processing.LoggingMixin().log
        query = f"""select name_folder from files.folders c
                join files.directories d on c.id_dir = d.id_dir and d.name_dir = '{parametrs['directory']}'"""
        loger.info(f'Сформирован запрос: {query}')
        folders = file_processing.get_meta_data(query)
        loger.info(f'Получен перечень папок: {folders}')
        return folders

    @task
    def get_meta_files(parametrs:Dict, folders:list)-> Optional[dict]:
        loger = file_processing.LoggingMixin().log
        files = {}
        for folder in folders:
            loger.info(f'Получения списка файлов из папки {folder}')
            query = f"""select name_file  from files.files f
                        join files.folders c on f.id_folder = c.id_folder and c.name_folder = '{folder}'
                        join files.directories d on c.id_dir = d.id_dir and d.name_dir = '{parametrs['directory']}' """
            loger.info(f'Сформирован запрос: {query}')
            files[folder] = file_processing.get_meta_data(query)
            loger.info(f'Получен перечень файлов: {files}')
        return files

    @task
    def get_folders_for_processing(meta_folder_list:list, folders_list:list)-> Optional[list]:
        return file_processing.check_new_folders(meta_folder_list, folders_list)

    @task
    def get_files_for_processing(processinf_folders: list, meta_files_dict:dict, files_dict:dict)-> Optional[dict]:
        files = {}
        for folder in processinf_folders:
            files_list= file_processing.check_new_files(files_dict[folder], meta_files_dict.get(folder, []))
            if files_list:
                files[folder] = files_list

        return files

    @task
    def trigger_or_skip(parametrs: Optional[Dict], processing_files: Optional[Dict], **context):
        loger = LoggingMixin().log
        if processing_files:
            parametrs['files'] = processing_files
            current_dag_id = context['dag'].dag_id
            target_dag_id = 'wf' + current_dag_id[2:]
            loger.info(f"Запуск дочернего DAG: {target_dag_id} с параметрами.")
            TriggerDagRunOperator(
                task_id=f"trigger_{target_dag_id}",
                trigger_dag_id=target_dag_id,
                conf={current_dag_id[3:]: parametrs}
            ).execute(context)
        else:
            raise AirflowSkipException("Новые файлы для обработки отсутствуют. Пропускаем запуск.")

    start_flow = check_data_availability()
    parametrs = prepare_parameters(start_flow)
    folders = get_folders(parametrs)
    meta_folders = get_meta_folders(parametrs)
    files = get_files(parametrs, folders)
    meta_files = get_meta_files(parametrs, folders)
    processinf_folders = get_folders_for_processing(meta_folders, folders)
    processing_files = get_files_for_processing(processinf_folders, meta_files, files)
    trigger_or_skip(parametrs, processing_files)

cf_xls_kafka_mart_fpc_lara_purchases()