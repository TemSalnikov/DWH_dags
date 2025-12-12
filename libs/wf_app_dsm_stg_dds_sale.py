from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.decorators import dag, task
from airflow.models.param import Param

from datetime import datetime, timedelta
import os
import sys
script_path = os.path.abspath(__file__)
project_path = os.path.dirname(script_path)+'/libs'
sys.path.append(project_path)
import dds_group_tasks.task_group_creation_surogate as tg_sur
# import functions_dds as fn_dds
from functions_dwh.functions_dds import load_delta, save_meta
# from task_group_creation_surogate import hub_load_processing_tasks, get_clickhouse_client

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
    dag_id='wf_app_dsm_stg_dds_sale',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['advanced'],
    params = {
        "p_version_prev": Param('2025-01-01 00:01:01', type = "string", title = "Processed_dttm прудыдущей выгрузки"),
        "p_version_new": Param('2025-01-02 00:01:01', type = "string", title = "Processed_dttm новой выгрузки")
    }
)
def wf_app_dsm_stg_dds_sale():
    pass

    # Создание таблицы с ключами
    # hub
    # sql_query: ddl_dds_sale.sql
    
    # CREATE table if not exists dds.hub_sale
    #     (sale_uuid String,
    #      sale_id String,
    #     src String,
    #     effective_from_dttm DateTime,
    #     effective_to_dttm DateTime,
    #     processed_dttm DateTime,
    #     deleted_flg bool
    #     )
    #     ENGINE = MergeTree
    #     order by (sale_uuid);
    # + после досоздать аксессоры через дипсик

    src_table_name = 'mart_dsm_stat_product'    #название таблицы источника
    tgt_table_name = 'dds.sale'              #название целевой таблицы НАДО СОЗДАТЬ ТАБЛИЦУ
    hub_table_name = 'dds.hub_sale'          #название таблицы Хаба
    pk_list = ['mnn', 'address', 'date_of_disposal']                      #список полей PK источника
    pk_list_dds = ['sale_uuid']              #список полей PK таргета
    bk_list = [] # можно вставить через запрос? путем вывода всех столбцов? #Список полей бизнесс данных источника
    bk_list_dds = [] #Список полей бизнесс данных таргета - КАКИЕ требуются?
    
    
