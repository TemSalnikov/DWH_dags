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
    name_sur_key = 'sale_uuid'       #название сурогатного ключа

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
        else: 
            raise # МБ break?
    
    @task
    def get_inc_load_data(source_table: str, pk_list: list, bk_list:list, p_version_prev: str, p_version_new: str) -> str:
        """Получение последних данных из источника"""
        client = None
        tmp_table_name = f"tmp.tmp_v_sv_all_{source_table}_{tg_sur.uuid.uuid4().hex}" # почему такое название таблицы?
        
        try:
            logger = LoggingMixin().log
            client = tg_sur.get_clickhouse_client()
            logger.info(f"Подклчение к clickhouse успешно выполнено")
            ### надо подумать над запросом
            # ПОЧЕМУ effective_dttm ПРИНУДИТЕЛЬНО ПРИСВАИВАЕТСЯ toDateTime('1990-01-01 00:01:01') ? это же полная дата в отчета в истонике
            query = f"""
            CREATE TABLE {tmp_table_name} 
            ENGINE = MergeTree()
            PRIMARY KEY ({', '.join(pk_list)})
            ORDER BY ({', '.join(pk_list)})
            AS 
            SELECT 
                {', '.join([f'{item1} as {item2}' for item1, item2 in zip(bk_list, bk_list_dds)])},
                'DSM' as src,
                toDateTime('1990-01-01 00:01:01') as effective_dttm                
            FROM stg.v_iv_{source_table}(p_from_dttm = \'{p_version_prev}\', p_to_dttm = \'{p_version_new}\')
            """
            logger.info(f"Создан запрос: {query}")

            client.execute(query)
            logger.info(f"Создана временная таблица {tmp_table_name} с данными из {source_table}")
            
            return tmp_table_name
            
        except tg_sur.ClickhouseError as e:
            logger.error(f"Ошибка при получении данных из {source_table}: {e}")
            raise
        finally:
            if client:
                client.disconnect()
                logger.debug("Подключение к ClickHouse закрыто")
    

    
    
    
