from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.api.common.trigger_dag import trigger_dag
from airflow.exceptions import AirflowSkipException
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.providers.postgres.hooks.postgres import PostgresHook
from clickhouse_driver import Client
from typing import Optional
import os
import sys
script_path = os.path.abspath(__file__)
project_path = os.path.dirname(script_path)+'/libs'
sys.path.append(project_path)
from functions_dwh.db_connection import get_clickhouse_client

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
    dag_id='cf_app_mdlp_stg_dds_counterparty',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['advanced']
)
def cf_app_mdlp_stg_dds_counterparty():
    src_tables = [
        'mart_mdlp_general_report_on_disposal',
        'mart_mdlp_general_pricing_report', 
        'mart_mdlp_general_report_on_movement'
    ]

    @task
    def check_data_availability() -> bool:
        # Проверяет готовность данных для всех источников
        return True

    @task
    def get_max_stg_all_tables(table_names: list) -> Optional[str]:
        """Получает минимальную дату из максимальных дат по всем таблицам"""
        logger = LoggingMixin().log
        
        try:
            ch_client = get_clickhouse_client()
            max_dates = []
            
            for table_name in table_names:
                query = f"""
                SELECT max(create_dttm) as max_processed_dttm 
                FROM stg.{table_name}
                """
                logger.info(f'Сформирован запрос для таблицы {table_name}: {query}')
                
                q_res = ch_client.execute(query)
                max_date = q_res[0][0] if q_res and q_res[0][0] else None
                
                if max_date:
                    logger.info(f'Максимальная дата для {table_name}: {max_date}')
                    max_dates.append({table_name:datetime.strftime(max_date, '%Y-%m-%d %H:%M:%S')})
                else:
                    logger.warning(f'Нет данных в таблице {table_name}')
            
            ch_client.disconnect()
            
            if not max_dates:
                logger.error("Нет данных ни в одной из таблиц источников")
                return None
            
            # # Берем минимальную дату из максимальных, чтобы гарантировать наличие данных во всех таблицах
            # min_max_date = min(max_dates)
            # logger.info(f'Минимальная дата из максимальных: {min_max_date}')
            
            return max_dates
            
        except Exception as e:
            logger.error(f"Ошибка при работе с ClickHouse: {e}")
            raise
        finally:
            if 'ch_client' in locals():
                ch_client.disconnect()

    @task
    def get_last_load_dds(**context) -> Optional[str]:
        """Получает дату последней загрузки из таблицы versions"""
        logger = LoggingMixin().log
        _dag_id = context["dag"] if "dag" in context else ''
        dag_id = str(_dag_id).split(':')[1].strip().strip('>')
        logger.info(f'Получен dag_id: {dag_id}')
        
        # Формируем целевой DAG ID (заменяем cf на wf)
        target_dag_id = 'wf' + dag_id[2:]
        logger.info(f'Целевой DAG ID: {target_dag_id}')
        
        query = f"""
        SELECT loaded_stg_processed_dttm 
        FROM versions
        WHERE dag_id = '{target_dag_id}'
        AND dds_processed_dttm = (SELECT max(dds_processed_dttm) FROM versions WHERE dag_id = '{target_dag_id}')
        """
        logger.info(f'Сформирован запрос: {query}')
        
        try:
            hook = PostgresHook(postgres_conn_id="postgres_conn")
            conn = hook.get_conn()
            cursor = conn.cursor()
            
            cursor.execute(query)
            res_list = cursor.fetchall()
            
            if not res_list:
                logger.warning("Не найдено записей в таблице versions, используем дефолтную дату")
                # Если нет предыдущих загрузок, используем старую дату
                default_date = datetime(1990, 1, 1, 0, 1, 1)
                return datetime.strftime(default_date, '%Y-%m-%d')
            
            result = [x[0] for x in res_list]
            last_load_date = result[0]
            logger.info(f'Полученный processed_dttm: {last_load_date}')  
            
            return last_load_date
            
        except Exception as e:
            logger.error(f"Ошибка при работе с PostgreSQL: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()

    @task
    def trigger_or_skip(p_version_prev: str, p_version_new: str, **context):
        """Запускает целевой DAG при наличии новых данных"""
        logger = LoggingMixin().log
        logger.info(f'Полученный контекст: {context}')
        
        _dag_id = context["dag"] if "dag" in context else ''
        _dag_id = str(_dag_id).split(':')[1].strip().strip('>')
        logger.info(f'Успешно получено dag_id {_dag_id}')
        logger.info(f'p_version_prev: {p_version_prev}, p_version_new: {p_version_new}')
        
        if (p_version_prev is not None and 
            p_version_new is not None and 
            datetime.strptime(p_version_prev, '%Y-%m-%d') < 
            datetime.strptime(p_version_new, '%Y-%m-%d')):
            
            parameters = {
                "p_version_prev": p_version_prev, 
                "p_version_new": p_version_new
            }
            
            target_dag_id = 'wf' + _dag_id[2:]
            logger.info(f'Запуск целевого DAG: {target_dag_id}')
            
            result = trigger_dag(
                dag_id=target_dag_id,
                run_id=f"triggered_by_{context['dag_run'].run_id}",
                conf=parameters,
                execution_date=None,
                replace_microseconds=False
            )
            
            if not result:
                raise RuntimeError("Не удалось запустить дочерний DAG")
            else:
                logger.info(f"Успешно запущен DAG {target_dag_id}")
                
        else:
            logger.info("Нет новых данных для обработки, пропускаем запуск")
            raise AirflowSkipException("Условия не выполнены, пропускаем запуск целевого DAG")

    # Определение потока задач
    start_flow = check_data_availability()
    get_max_stg_task = get_max_stg_all_tables(src_tables)
    get_last_load_dds_task = get_last_load_dds()
    trigger_or_skip_task = trigger_or_skip(get_last_load_dds_task, get_max_stg_task)
    
    # Определение зависимостей
    start_flow >> get_max_stg_task >> get_last_load_dds_task >> trigger_or_skip_task

cf_app_mdlp_stg_dds_counterparty()