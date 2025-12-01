
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import pandas as pd

from airflow.models import Param
import pendulum
import uuid
from airflow.exceptions import AirflowSkipException
import os
import sys 

script_path = os.path.abspath(__file__)
project_path = os.path.dirname(script_path)+'/libs'
sys.path.append(project_path)
from functions_dwh.functions_dsm import get_oracle_connection, get_clickhouse_client, compute_row_hash

# Настройка логирования
logger = LoggingMixin().log


@dag(
    dag_id='wf_dsm_mart_dsm_sale_data',
    start_date=days_ago(1),
    catchup=False,
    params={
        'loading_month': Param(
            '1900-01-01', 
            type='string'
        )
    },
    tags=['oracle', 'clickhouse', 'data_migration']
)
def wf_dsm_mart_dsm_sale_data():
        
    src_table_name = 'DATA_MART."V$ALTAY_DATA"' #название таблицы-источника
    tgt_table_name = 'stg.mart_dsm_sale' # название целевой таблицы
    pk_list = ['cd_reg', 'cd_u', 'stat_year', 'stat_month', 'sales_type_id'] # список полей PK источника
    
    message = f'Нет новых данных для загрузки в {tgt_table_name}'

    @task
    def get_loading_period(*args, **kwargs):
        """Определение периодов для загрузки на основе параметров"""
        
        # Получаем параметр месяца прогрузки из контекста
        dag_run = kwargs.get('dag_run')
        if dag_run and dag_run.conf:
            loading_month = dag_run.conf.get('loading_month')
            if loading_month:
                logger.info(f"Введена дата прогрузки данных, где loading_month = {loading_month}")              
                return loading_month
        # Если параметр не передан, используем значение по умолчанию
        params = kwargs.get('params', {})
        loading_month = params.get('loading_month', '1900-01-01')
        logger.info(f"Используется дата прогрузки по умолчанию: {loading_month}")
        return loading_month
        
    @task.short_circuit # (pool='sequential_processing', pool_slots=1) # декоратор для условного последовательного выполнения
    def check_new_data_altay_data(loading_month, **kwargs):
        """Проверка наличия новых данных из V$ALTAY_DATA """
        if not loading_month:
            logger.error("Параметр loading_month не передан")
            raise ValueError("Параметр loading_month обязателен")
        
        logger.info(f"___________Дата прогрузки: {loading_month}__________")
        
        ch_client = get_clickhouse_client()        
        
        # получение cd_reg
        with get_oracle_connection() as oracle_conn:
            get_cd_reg = f"""SELECT distinct cd_reg from {src_table_name} where stat_date = to_date('{loading_month}', 'YYYY-mm-dd')""" 
            
            all_cd_regs_for_month = pd.read_sql(get_cd_reg, oracle_conn)
            all_cd_regs_for_month = all_cd_regs_for_month['CD_REG'].tolist()
            logger.info(f"Получен перечень {len(all_cd_regs_for_month)} cd_reg: {all_cd_regs_for_month}")

        try:
            for cd_reg in all_cd_regs_for_month:
                oracle_query = f"""SELECT * from {src_table_name} where stat_date = to_date('{loading_month}', 'YYYY-mm-dd') and cd_reg = {cd_reg}"""

                with get_oracle_connection() as oracle_conn:
                    df_oracle = pd.read_sql(oracle_query, oracle_conn)
                    logger.info(f"Выполнен запрос: {oracle_query}")
                    logger.info(f"Запрос к источнику выполнен, получено данных: {df_oracle.size}, получено строк: {df_oracle.shape[0]}")
                    # 1. Проверка наличия данных в БД Oracle
                    if df_oracle.empty:
                        raise AirflowSkipException(message)
                    else:
                        # 2. Дельта новых данных в БД Oracle
                        # 2.1. Преобразование наименований столбцов, т.к. в Oracle указаны в верхнем регистре, в ClickHouse - в нижнем
                        df_oracle.columns = [col.lower() for col in df_oracle.columns]

                        # # 4. Данных нет
                        if df_oracle.empty:
                            raise AirflowSkipException(message)

                        else:
                            # 5.1. Создание хешей для всех столбцов таблицы-источника, кроме ключей
                            hash_cols = [row for row in df_oracle.columns.tolist() if row not in pk_list]

                            df_for_insert = df_oracle.assign(
                                                            processed_dttm=pd.Timestamp.now().normalize(), 
                                                            deleted_flag=0,
                                                            hash_diff=lambda df: df.apply(compute_row_hash, columns=hash_cols, axis=1)  # Хеш для выбранных столбцов
                                                        ).rename(columns={'stat_date': 'effective_dttm'}) # переименовали колонку из целевой таблицы stat_date -> effective_dttm  
                            logger.info(f"Запрос подготовки технических полей выполнен, получено данных: {df_for_insert.size}, строк: {df_for_insert.shape[0]}")
                            
                            # 5.2. Вставка дельты
                            ch_client.insert_dataframe(f"INSERT INTO {tgt_table_name} VALUES", df_for_insert, settings=dict(use_numpy=True))
                            logger.info(f"Прогружены данные в таблицу {tgt_table_name} за период {loading_month} по региону {cd_reg}")
            return True
        except AirflowSkipException:
        # Пробрасываем исключение пропуска дальше
            raise
        except Exception as e:
            logger.error(f"Ошибка при проверке новых данных в {src_table_name}: {str(e)}")
            raise
        finally:
            if ch_client:
                ch_client.disconnect()
                
    get_period = get_loading_period()
    check_result = check_new_data_altay_data(get_period)
    #check_result = check_new_data_altay_data.expand(period=get_period)
    #load_task = load_altay_data.expand(datas=check_result)
    #clean_task = clean_up_temp_altay_data.expand(tmp_table_name=load_task)

    get_period >> check_result

# Инициализация DAG
wf_dsm_mart_dsm_sale_data()