from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.models import Param
#import cx_Oracle
from clickhouse_driver import Client
import pandas as pd
from typing import Dict, List, Any
import oracledb
import hashlib
from airflow.utils.log.logging_mixin import LoggingMixin

loger = LoggingMixin().log

def compute_row_hash(row, columns=None):
    if columns:
        row = row[columns]
    # Преобразуем все значения строки в строки и объединяем их
    row_string = ''.join(str(value) for value in row)
    # Создаем хеш используя SHA-256
    return hashlib.sha256(row_string.encode()).hexdigest()

# Конфигурация подключений
ORACLE_CONN = {
    'user': 'ALTAYV',
    'password': 'sSwM913_xoAY', 
    'host': 'dsmviewer.ru',
    'port': 27091,
    'sid': 'webiasdb2'
}

CLICKHOUSE_CONN: dict[str, str | int] = {
    'host': '192.168.14.235',
    'port': 9001,
    'user': 'admin',
    'password': 'admin',
    'database': 'stg'
}

# Функции подключения к БД
def get_oracle_connection():
    dsn = f"{ORACLE_CONN['host']}:{ORACLE_CONN['port']}/{ORACLE_CONN['sid']}"
    return oracledb.connect(
        user=ORACLE_CONN['user'],
        password=ORACLE_CONN['password'],
        dsn=dsn
    )

def get_clickhouse_client() -> Client:
    """Создает и возвращает клиент ClickHouse"""
    return Client(
        host=CLICKHOUSE_CONN['host'],
        port=CLICKHOUSE_CONN['port'],
        user=CLICKHOUSE_CONN['user'],
        password=CLICKHOUSE_CONN['password'],
        database=CLICKHOUSE_CONN['database']
    )

# Декоратор DAG

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

logger = LoggingMixin().log

@dag(
    dag_id='wf_dsm_mart_dsm_region',
    # schedule_interval='0 9 6 * *', # в 9 утра каждого месяца 6 числа
    start_date=days_ago(1),
    default_args=default_args,
    catchup=False,
    # params={
        # 'dates_from': Param(
        #     (datetime.now() - timedelta(days=1)).replace(day=1).strftime("%Y-%m-%d"),
        #     type='string',
        #     description='Дата начала отчетного периода в формате YYYY-MM-01. Указывать тот месяц, за который требуется отчет. Если требуется прогрузка за 1 месяц, поле "dates_to" оставить пустым.'
        # ),
        # 'dates_to': Param(
        #     '-',
        #     type='string',
        #     description='Дата окончания отчетного периода в формате YYYY-MM-01.'
        # )
    # },
    tags=['oracle', 'clickhouse', 'data_migration']
)
def wf_dsm_mart_dsm_region():
    tgt_table_name = 'mart_dsm_region'
    @task
    def load_altay_reg():
        """Загрузка новых данных из V$ALTAY_REG """

        message = f'Нет новых данных для загрузки в {tgt_table_name}'

        oracle_query = f"""SELECT distinct * from DATA_MART."V$ALTAY_REG" """

        try:
            with get_oracle_connection() as oracle_conn:
                df_oracle = pd.read_sql(oracle_query, oracle_conn)
                logger.info(f"Выполнен запрос: {oracle_query}")
                logger.info(f"Запрос к источнику выполнен, получено данных: {df_oracle.size}, получено строк: {df_oracle.shape[0]}")

                # 1. Проверка наличия данных в БД Oracle
                if df_oracle.empty:
                    logger.info(message)

                else:
                    # 2. Проверка наличия новых данных в БД Oracle
                    # преобразования наименований столбцов, т.к. в Oracle указаны в верхнем регистре, в ClickHouse - в нижнем
                    df_oracle.columns = [col.lower() for col in df_oracle.columns]
                    hash_cols = ['cd_reg', 'sales_type_id']

                    df_for_insert = df_oracle.assign(
                                                            processed_dttm=pd.Timestamp.now().normalize(), 
                                                            deleted_flag=0,
                                                            hash_diff=lambda df: df.apply(compute_row_hash, columns=hash_cols, axis=1)  # Хеш для выбранных столбцов
                                                        )
                    logger.info(f"Запрос подготовки технических полей выполнен, получено данных: {df_for_insert.size}, строк: {df_for_insert.shape[0]}")
                    
                    # 5.2. Вставка дельты
                    ch_client = get_clickhouse_client()
                    ch_client.insert_dataframe(f"INSERT INTO {tgt_table_name} VALUES", df_for_insert, settings=dict(use_numpy=True))
                    logger.info(f"Прогружены данные в таблицу {tgt_table_name}")
    
                    ch_client.disconnect()
                    logger.info(f"Успешно загружено {len(df_for_insert)} записей в {tgt_table_name}")
        except Exception as e:
            logger.info(f"Ошибка при загрузке mart_dsm_region: {str(e)}")
            raise

    load_altay_reg()

wf_dsm_mart_dsm_region()
