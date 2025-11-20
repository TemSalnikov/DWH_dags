from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from clickhouse_driver import Client
import pandas as pd
import oracledb
import hashlib
from airflow.models import Param
import pendulum
import uuid
from airflow.exceptions import AirflowSkipException

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

def compute_row_hash(row, columns=None):
    """Создает хеш строки"""
    if columns:
        row = row[columns]
    # Преобразуем все значения строки в строки и объединяем их
    row_string = ''.join(str(value) for value in row)
    # Создаем хеш используя SHA-256
    return hashlib.sha256(row_string.encode()).hexdigest()

# Настройка логирования
logger = LoggingMixin().log

@dag(
    dag_id='wf_dsm_mart_dsm_sale_data',
    start_date=days_ago(1),
    catchup=False,
    params={
        'loading_month': Param( # месяц прогрузки проверить в управляющем потоке типы дат и форматы
            type='string'
        )
    },
    tags=['oracle', 'clickhouse', 'data_migration']
)
def wf_dsm_mart_dsm_sale_data():
        
    src_table_name = 'DATA_MART."V$ALTAY_DATA"' #название таблицы-источника
    tgt_table_name = 'mart_dsm_sale' # название целевой таблицы
    pk_list = ['cd_reg', 'cd_u', 'stat_year', 'stat_month', 'sales_type_id'] # список полей PK источника
    
    message = f'Нет новых данных для загрузки в {tgt_table_name}'

    @task
    def get_loading_periods(*args, **kwargs):
        """Определение периодов для загрузки на основе параметров"""
        
        # Получаем параметр периода дат из контекста
        dag_run = kwargs.get('dag_run')
        if dag_run and dag_run.conf:
            loading_month = dag_run.conf.get('loading_month')
            logger.info(f"Введены даты прогрузки данных, где loading_month = {loading_month}")              

    @task.short_circuit(pool='sequential_processing', pool_slots=1) # декоратор для условного последовательного выполнения
    def check_new_data_altay_data(period, **kwargs):
        """Проверка наличия новых данных из V$ALTAY_DATA """
        tmp_table_name = f"tmp.tmp_{tgt_table_name}_{uuid.uuid4().hex}" # Название для временной таблицы

        loading_month = period['start']
        report_date_to = period['end']
        period_str = f"{loading_month} - {report_date_to}"
        logger.info(f"___________ПЕРИОД: {period_str}__________")

        oracle_query = f"""SELECT distinct * from {src_table_name} where to_char(stat_date, 'yyyy-mm-dd') between :loading_month and :report_date_to""" 
        
        try:
            params = {'loading_month': loading_month, 'report_date_to': report_date_to}
            ch_client = None

            with get_oracle_connection() as oracle_conn:
                df_oracle = pd.read_sql(oracle_query, oracle_conn, params=params)

                # 1. Проверка наличия данных в БД Oracle
                if df_oracle.empty:
                    #logger.info(message)
                    raise AirflowSkipException(message)
                else:
                    # 2. Дельта новых данных в БД Oracle
                    # 2.1. Преобразование наименований столбцов, т.к. в Oracle указаны в верхнем регистре, в ClickHouse - в нижнем
                    df_oracle.columns = [col.lower() for col in df_oracle.columns]

                    ch_client = get_clickhouse_client()
                    df_click = pd.DataFrame(ch_client.execute(f"""select {', '.join(pk_list)} from {tgt_table_name}"""), columns=pk_list) 
                    
                    # 3.Выбор строк в Oracle, кот. отсутствуют в ClickHouse
                    # 3.1. Если первая прогрузка 
                    if df_click.empty:
                        diff_rows = df_oracle.copy()
                    else:
                    # 3.2. Если прогрузка не первая
                        diff_rows = df_oracle.merge(df_click[pk_list], on=pk_list, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

                    # 4. Данных нет
                    if diff_rows.empty:
                        logger.info(message)
                        return False

                    else:
                        # 5. Данные есть, создание временной таблицы -- поправить после результата left join 
                        # 5.1. Создание хешей для всех столбцов таблицы-источника, кроме ключей
                        hash_cols = [row for row in df_oracle.columns.tolist() if row not in pk_list]

                        df_for_insert = diff_rows.assign(
                                                        processed_dttm=pd.Timestamp.now().normalize(), 
                                                        deleted_flag=0,
                                                        hash_diff=lambda df: df.apply(compute_row_hash, columns=hash_cols, axis=1)  # Хеш для выбранных столбцов
                                                    ).rename(columns={'stat_date': 'effective_dttm'}) # переименовали колонку из целевой таблицы stat_date -> effective_dttm  

                        # 5.1. Создание временной таблицы по новым данным из Oracle
                        create_tbl_query = f""" 
                        CREATE TABLE IF NOT EXISTS {tmp_table_name} (
                            cd_reg Int32,
                            cd_u Int64,
                            stat_year Int32,
                            stat_month Int32,
                            sales_type_id Int32,
                            effective_dttm DateTime,
                            volsht_out Float64,
                            volrub_out Float64,
                            volsht_in Float64,
                            volrub_in Float64,
                            prcavg_w_in Float64,
                            prcavg_w_out Float64,
                            pred_pn Float64,
                            pred_pn_firm Float64,
                            pred_tn Float64,
                            pred_tn_firm Float64,
                            pred_br Float64,
                            pred_br_firm Float64,
                            wpred_pn Float64,
                            wpred_pn_firm Float64,
                            wpred_tn Float64,
                            wpred_tn_firm Float64,
                            wpred_br Float64,
                            wpred_br_firm Float64,
                            processed_dttm DateTime,
                            deleted_flag Boolean,
                            hash_diff Text	
                        )
                            ENGINE = MergeTree()
                            order by (cd_u)
                        """
                                    
                        ch_client.execute(create_tbl_query)

                        # 5.2. Вставка дельты
                        ch_client.insert_dataframe(f"INSERT INTO {tmp_table_name} VALUES", df_for_insert, settings=dict(use_numpy=True))
                        logger.info(f"Найдено {diff_rows.shape[0]} новых записей в {tmp_table_name}")
                        return {'tmp_table_name': tmp_table_name, 'loading_month': loading_month, 'report_date_to': report_date_to}
        except AirflowSkipException:
        # Пробрасываем исключение пропуска дальше
            raise
        except Exception as e:
            logger.error(f"Ошибка при проверке новых данных в {src_table_name}: {str(e)}")
            raise
        finally:
            if ch_client:
                ch_client.disconnect()
                

    @task(pool='sequential_processing', pool_slots=1)
    def load_altay_data(datas, **kwargs):
        f"""Загрузка новых данных из временной таблицы {datas['tmp_table_name']}"""

        if not datas:
            raise AirflowSkipException("Пропускаем загрузку - нет данных от проверки")

        loading_month = datas['loading_month']
        report_date_to = datas['report_date_to']
        tmp_table_name = datas['tmp_table_name']

        try:
            ch_client = get_clickhouse_client()
            ch_client.execute(f'INSERT INTO {tgt_table_name} select * from {tmp_table_name}')

            # 6. Проверка прогрузки всех данных за текущий день
            df_tmp = pd.DataFrame(ch_client.execute(f"""select {', '.join(pk_list)} from {tmp_table_name}"""), columns=pk_list)
            df_fact = pd.DataFrame(ch_client.execute(f"""select {', '.join(pk_list)} from {tgt_table_name} where processed_dttm = today()"""), columns=pk_list)

            diff_rows = df_tmp.merge(df_fact[pk_list], on=pk_list, how='left', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)

            if diff_rows.empty:
                logger.info(f"Успешно загружены все записи. {len(df_tmp)} записей в {tgt_table_name} за пероид {loading_month} - {report_date_to}.")
                return tmp_table_name 
                #{'message': f"Удалена временная таблица {tmp_table_name} для прогрузки пероида {loading_month} - {report_date_to}."}
            else:
                class NotAllDatasLoad(Exception):
                    pass
                raise NotAllDatasLoad
        except NotAllDatasLoad as n:
            ch_client.execute(f"drop table {tmp_table_name}")
            logger.error(f"Не все данные прогружены из {tmp_table_name} в {tgt_table_name}. Прогружено {df_tmp.shape[0]-diff_rows.shape[0]} из {df_tmp.shape[0]} за пероид {loading_month} - {report_date_to}.")
        except Exception as e:
            ch_client.execute(f"drop table {tmp_table_name}")
            logger.error(f"Ошибка при загрузке {tgt_table_name}: {str(e)} за пероид {loading_month} - {report_date_to}")
        finally:
            ch_client.disconnect()
    
    # 7. Удаление временной таблицы
    @task(pool='sequential_processing', pool_slots=1)
    def clean_up_temp_altay_data(tmp_table_name, **kwargs):
        """Удаление временной таблицы """
        if not tmp_table_name:
            raise AirflowSkipException("Пропускаем очистку - нет данных от загрузки")
        ch_client = get_clickhouse_client()
        ch_client.execute(f"drop table {tmp_table_name}")
        logger.info(f"Удалена временная таблица {tmp_table_name}.")
        ch_client.disconnect()

    # Определение кол-ва периодов для прогрузки
    periods = get_loading_periods()

    # Определяем зависимости между задачами
    check_result = check_new_data_altay_data.expand(period=periods)
    load_task = load_altay_data.expand(datas=check_result)
    clean_task = clean_up_temp_altay_data.expand(tmp_table_name=load_task)

    # Последовательное выполнение: проверка -> загрузка -> очистка
    check_result >> load_task >> clean_task

# Инициализация DAG
wf_dsm_mart_dsm_sale_data()