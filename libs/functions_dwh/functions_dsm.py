from clickhouse_driver import Client
import oracledb
import psycopg2
import hashlib
from clickhouse_driver.errors import Error as ClickhouseError
import pendulum

from airflow.decorators import task
from airflow.utils.log.logging_mixin import LoggingMixin

CLICKHOUSE_CONN: dict[str, str | int] = {
    'host': '192.168.14.235',
    'port': 9001,
    'user': 'admin',
    'password': 'admin'
}

ORACLE_CONN = {
    'user': 'ALTAYV',
    'password': 'sSwM913_xoAY', 
    'host': 'dsmviewer.ru',
    'port': 27091,
    'sid': 'webiasdb2'
}

def get_clickhouse_client():
    """Создание клиента ClickHouse"""
    try:
        return Client(
        host=CLICKHOUSE_CONN['host'],
        port=CLICKHOUSE_CONN['port'],
        user=CLICKHOUSE_CONN['user'],
        password=CLICKHOUSE_CONN['password']
    )
    except ClickhouseError as e:
        logger.error(f"Ошибка подключения к ClickHouse: {e}")
        raise

# Функции подключения к БД
def get_oracle_connection():
    dsn = f"{ORACLE_CONN['host']}:{ORACLE_CONN['port']}/{ORACLE_CONN['sid']}"
    return oracledb.connect(
        user=ORACLE_CONN['user'],
        password=ORACLE_CONN['password'],
        dsn=dsn
    )

def create_connection_psycopg2():
    conn = psycopg2.connect(
    dbname='airflow',
    user='airflow',
    password='airflow',
    host='192.168.14.235',
    port='5432')
    return conn

def compute_row_hash(row, columns=None):
    """Создает хеш строки"""
    if columns:
        row = row[columns]
    # Преобразуем все значения строки в строки и объединяем их
    row_string = ''.join(str(value) for value in row)
    # Создаем хеш используя SHA-256
    return hashlib.sha256(row_string.encode()).hexdigest()

@task
def save_meta(effective_dttm: str, processed_dttm: str, **context):
    tbl_smeta_dsm = 'airflow.public.meta_dsm'

    try:
        logger = LoggingMixin().log
        dag_id = context["dag_run"].dag_id if "dag_run" in context else ''
        logger.info(f'Успешно получено dag_id {dag_id}!')
        run_id = context["dag_run"].run_id if "dag_run" in context else ''
        logger.info(f'Успешно получено run_id {run_id}!')  

        with create_connection_psycopg2() as potsgr_conn:
            #conn = None
            #hook = PostgresHook(postgres_conn_id="postgres_conn")
            #conn = hook.get_conn()
            cur = potsgr_conn.cursor()
            cur.execute(f"""
                INSERT INTO {tbl_smeta_dsm} (dag_id, run_id, effective_dttm, processed_dttm)
                VALUES ('{dag_id}','{run_id}','{effective_dttm}', '{processed_dttm}')
            """)
            potsgr_conn.commit()
        return True
    except Exception as e:
        logger.error(f"Ошибка работы с PostgreSQL: {str(e)}")
        # Fallback: ждем минимальный интервал
        raise

def check_meta_start_dt_loading():
    """Вывод новой даты прогрузки"""
    with create_connection_psycopg2() as potsgr_conn:
        tbl_smeta_dsm = 'airflow.public.meta_dsm'
        cur = potsgr_conn.cursor()
        last_dt_loading = cur.execute(f'''select max(effective_dttm) from {tbl_smeta_dsm}''')[0][0]
        new_start_dt_loading = pendulum.fromformat(last_dt_loading, 'YYYY-MM-DD').add(months=1)
        return new_start_dt_loading
                