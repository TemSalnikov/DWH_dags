import psycopg2
import pandas as pd
from clickhouse_driver import Client
#from libs.functions_dwh.functions_dds import get_clickhouse_client
import libs.functions_dwh.functions_dsm

"""
def create_connection_psycopg2():
    conn = psycopg2.connect(
    dbname='airflow',
    user='airflow',
    password='airflow',
    host='192.168.14.235',
    port='5432')
    return conn

import pendulum

dt = pendulum.today()

print(dt)

#start_dt = pendulum.parse(dt)

#print(type(dt), isinstance(dt, datetime))

dt = pendulum.datetime(2015, 2, 5)
print(isinstance(dt, pendulum.DateTime))

CLICKHOUSE_CONN: dict[str, str | int] = {
    'host': '192.168.14.235',
    'port': 9001,
    'user': 'admin',
    'password': 'admin',
    'database': 'stg'
}

def get_clickhouse_client() -> Client:
    '''Создает и возвращает клиент ClickHouse'''
    return Client(
        host=CLICKHOUSE_CONN['host'],
        port=CLICKHOUSE_CONN['port'],
        user=CLICKHOUSE_CONN['user'],
        password=CLICKHOUSE_CONN['password'],
        database=CLICKHOUSE_CONN['database']
    )

ch_client = get_clickhouse_client()
df_click = ch_client.execute(f'''select max(processed_dttm) from mart_dsm_sale'''0][0]

print(df_click)

"""
print(type(create_connection_psycopg2))
