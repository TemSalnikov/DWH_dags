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

	# CREATE table if not exists dds.sale
	# (
	#   sale_uuid String
	# , sale_date String
	# , counterparty_uuid String
	# , counterparty_salepoint_uuid String
	# , product_uuid String
	# , gtin_code String
	# , series_code String
	# , best_before_date String
	# , type_of_disposal_name String
	# , sale_sum int32
	# , sale_cnt int32
	# , source_of_financing_name String
	# , update_date DateTime
	# , type_of_export_name String
	# , completeness_of_disposal_name String
	# -- обязательные поля
	# , effective_from_dttm DateTime
	# , effective_to_dttm DateTime
	# , processed_dttm DateTime
	# , deleted_flg bool
	# , src String
	# , hash_diff String
	# )
	# ENGINE = MergeTree
	# order by (sale_uuid, hash_diff)

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
    
    # Алгоритм 1: mart_mdlp_general_report_on_disposal (полные данные участника)
    @task
    def get_inc_load_data(p_version: dict, name_src_tbl:str) -> str:
        client = None
        logger = LoggingMixin().log
        tmp_table_name = f"tmp.tmp_algo1_{tg_sur.uuid.uuid4().hex}"
        logger.info(f"получены параметры: {p_version}: {type(p_version)}")
        p_version_prev = p_version['p_version_prev'][name_src_tbl][:19]
        p_version_new = p_version['p_version_new'][name_src_tbl][:19]
        logger.info(f"получены параметры: {p_version_prev}: {p_version_new}")
        try:
            
            client = tg_sur.get_clickhouse_client()
            logger.info("Подключение к ClickHouse успешно выполнено")
            
            query = f"""
            CREATE TABLE {tmp_table_name} 
            ENGINE = MergeTree()
            PRIMARY KEY (inn_code, address_name)
            ORDER BY (inn_code, address_name)
            AS 
            SELECT 
                tin_of_the_participant as inn_code,
                name_of_the_participant as counterparty_name,
                code_of_the_subject_of_the_russian_federation as the_subject_code,
                the_subject_of_the_russian_federation as the_subject_name,
                settlement as settlement_name,
                district as district_name,
                trim(BOTH ', ' FROM 
				        if(
						        match(address, '^[0-9]{6},'),
						        trimLeft(substring(address, position(address, ',') + 1)),
						        address
						    )
				    )
                 as address_name,
                identifier_md_participant as md_system_code,
                'MDLP' as src,
                toDateTime('1990-01-01 00:01:01') as effective_dttm,
                1 as algorithm_type,
                concat(toString(tin_of_the_participant), '^^', address_name) as salepoint_business_key
            FROM stg.v_iv_mart_mdlp_general_report_on_disposal(p_from_dttm = '{p_version_prev}', p_to_dttm = '{p_version_new}')
            """
            
            logger.info(f"Создан запрос для алгоритма 1: {tmp_table_name}: \n {query}")
            client.execute(query)
            logger.info(f"Создана временная таблица {tmp_table_name} для алгоритма 1")
            
            return tmp_table_name
            
        except tg_sur.ClickhouseError as e:
            logger.error(f"Ошибка при получении данных алгоритма 1: {e}")
            raise
        finally:
            if client:
                client.disconnect()
                logger.debug("Подключение к ClickHouse закрыто")
    
    @task
    def get_prepared_data(tmp_table: str, hub_table: str, src_pk:str,  hub_pk: str, hub_id: str) -> str:
        """Получение ключей из Hub"""
        client = None
        
        try:
            tmp_table_name = f"tmp.tmp_preload_{tg_sur.uuid.uuid4().hex}"
            logger = LoggingMixin().log # ПОЧЕМУ НЕ ВЫНЕСТИ КАК ГЛОАБЛЬНУЮ ПЕРМЕННУЮ?
            client = tg_sur.get_clickhouse_client()
            logger.info(f"Подклчение к clickhouse успешно выполнено")

            query_set = "SET allow_experimental_join_condition = 1"
            client.execute(query_set)

            ### надо подумать над запросом
            query = f"""
            CREATE TABLE {tmp_table_name} 
            ENGINE = MergeTree()
            PRIMARY KEY ({', '.join(pk_list)})
            ORDER BY ({', '.join(pk_list)})
            AS
            SELECT DISTINCT 
                h.{hub_pk} as {name_sur_key},
                t.*,
                toDateTime('1990-01-01 00:01:01') as effective_from_dttm,
                toDateTime('2999-12-31 23:59:59') as effective_to_dttm
            FROM {tmp_table} t
            JOIN dds.v_sn_{hub_table[4:]} h 
                ON t.{src_pk} = h.{hub_id} AND t.src = h.src AND h.effective_from_dttm <= t.effective_dttm
                AND h.effective_to_dttm > t.effective_dttm
            """
            logger.info(f"Создан запрос: {query}")

            client.execute(query)
            logger.info(f"Создана временная таблица {tmp_table_name}")
            
            return tmp_table_name
            
        except tg_sur.ClickhouseError as e:
            logger.error(f"Ошибка при получении данных из {tmp_table}: {e}")
            raise
        finally:
            if client:
                client.disconnect()
                logger.debug("Подключение к ClickHouse закрыто")
    
    check_task = check_data_availability()
    parametrs_data_task = prepare_parameters(check_task)
    inc_table_task = get_inc_load_data(src_table_name,pk_list, bk_list, parametrs_data_task['p_version_prev'], parametrs_data_task['p_version_new'])
    generate_sur_key_task = tg_sur.hub_load_processing_tasks(hub_table_name, inc_table_task, pk_list[0], 'product_uuid', 'product_id')
    prepared_data_task = get_prepared_data(inc_table_task, hub_table_name, pk_list[0], 'product_uuid', 'product_id')
    # hist_p2i_task = convert_hist_p2i(prepared_data_task, pk_list)
    load_delta_task = load_delta(prepared_data_task, tgt_table_name, ['product_uuid'], bk_list_dds)
    save_meta_task = save_meta(load_delta_task, parametrs_data_task['p_version_new'])
    
    check_task >> parametrs_data_task >> inc_table_task >> generate_sur_key_task >> prepared_data_task
    inc_table_task >> prepared_data_task >> load_delta_task >> save_meta_task

wf_app_dsm_stg_dds_sale()

    
    
    
