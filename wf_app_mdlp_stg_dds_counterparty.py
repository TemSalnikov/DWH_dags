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
import dds_group_tasks.task_group_creation_surogate_salepoint as tg_sur_salepoint
from functions_dwh.functions_dds import load_delta, save_meta
import json



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
}

@dag(
    dag_id='wf_app_mdlp_stg_dds_counterparty',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['advanced'],
    params = {
        "p_version_prev": Param({'mart_mdlp_general_report_on_disposal':'2025-01-01',
                                 'mart_mdlp_general_pricing_report':'2025-01-01',
                                 'mart_mdlp_general_report_on_movement':'2025-01-01'}, title = "Dict с create_dttm предыдущей выгрузки иточников"),
        "p_version_new": Param({'mart_mdlp_general_report_on_disposal':'2025-01-01',
                                 'mart_mdlp_general_pricing_report':'2025-01-01',
                                 'mart_mdlp_general_report_on_movement':'2025-01-01'}, title = "Dict с create_dttm новой выгрузки иточников")
    }
)

def wf_app_mdlp_stg_dds_counterparty():
    tgt_table_name = 'dds.counterparty'
    hub_counterparty_table = 'dds.hub_counterparty'
    hub_salepoint_table = 'dds.hub_counterparty_salepoint'
    pk_list_dds = ['counterparty_uuid', 'counterparty_salepoint_uuid']
    
    bk_list_dds = [
        'inn_code', 'counterparty_name', 'the_subject_code', 'the_subject_name',
        'settlement_name', 'district_name', 'address_name', 'md_system_code'
    ]
    
    name_counterparty_sur_key = 'counterparty_uuid'
    name_salepoint_sur_key = 'counterparty_salepoint_uuid'

    @task
    def check_data_availability() -> bool:
        return True

    @task
    def prepare_parameters(data_ready: bool, **context) -> dict:
        if data_ready:
            logger = LoggingMixin().log
            dag_run_conf = context["dag_run"].conf if "dag_run" in context else {}
            # for name_par, val_par in context["params"].items():
            #     dag_run_conf[name_par] = json.loads(val_par)
            
            # logger.info(f"Получен набор параметров: {context["params"]}")
            logger.info(f"Обработанный набор параметров набор параметров: {dag_run_conf}")
            parameters = {**context["params"]
                        #   , **dag_run_conf
                          }
            logger.info(f"Получен набор параметров: {parameters}, {type(parameters)}")
            return parameters
        else: 
            raise Exception("Data not ready")

    # Алгоритм 1: mart_mdlp_general_report_on_disposal (полные данные участника)
    @task
    def get_inc_load_data_algo1(p_version: dict, name_src_tbl:str) -> str:
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
                trim(replaceRegexpAll(
                        replaceRegexpAll(
                            replaceRegexpAll(address, '\\d{6}', ''),
                            ',\\s*,', 
                            ','
                        ),
                        '\\s+', ' ' )
                    ) as address_name,
                identifier_md_participant as md_system_code,
                'MDLP' as src,
                toDateTime('1990-01-01 00:01:01') as effective_dttm,
                1 as algorithm_type,
                concat(toString(tin_of_the_participant), '^^', trim(replaceRegexpAll(
                                                                        replaceRegexpAll(
                                                                            replaceRegexpAll(address, '\\d{6}', ''),
                                                                            ',\\s*,', 
                                                                            ','
                                                                        ),
                                                                        '\\s+', ' ' )
                                                                    )) as salepoint_business_key
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

    # Алгоритм 2: mart_mdlp_general_report_on_disposal (эмитент)
    @task
    def get_inc_load_data_algo2(p_version: dict, name_src_tbl:str) -> str:
        client = None
        tmp_table_name = f"tmp.tmp_algo2_{tg_sur.uuid.uuid4().hex}"
        
        try:
            logger = LoggingMixin().log
            client = tg_sur.get_clickhouse_client()
            logger.info("Подключение к ClickHouse успешно выполнено")
            p_version_prev = p_version['p_version_prev'][name_src_tbl][:19]
            p_version_new = p_version['p_version_new'][name_src_tbl][:19]
            query = f"""
            CREATE TABLE {tmp_table_name} 
            ENGINE = MergeTree()
            PRIMARY KEY (inn_code, address_name)
            ORDER BY (inn_code, address_name)
            AS 
            SELECT 
                tin_to_the_issuer as inn_code,
                the_name_of_the_issuer as counterparty_name,
                NULL as the_subject_code,
                NULL as the_subject_name,
                NULL as settlement_name,
                NULL as district_name,
                'DEFAULT_SALEPOINT' as address_name,
                NULL as md_system_code,
                'MDLP' as src,
                toDateTime('1990-01-01 00:01:01') as effective_dttm,
                2 as algorithm_type,
                concat(toString(tin_to_the_issuer), '^^', 'DEFAULT_SALEPOINT') as salepoint_business_key
            FROM stg.v_iv_mart_mdlp_general_report_on_disposal(p_from_dttm = '{p_version_prev}', p_to_dttm = '{p_version_new}')
            """
            
            logger.info(f"Создан запрос для алгоритма 2: {query}")
            client.execute(query)
            logger.info(f"Создана временная таблица {tmp_table_name} для алгоритма 2")
            
            return tmp_table_name
            
        except tg_sur.ClickhouseError as e:
            logger.error(f"Ошибка при получении данных алгоритма 2: {e}")
            raise
        finally:
            if client:
                client.disconnect()
                logger.debug("Подключение к ClickHouse закрыто")

    # Алгоритм 3: mart_mdlp_general_pricing_report (эмитент)
    @task
    def get_inc_load_data_algo3(p_version: dict, name_src_tbl:str) -> str:
        client = None
        tmp_table_name = f"tmp.tmp_algo3_{tg_sur.uuid.uuid4().hex}"
        
        try:
            logger = LoggingMixin().log
            client = tg_sur.get_clickhouse_client()
            logger.info("Подключение к ClickHouse успешно выполнено")
            p_version_prev = p_version['p_version_prev'][name_src_tbl][:19]
            p_version_new = p_version['p_version_new'][name_src_tbl][:19]
            query = f"""
            CREATE TABLE {tmp_table_name} 
            ENGINE = MergeTree()
            PRIMARY KEY (inn_code, address_name)
            ORDER BY (inn_code, address_name)
            AS 
            SELECT 
                tin_to_the_issuer as inn_code,
                the_name_of_the_issuer as counterparty_name,
                NULL as the_subject_code,
                NULL as the_subject_name,
                NULL as settlement_name,
                NULL as district_name,
                'DEFAULT_SALEPOINT' as address_name,
                NULL as md_system_code,
                'MDLP' as src,
                toDateTime('1990-01-01 00:01:01') as effective_dttm,
                3 as algorithm_type,
                concat(toString(tin_to_the_issuer), '^^', 'DEFAULT_SALEPOINT') as salepoint_business_key
            FROM stg.v_iv_mart_mdlp_general_pricing_report(p_from_dttm = '{p_version_prev}', p_to_dttm = '{p_version_new}')
            """
            
            logger.info(f"Создан запрос для алгоритма 3: {query}")
            client.execute(query)
            logger.info(f"Создана временная таблица {tmp_table_name} для алгоритма 3")
            
            return tmp_table_name
            
        except tg_sur.ClickhouseError as e:
            logger.error(f"Ошибка при получении данных алгоритма 3: {e}")
            raise
        finally:
            if client:
                client.disconnect()
                logger.debug("Подключение к ClickHouse закрыто")

    # Алгоритм 4: mart_mdlp_general_pricing_report (участник)
    @task
    def get_inc_load_data_algo4(p_version: dict, name_src_tbl:str) -> str:
        client = None
        tmp_table_name = f"tmp.tmp_algo4_{tg_sur.uuid.uuid4().hex}"
        
        try:
            logger = LoggingMixin().log
            client = tg_sur.get_clickhouse_client()
            logger.info("Подключение к ClickHouse успешно выполнено")
            p_version_prev = p_version['p_version_prev'][name_src_tbl][:19]
            p_version_new = p_version['p_version_new'][name_src_tbl][:19]
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
                NULL as settlement_name,
                NULL as district_name,
                'DEFAULT_SALEPOINT' as address_name,
                NULL as md_system_code,
                'MDLP' as src,
                toDateTime('1990-01-01 00:01:01') as effective_dttm,
                4 as algorithm_type,
                concat(toString(tin_of_the_participant), '^^', 'DEFAULT_SALEPOINT') as salepoint_business_key
            FROM stg.v_iv_mart_mdlp_general_pricing_report(p_from_dttm = '{p_version_prev}', p_to_dttm = '{p_version_new}')
            """
            
            logger.info(f"Создан запрос для алгоритма 4: {query}")
            client.execute(query)
            logger.info(f"Создана временная таблица {tmp_table_name} для алгоритма 4")
            
            return tmp_table_name
            
        except tg_sur.ClickhouseError as e:
            logger.error(f"Ошибка при получении данных алгоритма 4: {e}")
            raise
        finally:
            if client:
                client.disconnect()
                logger.debug("Подключение к ClickHouse закрыто")

    # Алгоритм 5: mart_mdlp_general_report_on_movement (эмитент)
    @task
    def get_inc_load_data_algo5(p_version: dict, name_src_tbl:str) -> str:
        client = None
        tmp_table_name = f"tmp.tmp_algo5_{tg_sur.uuid.uuid4().hex}"
        
        try:
            logger = LoggingMixin().log
            client = tg_sur.get_clickhouse_client()
            logger.info("Подключение к ClickHouse успешно выполнено")
            p_version_prev = p_version['p_version_prev'][name_src_tbl][:19]
            p_version_new = p_version['p_version_new'][name_src_tbl][:19]
            query = f"""
            CREATE TABLE {tmp_table_name} 
            ENGINE = MergeTree()
            PRIMARY KEY (inn_code, address_name)
            ORDER BY (inn_code, address_name)
            AS 
            SELECT 
                tin_to_the_issuer as inn_code,
                the_name_of_the_issuer as counterparty_name,
                NULL as the_subject_code,
                NULL as the_subject_name,
                NULL as settlement_name,
                NULL as district_name,
                'DEFAULT_SALEPOINT' as address_name,
                NULL as md_system_code,
                'MDLP' as src,
                toDateTime('1990-01-01 00:01:01') as effective_dttm,
                5 as algorithm_type,
                concat(toString(tin_to_the_issuer), '^^', 'DEFAULT_SALEPOINT') as salepoint_business_key
            FROM stg.v_iv_mart_mdlp_general_report_on_movement(p_from_dttm = '{p_version_prev}', p_to_dttm = '{p_version_new}')
            """
            
            logger.info(f"Создан запрос для алгоритма 5: {query}")
            client.execute(query)
            logger.info(f"Создана временная таблица {tmp_table_name} для алгоритма 5")
            
            return tmp_table_name
            
        except tg_sur.ClickhouseError as e:
            logger.error(f"Ошибка при получении данных алгоритма 5: {e}")
            raise
        finally:
            if client:
                client.disconnect()
                logger.debug("Подключение к ClickHouse закрыто")

    # Алгоритм 6: mart_mdlp_general_report_on_movement (отправитель)
    @task
    def get_inc_load_data_algo6(p_version: dict, name_src_tbl:str) -> str:
        client = None
        tmp_table_name = f"tmp.tmp_algo6_{tg_sur.uuid.uuid4().hex}"
        
        try:
            logger = LoggingMixin().log
            client = tg_sur.get_clickhouse_client()
            logger.info("Подключение к ClickHouse успешно выполнено")
            p_version_prev = p_version['p_version_prev'][name_src_tbl][:19]
            p_version_new = p_version['p_version_new'][name_src_tbl][:19]
            query = f"""
            CREATE TABLE {tmp_table_name} 
            ENGINE = MergeTree()
            PRIMARY KEY (inn_code, address_name)
            ORDER BY (inn_code, address_name)
            AS 
            SELECT 
                tin_of_the_sender as inn_code,
                NULL as counterparty_name,
                NULL as the_subject_code,
                NULL as the_subject_name,
                NULL as settlement_name,
                NULL as district_name,
                'DEFAULT_SALEPOINT' as address_name,
                identifier_md_of_the_sender as md_system_code,
                'MDLP' as src,
                toDateTime('1990-01-01 00:01:01') as effective_dttm,
                6 as algorithm_type,
                concat(toString(tin_of_the_sender), '^^', 'DEFAULT_SALEPOINT') as salepoint_business_key
            FROM stg.v_iv_mart_mdlp_general_report_on_movement(p_from_dttm = '{p_version_prev}', p_to_dttm = '{p_version_new}')
            """
            
            logger.info(f"Создан запрос для алгоритма 6: {query}")
            client.execute(query)
            logger.info(f"Создана временная таблица {tmp_table_name} для алгоритма 6")
            
            return tmp_table_name
            
        except tg_sur.ClickhouseError as e:
            logger.error(f"Ошибка при получении данных алгоритма 6: {e}")
            raise
        finally:
            if client:
                client.disconnect()
                logger.debug("Подключение к ClickHouse закрыто")

    # Алгоритм 7: mart_mdlp_general_report_on_movement (получатель)
    @task
    def get_inc_load_data_algo7(p_version: dict, name_src_tbl:str) -> str:
        client = None
        tmp_table_name = f"tmp.tmp_algo7_{tg_sur.uuid.uuid4().hex}"
        
        try:
            logger = LoggingMixin().log
            client = tg_sur.get_clickhouse_client()
            logger.info("Подключение к ClickHouse успешно выполнено")
            p_version_prev = p_version['p_version_prev'][name_src_tbl][:19]
            p_version_new = p_version['p_version_new'][name_src_tbl][:19]
            query = f"""
            CREATE TABLE {tmp_table_name} 
            ENGINE = MergeTree()
            PRIMARY KEY (inn_code, address_name)
            ORDER BY (inn_code, address_name)
            AS 
            SELECT 
                tin_of_the_recipient as inn_code,
                name_of_the_recipient as counterparty_name,
                NULL as the_subject_code,
                NULL as the_subject_name,
                NULL as settlement_name,
                NULL as district_name,
                'DEFAULT_SALEPOINT' as address_name,
                identifier_md_of_the_recipient as md_system_code,
                'MDLP' as src,
                toDateTime('1990-01-01 00:01:01') as effective_dttm,
                7 as algorithm_type,
                concat(toString(tin_of_the_recipient), '^^', 'DEFAULT_SALEPOINT') as salepoint_business_key
            FROM stg.v_iv_mart_mdlp_general_report_on_movement(p_from_dttm = '{p_version_prev}', p_to_dttm = '{p_version_new}')
            """
            
            logger.info(f"Создан запрос для алгоритма 7: {query}")
            client.execute(query)
            logger.info(f"Создана временная таблица {tmp_table_name} для алгоритма 7")
            
            return tmp_table_name
            
        except tg_sur.ClickhouseError as e:
            logger.error(f"Ошибка при получении данных алгоритма 7: {e}")
            raise
        finally:
            if client:
                client.disconnect()
                logger.debug("Подключение к ClickHouse закрыто")

    @task
    def union_all_algorithms(algo1_table: str, algo2_table: str, algo3_table: str, 
                           algo4_table: str, algo5_table: str, algo6_table: str, 
                           algo7_table: str) -> str:
        """Объединение данных всех алгоритмов в одну таблицу"""
        client = None
        tmp_table_name = f"tmp.tmp_union_all_counterparty_{tg_sur.uuid.uuid4().hex}"
        
        try:
            logger = LoggingMixin().log
            client = tg_sur.get_clickhouse_client()
            logger.info("Подключение к ClickHouse успешно выполнено")

            query = f"""
            CREATE TABLE {tmp_table_name} 
            ENGINE = MergeTree()
            PRIMARY KEY (inn_code, address_name)
            ORDER BY (inn_code, address_name)
            AS 
            SELECT * FROM {algo1_table}
            UNION ALL
            SELECT * FROM {algo2_table}
            UNION ALL
            SELECT * FROM {algo3_table}
            UNION ALL
            SELECT * FROM {algo4_table}
            UNION ALL
            SELECT * FROM {algo5_table}
            UNION ALL
            SELECT * FROM {algo6_table}
            UNION ALL
            SELECT * FROM {algo7_table}
            """
            
            logger.info(f"Создан запрос для объединения всех алгоритмов: {tmp_table_name}")
            client.execute(query)
            logger.info(f"Создана объединенная временная таблица {tmp_table_name}")
            
            return tmp_table_name
            
        except tg_sur.ClickhouseError as e:
            logger.error(f"Ошибка при объединении алгоритмов: {e}")
            raise
        finally:
            if client:
                client.disconnect()
                logger.debug("Подключение к ClickHouse закрыто")

    @task
    def join_surrogate_keys(inc_table: str, counterparty_hub_table: str, salepoint_hub_table: str, threshold: float = 0.35) -> str:
        """Объединение суррогатных ключей из двух хабов"""
        #threshold: float = 0.35  Чем меньше, тем больше сходство. Обычно 0.3–0.5.
        client = None
        tmp_table_name = f"tmp.tmp_joined_counterparty_{tg_sur.uuid.uuid4().hex}"
        
        try:
            logger = LoggingMixin().log
            client = tg_sur.get_clickhouse_client()
            logger.info("Подключение к ClickHouse успешно выполнено")

            query_set = "SET allow_experimental_join_condition = 1"
            client.execute(query_set)

            query = f"""
            CREATE TABLE {tmp_table_name} 
            ENGINE = MergeTree()
            PRIMARY KEY (inn_code, address_name)
            ORDER BY (inn_code, address_name)
            AS
            SELECT 
                t.*,
                hc.counterparty_pk as {name_counterparty_sur_key},
                hs.counterparty_salepoint_pk as {name_salepoint_sur_key},
                toDateTime('1990-01-01 00:01:01') as effective_from_dttm,
                toDateTime('2999-12-31 23:59:59') as effective_to_dttm
            FROM {inc_table} t
            LEFT JOIN {counterparty_hub_table} hc 
                ON t.inn_code = hc.counterparty_id 
                AND t.src = hc.src 
                AND hc.effective_from_dttm <= t.effective_dttm
                AND hc.effective_to_dttm > t.effective_dttm
            LEFT JOIN {salepoint_hub_table} hs 
                ON ngramDistanceUTF8(t.salepoint_business_key, hs.counterparty_salepoint_id) <= {threshold}
                AND t.src = hs.src 
                AND hs.effective_from_dttm <= t.effective_dttm
                AND hs.effective_to_dttm > t.effective_dttm
            """
            
            logger.info(f"Создан запрос для объединения суррогатных ключей: {query}")
            client.execute(query)
            logger.info(f"Создана временная таблица {tmp_table_name} с объединенными ключами")
            
            return tmp_table_name
            
        except tg_sur.ClickhouseError as e:
            logger.error(f"Ошибка при объединении суррогатных ключей: {e}")
            raise
        finally:
            if client:
                client.disconnect()
                logger.debug("Подключение к ClickHouse закрыто")

    # Задачи DAG
    check_task = check_data_availability()
    parameters_task = prepare_parameters(check_task)
    
    # Параллельное выполнение всех алгоритмов
    algo1_task = get_inc_load_data_algo1(parameters_task, name_src_tbl = 'mart_mdlp_general_report_on_disposal')
    algo2_task = get_inc_load_data_algo2(parameters_task, name_src_tbl = 'mart_mdlp_general_report_on_disposal')
    algo3_task = get_inc_load_data_algo3(parameters_task, name_src_tbl = 'mart_mdlp_general_pricing_report')
    algo4_task = get_inc_load_data_algo4(parameters_task,name_src_tbl = 'mart_mdlp_general_pricing_report')
    algo5_task = get_inc_load_data_algo5(parameters_task,name_src_tbl = 'mart_mdlp_general_pricing_report')
    algo6_task = get_inc_load_data_algo6(parameters_task,name_src_tbl = 'mart_mdlp_general_report_on_movement')
    algo7_task = get_inc_load_data_algo7(parameters_task, name_src_tbl = 'mart_mdlp_general_report_on_movement')
    
    
    # Объединение результатов всех алгоритмов
    union_table_task = union_all_algorithms(algo1_task, algo2_task, algo3_task, algo4_task, 
                                          algo5_task, algo6_task, algo7_task)
    
    # Генерация суррогатных ключей для контрагентов (по inn_code)
    generate_counterparty_sur_key_task = tg_sur.hub_load_processing_tasks(
        hub_counterparty_table,
        union_table_task,
        'inn_code',
        'counterparty_pk',
        'counterparty_id'
    )
    
    # Генерация суррогатных ключей для точек продаж (по составному ключу)
    generate_salepoint_sur_key_task = tg_sur_salepoint.hub_load_processing_tasks(
        hub_salepoint_table,
        union_table_task,
        'salepoint_business_key',
        'counterparty_salepoint_pk',
        'counterparty_salepoint_id'
    )
    
    # Объединение суррогатных ключей из двух хабов
    joined_keys_task = join_surrogate_keys(
        union_table_task, 
        generate_counterparty_sur_key_task,
        generate_salepoint_sur_key_task
    )
    
    # Загрузка данных в целевую таблицу
    load_delta_task = load_delta(joined_keys_task, tgt_table_name, pk_list_dds, bk_list_dds)
    save_meta_task = save_meta(load_delta_task, parameters_task['p_version_new'])
    
    # Определение зависимостей
    check_task >> parameters_task
    
    # Все алгоритмы выполняются параллельно после parameters_task
    parameters_task >> [algo1_task, algo2_task, algo3_task, algo4_task, 
                       algo5_task, algo6_task, algo7_task] >> union_table_task
    
    union_table_task >> [generate_counterparty_sur_key_task, generate_salepoint_sur_key_task] >> joined_keys_task
    joined_keys_task >> load_delta_task >> save_meta_task

wf_app_mdlp_stg_dds_counterparty()