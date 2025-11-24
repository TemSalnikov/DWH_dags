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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import psycopg2

# Настройка логирования
logger = LoggingMixin().log

# Деление временного интервала по месяцам
def check_and_split_date_range(start_date, end_date):
    """Разбивает временной интервал на месяцы"""

    class NotCorrectData(Exception):
        pass

    class TooBigPeriodDates(Exception):
        pass

    try:
        if not isinstance(start_dt, pendulum.DateTime) and isinstance(end_dt, pendulum.DateTime):
            start_dt = pendulum.parse(start_date)
            end_dt = pendulum.parse(end_date)

        diff_month = end_dt.diff(start_dt).in_months()

        if end_dt < start_dt:
            raise NotCorrectData
        elif diff_month > 12:
            raise TooBigPeriodDates
        else:
            all_dates = [start_dt.add(months=mnth) for mnth in range(diff_month)]
        return diff_month, all_dates
    except NotCorrectData as n:
        print(f"Дата окончания периода прогрузки меньше даты начала: {str(n)}")
        raise
    except Exception as e:
        print(f"Ошибка при вводе периода прогрузки: {str(e)}")
        raise
        
@dag(
    dag_id='cf_dsm_mart_dsm_sale_data',
    schedule_interval='0 9 6 * *', # в 9 утра каждого месяца 6 числа
    start_date=days_ago(1),
    catchup=False,
    params={
        'dates_from': Param(
            '-',
            type='string',
            description='Дата начала отчетного периода в формате YYYY-MM-01. Указывать тот месяц, за который требуется отчет. Если требуется прогрузка за 1 месяц, поле "dates_to" оставить пустым.'
        ),
        'dates_to': Param(
            '-',
            type='string',
            description='Дата окончания отчетного периода в формате YYYY-MM-01.'
        )
    },
    tags=['oracle', 'clickhouse', 'data_migration']
)
def cf_dsm_mart_dsm_sale_data():
    @task.short_circuit
    def create_date_parametrs(*args, **kwargs):
        """Генерация периодов для загрузки на основе параметров"""

        # Получаем параметр периода дат из контекста
        try:
            dag_run = kwargs.get('dag_run')
            report_date_from = dag_run.conf.get('dates_from')
            report_date_to = dag_run.conf.get('dates_to')
            
            if report_date_from == '-' and report_date_to == '-':
                # прогрузка данных за последний месяц + 1 и до текущего месяца
                logger.info(f"Запускается автоматическая прогрузка.")
                new_start_dt_loading = check_meta_start_dt_loading()
                today = pendulum.today()# перевести today к формату yyyy-mm-01

                if new_start_dt_loading:
                    if new_start_dt_loading < today: 
                        logger.info(f"Введены даты прогрузки данных, где dates_from = {last_dt_loading.format('YYYY-MM-DD')}, dates_to: {today.format('YYYY-MM-DD')}.")
                    else:
                        logger.info(f"Введены даты прогрузки данных за текущий месяц или более. Данных еще нет.")
                        return False
                else:
                # если прогрузок еще никогда не было, загружаем все за последние 2 месяца
                    new_start_dt_loading = today.substract(months=2)
                    logger.info(f"В метаданных таблицы airflow.public.meta_dsm еще не было прогрузок. Запуск прогрузки данных за последние два месяца: dates_from = {new_start_dt_loading.format('YYYY-MM-DD')}, dates_to: {today.format('YYYY-MM-DD')}.")
                
                periods = split_date_range(new_start_dt_loading, today)
                logger.info(f"Будет загружено {len(periods)} месяцев:")
                for i, period in enumerate(periods):
                    logger.info(f"Месяц {i+1}: {period}")
            elif report_date_to == '-':
                periods = [pendulum.fromformat(report_date_from, 'YYYY-MM-DD')]
                logger.info(f"Будет загружен 1 месяц: {report_date_from}")
            else:
                logger.info(f"Введены даты прогрузки данных, где dates_from = {report_date_from}, dates_to: {report_date_to}.")
                periods = split_date_range(report_date_from, report_date_to)
                logger.info(f"Будет загружено {len(periods)} месяцев:")
                for i, period in enumerate(periods):
                    logger.info(f"Месяц {i+1}: {period}")
            
            return periods
        except Exception as e:
            logger.error(f"Ошибка при вводе даты: {str(e)}")
            raise

    @task
    def trigger_dags(periods, **kwargs):
        for period in periods:
            trigger = TriggerDagRunOperator(
                task_id=f'trigger_{period}',
                trigger_dag_id='wf_dsm_mart_dsm_sale_data',
                conf={'loading_month': period.format('YYYY-MM-DD')},
                wait_for_completion=True # ждать завершения DAG перед следующим
            )

    task1 = create_date_parametrs()
    if task1:
        trigger_task = trigger_dags(task1)

    task1 >> trigger_task

cf_dsm_mart_dsm_sale_data()
