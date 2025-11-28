-- dag_id - название потока
-- effective_dttm - дата отчета
-- processed_dttm - дата прогрузки потока
create table airflow.public.meta_dsm (dag_id text, run_id text, effective_dttm timestamp , processed_dttm timestamp);

select * from airflow.public.meta_dsm;