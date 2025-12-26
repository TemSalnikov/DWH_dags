--drop table stg.mart_fpc_erkafarm_report on cluster cluster_2S_2R
    'uuid_report',  
    'product_name', 
    'pharmacy_name', 
    'pharmacy_id', 
    'pharmacy_inn',         
    'pharmacy_address', 
    'pharmacy_city', 
    'pharmacy_jur_person', 
    'supplier_name',     
    'product_code_ap',      
    'product_code_2005',
    'purchase_price',      
    'row_sum_up',           
    'row_sum_cond_price',   
    'retail',              
    'quantity', 
    'name_pharm_chain', 
    'name_report',         
    'start_date', 
    'end_date', 
    'processed_dttm'
create table stg.mart_fpc_erkafarm_report on cluster cluster_2S_2R
(
    uuid_report text,
    product_name text,
    pharmacy_name text,
    pharmacy_id text,
    pharmacy_inn text,
    pharmacy_address text,
    pharmacy_city text,
    pharmacy_jur_person text,
    supplier_name text,
    product_code_ap text,
    product_code_2005 text,
    purchase_price text,
    row_sum_up text,
    row_sum_cond_price text,
    retail text,
    quantity text,
    name_pharm_chain text,
    name_report text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_erkafarm_report on cluster cluster_2S_2R

create table kafka.fpc_erkafarm_report on cluster cluster_2S_2R
(
    uuid_report text,
    product_name text,
    pharmacy_name text,
    pharmacy_id text,
    pharmacy_inn text,
    pharmacy_address text,
    pharmacy_city text,
    pharmacy_jur_person text,
    supplier_name text,
    product_code_ap text,
    product_code_2005 text,
    purchase_price text,
    row_sum_up text,
    row_sum_cond_price text,
    retail text,
    quantity text,
    name_pharm_chain text,
    name_report text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_erkafarm_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_erkafarm_report_mv on cluster cluster_2S_2R

CREATE MATERIALIZED VIEW kafka.fpc_erkafarm_report_mv  on cluster cluster_2S_2R TO stg.mart_fpc_erkafarm_report AS 
SELECT * FROM kafka.fpc_erkafarm_report;

