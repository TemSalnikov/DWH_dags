--drop table stg.mart_fpc_zdorovie_report on cluster cluster_2S_2R

create table stg.mart_fpc_zdorovie_report on cluster cluster_2S_2R
(
    uuid_report text,
    product_name text,
    quantity text,
    supplier text,
    invoice_number text,
    invoice_date text,
    pharmacy_name text,
    legal_entity text,
    brand text,
    city_type text,
    city text,
    address text,
    inn text,
    category_ntz text,
    category_display text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_zdorovie_report on cluster cluster_2S_2R

create table kafka.fpc_zdorovie_report on cluster cluster_2S_2R
(
    uuid_report text,
    product_name text,
    quantity text,
    supplier text,
    invoice_number text,
    invoice_date text,
    pharmacy_name text,
    legal_entity text,
    brand text,
    city_type text,
    city text,
    address text,
    inn text,
    category_ntz text,
    category_display text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_zdorovie_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_zdorovie_report_mv on cluster cluster_2S_2R

CREATE MATERIALIZED VIEW kafka.fpc_zdorovie_report_mv  on cluster cluster_2S_2R TO stg.mart_fpc_zdorovie_report AS 
SELECT * FROM kafka.fpc_zdorovie_report;

