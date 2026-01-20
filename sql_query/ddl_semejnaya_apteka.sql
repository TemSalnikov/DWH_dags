--drop table stg.mart_fpc_semejnaya_apteka_report on cluster cluster_2S_2R

create table stg.mart_fpc_semejnaya_apteka_report on cluster cluster_2S_2R
(
    uuid_report text,
    legal_entity text,
    pharmacy_name text,
    product_name text,
    invoice_number text,
    doc_date text,
    supplier text,
    quantity text,
    price text,
    amount text,
    start_stock text,
    start_sum text,
    income_quantity text,
    income_sum text,
    sale_quantity text,
    sale_sum text,
    remains_quantity text,
    remains_sum text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_semejnaya_apteka_report on cluster cluster_2S_2R

create table kafka.fpc_semejnaya_apteka_report on cluster cluster_2S_2R
(
    uuid_report text,
    legal_entity text,
    pharmacy_name text,
    product_name text,
    invoice_number text,
    doc_date text,
    supplier text,
    quantity text,
    price text,
    amount text,
    start_stock text,
    start_sum text,
    income_quantity text,
    income_sum text,
    sale_quantity text,
    sale_sum text,
    remains_quantity text,
    remains_sum text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_semejnaya_apteka_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_semejnaya_apteka_report_mv on cluster cluster_2S_2R

CREATE MATERIALIZED VIEW kafka.fpc_semejnaya_apteka_report_mv  on cluster cluster_2S_2R TO stg.mart_fpc_semejnaya_apteka_report AS 
SELECT * FROM kafka.fpc_semejnaya_apteka_report;

