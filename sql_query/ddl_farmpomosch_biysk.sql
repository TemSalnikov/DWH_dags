--drop table stg.mart_fpc_farmpomosch_biysk_report on cluster cluster_2S_2R
    'uuid_report', 'invoice_number', 'doc_date', 'product_name', 'supplier', 
    'pharmacy_name', 'legal_entity', 'price', 'purchase_quantity', 
    'purchase_sum', 'vat', 'quantity',
    'name_report', 'name_pharm_chain', 'start_date', 'end_date', 'processed_dttm'
create table stg.mart_fpc_farmpomosch_biysk_report on cluster cluster_2S_2R
(
    uuid_report text,
    invoice_number text,
    doc_date text,
    product_name text,
    supplier text,
    pharmacy_name text,
    legal_entity text,
    price text,
    purchase_quantity text,
    purchase_sum text,
    vat text,
    quantity text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_farmpomosch_biysk_report on cluster cluster_2S_2R

create table kafka.fpc_farmpomosch_biysk_report on cluster cluster_2S_2R
(
    uuid_report text,
    invoice_number text,
    doc_date text,
    product_name text,
    supplier text,
    pharmacy_name text,
    legal_entity text,
    price text,
    purchase_quantity text,
    purchase_sum text,
    vat text,
    quantity text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_farmpomosch_biysk_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_farmpomosch_biysk_report_mv on cluster cluster_2S_2R

CREATE MATERIALIZED VIEW kafka.fpc_farmpomosch_biysk_report_mv  on cluster cluster_2S_2R TO stg.mart_fpc_farmpomosch_biysk_report AS 
SELECT * FROM kafka.fpc_farmpomosch_biysk_report;

