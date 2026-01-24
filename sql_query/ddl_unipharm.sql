--drop table stg.mart_fpc_unipharm_report

create table stg.mart_fpc_unipharm_report
(
    uuid_report text,
    product_code text,
    product_name text,
    manufacturer text,
    vital_sign text,
    contractor text,
    pharmacy_name text,
    product_category text,
    marking_sign text,
    purchase_quantity text,
    purchase_sum_opt text,
    purchase_sum_retail text,
    markup_percent text,
    doc_date text,
    supplier text,
    batch text,
    doc_number text,
    manufacturer_barcode text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_unipharm_report

create table kafka.fpc_unipharm_report
(
    uuid_report text,
    product_code text,
    product_name text,
    manufacturer text,
    vital_sign text,
    contractor text,
    pharmacy_name text,
    product_category text,
    marking_sign text,
    purchase_quantity text,
    purchase_sum_opt text,
    purchase_sum_retail text,
    markup_percent text,
    doc_date text,
    supplier text,
    batch text,
    doc_number text,
    manufacturer_barcode text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_unipharm_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_unipharm_report_mv

CREATE MATERIALIZED VIEW kafka.fpc_unipharm_report_mv  TO stg.mart_fpc_unipharm_report AS 
SELECT * FROM kafka.fpc_unipharm_report;

