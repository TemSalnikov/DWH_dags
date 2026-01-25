--drop table stg.mart_fpc_vita_tomsk_report
create table stg.mart_fpc_vita_tomsk_report
(
    uuid_report text,
    ekn text,
    product text,
    quantity text,
    purchase_amount_with_vat text,
    sales_quantity text,
    balance_end_period_quantity text,
    current_balance_quantity text,
    legal_entity_inn text,
    legal_entity text,
    settlement text,
    pharmacy_address text,
    supplier_inn text,
    supplier text,
    factory_barcode text,
    receipt_date text,
    invoice_number text,
    expiration_date text,
    product_amount text,
    point_code text,
    ofd_name text,
    fn text,
    fd text,
    fpd text,
    receipt_amount text,
    kkt text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_vita_tomsk_report

create table kafka.fpc_vita_tomsk_report
(
    uuid_report text,
    ekn text,
    product text,
    quantity text,
    purchase_amount_with_vat text,
    sales_quantity text,
    balance_end_period_quantity text,
    current_balance_quantity text,
    legal_entity_inn text,
    legal_entity text,
    settlement text,
    pharmacy_address text,
    supplier_inn text,
    supplier text,
    factory_barcode text,
    receipt_date text,
    invoice_number text,
    expiration_date text,
    product_amount text,
    point_code text,
    ofd_name text,
    fn text,
    fd text,
    fpd text,
    receipt_amount text,
    kkt text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_vita_tomsk_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_vita_tomsk_report_mv

CREATE MATERIALIZED VIEW kafka.fpc_vita_tomsk_report_mv  TO stg.mart_fpc_vita_tomsk_report AS 
SELECT * FROM kafka.fpc_vita_tomsk_report;

