--drop table stg.mart_fpc_mfo_report
    
create table stg.mart_fpc_mfo_report
(
	uuid_report text,
	period_date text,
	partner_code text,
	client_name text, 
	legal_entity text,
	region text,
	city text,
    shipping_address text,
    full_address text,
    pharmacy_inn text,
    mfo_supplier text,
    counted_db text,
    customer text,
    product_name text,
    product_group text,
    distribution text,
    purchase_quantity text,
    purchase_amount text,
    sale_quantity text,
    stock_quantity text,
    invoice_number text,
    invoice_date text,
    name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_mfo_report

create table kafka.fpc_mfo_report
(
	uuid_report text,
	period_date text,
	partner_code text,
	client_name text, 
	legal_entity text,
	region text,
	city text,
    shipping_address text,
    full_address text,
    pharmacy_inn text,
    mfo_supplier text,
    counted_db text,
    customer text,
    product_name text,
    product_group text,
    distribution text,
    purchase_quantity text,
    purchase_amount text,
    sale_quantity text,
    stock_quantity text,
    invoice_number text,
    invoice_date text,
    name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_mfo_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_mfo_report_mv

CREATE MATERIALIZED VIEW kafka.fpc_mfo_report_mv  TO stg.mart_fpc_mfo_report AS 
SELECT * FROM kafka.fpc_mfo_report;

