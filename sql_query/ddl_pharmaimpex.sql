--drop table stg.mart_fpc_pharmaimpex_report

create table stg.mart_fpc_pharmaimpex_report
(
	uuid_report text,
	pharmacy_name text,
	pharmacy_code text,
	city text, 
	region text,
    pharmacy_format text,
    product_name text,
    supplier text,
    doc_date text,
    doc_number text,
    purchase_quantity text,
    purchase_sum text,
    sale_quantity text,
    sale_sum_purchase_price text,
    stock_quantity text,
    expiration_date text,
	name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_pharmaimpex_report

create table kafka.fpc_pharmaimpex_report
(
	uuid_report text,
	pharmacy_name text,
	pharmacy_code text,
	city text, 
	region text,
    pharmacy_format text,
    product_name text,
    supplier text,
    doc_date text,
    doc_number text,
    purchase_quantity text,
    purchase_sum text,
    sale_quantity text,
    sale_sum_purchase_price text,
    stock_quantity text,
    expiration_date text,
	name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_pharmaimpex_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_pharmaimpex_report_mv

CREATE MATERIALIZED VIEW kafka.fpc_pharmaimpex_report_mv  TO stg.mart_fpc_pharmaimpex_report AS 
SELECT * FROM kafka.fpc_pharmaimpex_report;

