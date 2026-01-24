--drop table stg.mart_fpc_moya_apteka_nsk_report
create table stg.mart_fpc_moya_apteka_nsk_report
(
	uuid_report text,
	legal_entity text,
	pharmacy_inn text,
	product_name text, 
	supplier text,
	manufacturer_barcode text,
	purchase_quantity text,
    sale_quantity text,
    stock_quantity text,
    name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_moya_apteka_nsk_report

create table kafka.fpc_moya_apteka_nsk_report
(
	uuid_report text,
	legal_entity text,
	pharmacy_inn text,
	product_name text, 
	supplier text,
	manufacturer_barcode text,
	purchase_quantity text,
    sale_quantity text,
    stock_quantity text,
    name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_moya_apteka_nsk_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_moya_apteka_nsk_report_mv

CREATE MATERIALIZED VIEW kafka.fpc_moya_apteka_nsk_report_mv  TO stg.mart_fpc_moya_apteka_nsk_report AS 
SELECT * FROM kafka.fpc_moya_apteka_nsk_report;

