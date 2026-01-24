--drop table stg.mart_fpc_garmoniya_zdorovya_report
create table stg.mart_fpc_garmoniya_zdorovya_report
(
	uuid_report text,
	distributor text,
	distributor_inn text,
	internet_order text,
	subdivision text,
	address text,
    legal_entity text,
    legal_entity_inn text,
    product text,
    product_code text,
    quantity text,
    sum_purchase text,
    sum_manufacturer text,
    sum_vat text,
    doc_date text,
    doc_number text,
    sales_channel text,
    contract_group text,
    subdivision_name text,
    expiration_date text,
	name_pharm_chain text,
    name_report text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_garmoniya_zdorovya_report

create table kafka.fpc_garmoniya_zdorovya_report
(
	uuid_report text,
	distributor text,
	distributor_inn text,
	internet_order text,
	subdivision text,
	address text,
    legal_entity text,
    legal_entity_inn text,
    product text,
    product_code text,
    quantity text,
    sum_purchase text,
    sum_manufacturer text,
    sum_vat text,
    doc_date text,
    doc_number text,
    sales_channel text,
    contract_group text,
    subdivision_name text,
    expiration_date text,
	name_pharm_chain text,
    name_report text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_garmoniya_zdorovya_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_garmoniya_zdorovya_report_mv

CREATE MATERIALIZED VIEW kafka.fpc_garmoniya_zdorovya_report_mv  TO stg.mart_fpc_garmoniya_zdorovya_report AS 
SELECT * FROM kafka.fpc_garmoniya_zdorovya_report;

