--drop table stg.mart_fpc_nevis_report on cluster cluster_2S_2R

create table stg.mart_fpc_nevis_report on cluster cluster_2S_2R
(
	uuid_report text,
	product_code text,
	product_name text,
	branch_inn text, 
	legal_entity text,
	Pharmacy_name text,
	address text,
    legal_entity_inn text,
    supplier text,
    quantity text,
    order_date text,
    doc_date text,
    doc_number text,
    document text,
    sum_no_vat text,
    product_total_sum text,
    name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_nevis_report on cluster cluster_2S_2R

create table kafka.fpc_nevis_report on cluster cluster_2S_2R
(
	uuid_report text,
	product_code text,
	product_name text,
	branch_inn text, 
	legal_entity text,
	Pharmacy_name text,
	address text,
    legal_entity_inn text,
    supplier text,
    quantity text,
    order_date text,
    doc_date text,
    doc_number text,
    document text,
    sum_no_vat text,
    product_total_sum text,
    name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_nevis_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_nevis_report_mv on cluster cluster_2S_2R

CREATE MATERIALIZED VIEW kafka.fpc_nevis_report_mv  on cluster cluster_2S_2R TO stg.mart_fpc_nevis_report AS 
SELECT * FROM kafka.fpc_nevis_report;

