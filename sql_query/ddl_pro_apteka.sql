--drop table stg.mart_fpc_pro_apteka_report on cluster cluster_2S_2R

create table stg.mart_fpc_pro_apteka_report on cluster cluster_2S_2R
(
	uuid_report text,
	inn text,
	contractor_name text,
	pharmacy_name text, 
	pharmacy_address text,
	product_name text,
	pharmacy_id text,
    commercial_group text,
    product_id text,
    distributor_name text,
    purchase_quantity text,
    purchase_amount text,
    sale_quantity text,
    sale_amount text,
    region text,
    pharmacy_type text,
    matrix_category text,
    purchase_volume text,
    contractor_group text,
    ekg text,
    pharmacy_chain text,
    actual_contractor text,
    product_code text,
    manufacturer text,
    period text,
    start_period_stock text,
    end_period_stock text,
	name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_pro_apteka_report on cluster cluster_2S_2R

create table kafka.fpc_pro_apteka_report on cluster cluster_2S_2R
(
	uuid_report text,
	inn text,
	contractor_name text,
	pharmacy_name text, 
	pharmacy_address text,
	product_name text,
	pharmacy_id text,
    commercial_group text,
    product_id text,
    distributor_name text,
    purchase_quantity text,
    purchase_amount text,
    sale_quantity text,
    sale_amount text,
    region text,
    pharmacy_type text,
    matrix_category text,
    purchase_volume text,
    contractor_group text,
    ekg text,
    pharmacy_chain text,
    actual_contractor text,
    product_code text,
    manufacturer text,
    period text,
    start_period_stock text,
    end_period_stock text,
	name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_pro_apteka_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_pro_apteka_report_mv on cluster cluster_2S_2R

CREATE MATERIALIZED VIEW kafka.fpc_pro_apteka_report_mv  on cluster cluster_2S_2R TO stg.mart_fpc_pro_apteka_report AS 
SELECT * FROM kafka.fpc_pro_apteka_report;

