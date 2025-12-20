--drop table stg.mart_fpc_social_pharmacy_report on cluster cluster_2S_2R

create table stg.mart_fpc_social_pharmacy_report on cluster cluster_2S_2R
(
	uuid_report text,
	subcontract_name text,
	product_name text,
    marketing_name text,
    contractor_short_name text,
    pharmacy_short_name text,
    pharmacy_address text,
    pharmacy_inn text,
    head_organization text,
    bonus_condition text,
    purchase_quantity text,
    purchase_sip text,
    purchase_sum_no_vat text,
    purchase_sum_vat text,
    sale_quantity text,
    sale_sip text,
    sale_sum_no_vat text,
    sale_sum_vat text,
    batch_series text,
    batch_invoice_date text,
    batch_expiration_date text,
    supplier text,
    price_sip text,
    stock_quantity text,
    stock_sip text,
    stock_sum_supplier text,
    illiquid_quantity text,
    illiquid_sum_supplier text,
	name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_social_pharmacy_report on cluster cluster_2S_2R

create table kafka.fpc_social_pharmacy_report on cluster cluster_2S_2R
(
	uuid_report text,
	subcontract_name text,
	product_name text,
    marketing_name text,
    contractor_short_name text,
    pharmacy_short_name text,
    pharmacy_address text,
    pharmacy_inn text,
    head_organization text,
    bonus_condition text,
    purchase_quantity text,
    purchase_sip text,
    purchase_sum_no_vat text,
    purchase_sum_vat text,
    sale_quantity text,
    sale_sip text,
    sale_sum_no_vat text,
    sale_sum_vat text,
    batch_series text,
    batch_invoice_date text,
    batch_expiration_date text,
    supplier text,
    price_sip text,
    stock_quantity text,
    stock_sip text,
    stock_sum_supplier text,
    illiquid_quantity text,
    illiquid_sum_supplier text,
	name_report text,
	name_pharm_chain text,
	start_date text,
	end_date text,
	processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_social_pharmacy_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_social_pharmacy_report_mv on cluster cluster_2S_2R

CREATE MATERIALIZED VIEW kafka.fpc_social_pharmacy_report_mv  on cluster cluster_2S_2R TO stg.mart_fpc_social_pharmacy_report AS 
SELECT * FROM kafka.fpc_social_pharmacy_report;

