--drop table stg.mart_fpc_iris_report

create table stg.mart_fpc_iris_report
(
    uuid_report text,
    contract text,
    chain_name text,
    brand text,
    iris_code text,
    iris_name text,
    product_name text,
    portfolio_1 text,
    portfolio_2 text,
    portfolio_3 text,
    historical_code text,
    region text,
    city text,
    address text,
    legal_entity text,
    inn text,
    supplier_code text,
    supplier text,
    agreed_distributor text,
    so_sales_flag text,
    purchase_quantity text,
    sale_quantity text,
    remains_quantity text,
    cip_price text,
    cip_sum text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text

)
engine = ReplacingMergeTree()
order by (uuid_report)


--drop table kafka.fpc_iris_report

create table kafka.fpc_iris_report
(
    uuid_report text,
    contract text,
    chain_name text,
    brand text,
    iris_code text,
    iris_name text,
    product_name text,
    portfolio_1 text,
    portfolio_2 text,
    portfolio_3 text,
    historical_code text,
    region text,
    city text,
    address text,
    legal_entity text,
    inn text,
    supplier_code text,
    supplier text,
    agreed_distributor text,
    so_sales_flag text,
    purchase_quantity text,
    sale_quantity text,
    remains_quantity text,
    cip_price text,
    cip_sum text,
    name_report text,
    name_pharm_chain text,
    start_date text,
    end_date text,
    processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'fpc_iris_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;


--drop VIEW kafka.fpc_iris_report_mv

CREATE MATERIALIZED VIEW kafka.fpc_iris_report_mv  TO stg.mart_fpc_iris_report AS 
SELECT * FROM kafka.fpc_iris_report;

