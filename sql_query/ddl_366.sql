create database stg

create database dds

create database bdm

create database kafka

SELECT * FROM system.clusters WHERE cluster = 'cluster_2S_2R'

--drop table stg.mart_fpc_366_drugstor




create table stg.mart_fpc_366_drugstor
(
	name text,
	legal_name text,
	inn bigint,
	id bigint,
	address text,
	hash_drugstore text
)
engine = ReplacingMergeTree()
order by (hash_drugstore)

alter TABLE stg.mart_fpc_366_drugstor
add column hash_drugstore_addr text;

alter TABLE stg.mart_fpc_366_drugstor
MODIFY ORDER BY (hash_drugstore_addr)

drop table kafka.fpc_366_drugstor

create table kafka.fpc_366_drugstor
(
	name text,
	legal_name text,
	inn bigint,
	id bigint,
	address text,
	hash_drugstore_addr text,
	hash_drugstore text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = '192.168.14.235:9091, 192.168.14.235:9092, 192.168.14.235:9093',
    kafka_topic_list = 'fpc_366_table_drugstor',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_num_consumers = 1;

alter TABLE kafka.fpc_366_drugstor
add column hash_drugstore_addr text;



drop VIEW kafka.fpc_366_drugstor_mv

CREATE MATERIALIZED VIEW kafka.fpc_366_drugstor_mv TO stg.mart_fpc_366_drugstor AS
SELECT * FROM kafka.fpc_366_drugstor;

select * from stg.mart_fpc_366_drugstor

--
drop table stg.mart_fpc_366_product

create table stg.mart_fpc_366_product 
(
	name text,
	id bigint,
	manufacturer text,
	hash_product text
)
engine = ReplacingMergeTree()
order by (hash_product)

truncate table if exists stg.mart_fpc_366_report

drop table kafka.fpc_366_product

create table kafka.fpc_366_product
(
	name text,
	id bigint,
	manufacturer text,
	hash_product text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = '192.168.14.235:9091, 192.168.14.235:9092, 192.168.14.235:9093',
    kafka_topic_list = 'fpc_366_table_product',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_num_consumers = 1;

drop VIEW kafka.fpc_366_product_mv

CREATE MATERIALIZED VIEW kafka.fpc_366_product_mv TO stg.mart_fpc_366_product AS
SELECT * FROM kafka.fpc_366_product;

--
drop table stg.mart_fpc_366_supplier

truncate table stg.mart_fpc_366_report 

create table stg.mart_fpc_366_supplier
(
	name text,
	legal_name text,
	inn bigint,
	id bigint,
	address text,
	hash_supplier text
)
engine = ReplacingMergeTree()
order by (hash_suplier)

drop table kafka.fpc_366_supplier

create table kafka.fpc_366_supplier
(
	name text,
	legal_name text,
	inn bigint,
	id bigint,
	address text,
	hash_supplier text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = '192.168.14.235:9091, 192.168.14.235:9092, 192.168.14.235:9093',
    kafka_topic_list = 'fpc_366_table_suplier',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_num_consumers = 1;

drop VIEW stg.fpc_366_suplier_mv

CREATE MATERIALIZED VIEW kafka.fpc_366_supplier_mv TO stg.mart_fpc_366_supplier AS
SELECT * FROM kafka.fpc_366_supplier;

--
drop table stg.mart_fpc_366_report

create table stg.mart_fpc_366_report
(
	uuid_report text,
	period text,
	quantity text,
	total_cost text,
	hash_drugstore text,
	hash_supplier text,
	hash_product text,
	processed_dttm text
)
engine = ReplacingMergeTree()
order by (uuid_report)

alter TABLE stg.mart_fpc_366_report
add column hash_drugstore_addr text;

drop table kafka.fpc_366_report

create table kafka.fpc_366_report
(
	uuid_report text,
	name_report text,
	name_pharm_chain text,
	period text,
	quantity text,
	total_cost text,
	hash_drugstore text,
	hash_drugstore_addr text,
	hash_supplier text,
	hash_product text,
	processed_dttm text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = '192.168.14.235:9091, 192.168.14.235:9092, 192.168.14.235:9093',
    kafka_topic_list = 'fpc_366_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_max_block_size = 10485760,
    kafka_poll_max_batch_size = 10485760,
    kafka_handle_error_mode = 'stream',
    kafka_num_consumers = 1;

alter TABLE kafka.fpc_366_report
add column hash_drugstore_addr text;

drop VIEW kafka.fpc_366_report_mv

CREATE MATERIALIZED VIEW kafka.fpc_366_report_mv  TO stg.mart_fpc_366_report AS
SELECT * FROM kafka.fpc_366_report;

select hash_drugstore_addr, count(*) from cluster('cluster_2S_2R', 'stg', 'mart_fpc_366_drugstor')
group by hash_drugstore_addr
having count(*) > 1

select * from cluster('cluster_2S_2R', 'stg', 'mart_fpc_366_supplier' ) 

select * from cluster('cluster_2S_2R', 'stg', 'mart_fpc_366_product')

select * from cluster('cluster_2S_2R', 'stg', 'mart_fpc_366_report')  
where hash_supplier != ''

truncate table stg.mart_fpc_366_report

truncate table stg.mart_fpc_366_drugstor
