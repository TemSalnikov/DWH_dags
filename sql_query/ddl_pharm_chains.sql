
create table stg.dict_pharm_chains 
(
	inn bigint,
	legal_name text,
	brand text,
	uuid_report text,
	processed_dttm text,
	record_hash text
)
engine = ReplacingMergeTree()
order by (inn, legal_name, brand)


--drop table kafka.dict_pharm_chains 

create table kafka.dict_pharm_chains 
(
	inn bigint,
	legal_name text,
	brand text,
	uuid_report text,
	processed_dttm text,
	record_hash text
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafak2:19092, kafka3:19092',
    kafka_topic_list = 'dict_ptharm_chain_table_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_num_consumers = 1;



--drop VIEW kafka.dict_pharm_chains_mv 

CREATE MATERIALIZED VIEW kafka.dict_pharm_chains_mv  TO stg.dict_pharm_chains AS
SELECT * FROM kafka.dict_pharm_chains;

select * from stg.dict_pharm_chains 