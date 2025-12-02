drop table kafka.mdlp_general_report_on_movement on cluster cluster_2S_2R

create table kafka.mdlp_general_report_on_movement on cluster cluster_2S_2R
(
	
	the_date_of_the_operation text, 
	tin_to_the_issuer text, 
	the_name_of_the_issuer text, 
	mnn text, 
	trade_name text, 
	gtin text, 
	series text, 
	operation_number text, 
	tin_of_the_sender text, 
	name_of_the_sender text, 
	identifier_md_of_the_sender text, 
	tin_of_the_recipient text, 
	name_of_the_recipient text, 
	identifier_md_of_the_recipient text, 
	quantity_units text, 
	source_of_financing text, 
	data_update_date text, 
	type_report text, 
	date_to text, 
	create_dttm text, 
	deleted_flag bool, 
	uuid_report text
	)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafka2:19092, kafka3:19092',
    kafka_topic_list = 'mdlp_general_report_on_movement',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_num_consumers = 1;

alter table kafka.mdlp_general_report_on_movement on cluster cluster_2S_2R 
MODIFY column deleted_flag bool;
	
drop VIEW kafka.mdlp_general_report_on_movement_mv on cluster cluster_2S_2R 
	
CREATE MATERIALIZED VIEW kafka.mdlp_general_report_on_movement_mv on cluster cluster_2S_2R 
TO stg.mart_mdlp_general_report_on_movement AS
SELECT * FROM kafka.mdlp_general_report_on_movement;


drop table stg.mart_mdlp_general_report_on_movement on cluster cluster_2S_2R

create table stg.mart_mdlp_general_report_on_movement on cluster cluster_2S_2R
(
	the_date_of_the_operation text, 
	tin_to_the_issuer text, 
	the_name_of_the_issuer text, 
	mnn text, 
	trade_name text, 
	gtin text, 
	series text, 
	operation_number text, 
	tin_of_the_sender text, 
	name_of_the_sender text, 
	identifier_md_of_the_sender text, 
	tin_of_the_recipient text, 
	name_of_the_recipient text, 
	identifier_md_of_the_recipient text, 
	quantity_units text, 
	source_of_financing text, 
	data_update_date text, 
	type_report text, 
	date_to text, 
	create_dttm text, 
	deleted_flag bool, 
	uuid_report text
)
engine = ReplacingMergeTree()
order by (uuid_report, gtin, tin_to_the_issuer, deleted_flag)

alter table stg.mart_mdlp_general_report_on_movement on cluster cluster_2S_2R 
MODIFY column deleted_flag bool;

select uuid_report, count(*) from cluster('cluster_2S_2R', 'stg', 'mart_mdlp_general_report_on_movement')
group by uuid_report


CREATE VIEW stg.v_sn_mart_mdlp_general_report_on_movement AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
        the_date_of_the_operation,
        tin_to_the_issuer,
		mnn,
		gtin,

        argMax(the_name_of_the_issuer, toDateTime64(create_dttm, 0)) AS the_name_of_the_issuer,
        argMax(trade_name, toDateTime64(create_dttm, 0)) AS trade_name,
        argMax(series, toDateTime64(create_dttm, 0)) AS series,
        argMax(operation_number, toDateTime64(create_dttm, 0)) AS operation_number,
        argMax(tin_of_the_sender, toDateTime64(create_dttm, 0)) AS tin_of_the_sender,
        argMax(name_of_the_sender, toDateTime64(create_dttm, 0)) AS name_of_the_sender,
        argMax(identifier_md_of_the_sender, toDateTime64(create_dttm, 0)) AS identifier_md_of_the_sender,
        argMax(tin_of_the_recipient, toDateTime64(create_dttm, 0)) AS tin_of_the_recipient,
        argMax(name_of_the_recipient, toDateTime64(create_dttm, 0)) AS name_of_the_recipient,
        argMax(identifier_md_of_the_recipient, toDateTime64(create_dttm, 0)) AS identifier_md_of_the_recipient,
        argMax(quantity_units, toDateTime64(create_dttm, 0)) AS quantity_units,
        argMax(source_of_financing, toDateTime64(create_dttm, 0)) AS source_of_financing,
        argMax(data_update_date, toDateTime64(create_dttm, 0)) AS data_update_date,
        argMax(type_report, toDateTime64(create_dttm, 0)) AS type_report,
        argMax(date_to, toDateTime64(create_dttm, 0)) AS date_to,
        argMax(uuid_report, toDateTime64(create_dttm, 0)) AS uuid_report,

        -- технические поля
        argMax(deleted_flag, toDateTime64(create_dttm, 0)) AS deleted_flag,
        max(toDateTime64(create_dttm, 0)) AS create_dttm_max

    FROM stg.mart_mdlp_general_report_on_movement
    WHERE toDateTime64(create_dttm, 0) BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY gtin, tin_to_the_issuer, mnn, the_date_of_the_operation
    HAVING deleted_flag = false
) t1;

CREATE VIEW IF NOT EXISTS stg.v_sv_mart_mdlp_general_report_on_movement AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
		the_date_of_the_operation,
        tin_to_the_issuer,
		mnn,
		gtin,

        argMax(the_name_of_the_issuer, toDateTime64(create_dttm, 0)) AS the_name_of_the_issuer,
        argMax(trade_name, toDateTime64(create_dttm, 0)) AS trade_name,
        argMax(series, toDateTime64(create_dttm, 0)) AS series,
        argMax(operation_number, toDateTime64(create_dttm, 0)) AS operation_number,
        argMax(tin_of_the_sender, toDateTime64(create_dttm, 0)) AS tin_of_the_sender,
        argMax(name_of_the_sender, toDateTime64(create_dttm, 0)) AS name_of_the_sender,
        argMax(identifier_md_of_the_sender, toDateTime64(create_dttm, 0)) AS identifier_md_of_the_sender,
        argMax(tin_of_the_recipient, toDateTime64(create_dttm, 0)) AS tin_of_the_recipient,
        argMax(name_of_the_recipient, toDateTime64(create_dttm, 0)) AS name_of_the_recipient,
        argMax(identifier_md_of_the_recipient, toDateTime64(create_dttm, 0)) AS identifier_md_of_the_recipient,
        argMax(quantity_units, toDateTime64(create_dttm, 0)) AS quantity_units,
        argMax(source_of_financing, toDateTime64(create_dttm, 0)) AS source_of_financing,
        argMax(data_update_date, toDateTime64(create_dttm, 0)) AS data_update_date,
        argMax(type_report, toDateTime64(create_dttm, 0)) AS type_report,
        argMax(date_to, toDateTime64(create_dttm, 0)) AS date_to,
        argMax(uuid_report, toDateTime64(create_dttm, 0)) AS uuid_report,

        -- технические поля
        argMax(deleted_flag, toDateTime64(create_dttm, 0)) AS deleted_flag,
        max(toDateTime64(create_dttm, 0)) AS create_dttm_max

    FROM stg.mart_mdlp_general_report_on_movement
    WHERE toDateTime64(create_dttm, 0) <= {p_processed_dttm_user:DateTime}
    GROUP BY gtin, tin_to_the_issuer, mnn, the_date_of_the_operation
    HAVING deleted_flag = false
) t1;


CREATE VIEW IF NOT EXISTS stg.v_iv_mart_mdlp_general_report_on_movement AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
		the_date_of_the_operation,
        tin_to_the_issuer,
		mnn,
		gtin,

        argMax(the_name_of_the_issuer, toDateTime64(create_dttm, 0)) AS the_name_of_the_issuer,
        argMax(trade_name, toDateTime64(create_dttm, 0)) AS trade_name,
        argMax(series, toDateTime64(create_dttm, 0)) AS series,
        argMax(operation_number, toDateTime64(create_dttm, 0)) AS operation_number,
        argMax(tin_of_the_sender, toDateTime64(create_dttm, 0)) AS tin_of_the_sender,
        argMax(name_of_the_sender, toDateTime64(create_dttm, 0)) AS name_of_the_sender,
        argMax(identifier_md_of_the_sender, toDateTime64(create_dttm, 0)) AS identifier_md_of_the_sender,
        argMax(tin_of_the_recipient, toDateTime64(create_dttm, 0)) AS tin_of_the_recipient,
        argMax(name_of_the_recipient, toDateTime64(create_dttm, 0)) AS name_of_the_recipient,
        argMax(identifier_md_of_the_recipient, toDateTime64(create_dttm, 0)) AS identifier_md_of_the_recipient,
        argMax(quantity_units, toDateTime64(create_dttm, 0)) AS quantity_units,
        argMax(source_of_financing, toDateTime64(create_dttm, 0)) AS source_of_financing,
        argMax(data_update_date, toDateTime64(create_dttm, 0)) AS data_update_date,
        argMax(type_report, toDateTime64(create_dttm, 0)) AS type_report,
        argMax(date_to, toDateTime64(create_dttm, 0)) AS date_to,
        argMax(uuid_report, toDateTime64(create_dttm, 0)) AS uuid_report,

        -- технические поля
        argMax(deleted_flag, toDateTime64(create_dttm, 0)) AS deleted_flag,
        max(toDateTime64(create_dttm, 0)) AS create_dttm_max

    FROM stg.mart_mdlp_general_report_on_movement
    WHERE toDateTime64(create_dttm, 0) BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY gtin, tin_to_the_issuer, mnn, the_date_of_the_operation
    HAVING deleted_flag = false
) t1;