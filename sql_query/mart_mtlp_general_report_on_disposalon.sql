drop table kafka.mdlp_general_report_on_disposalon on cluster cluster_2S_2R

create table kafka.mdlp_general_report_on_disposal on cluster cluster_2S_2R
(
	date_of_disposal text, 
	tin_to_the_issuer text, 
	the_name_of_the_issuer text, 
	code_of_the_subject_of_the_russian_federation text, 
	the_subject_of_the_russian_federation text, 
	settlement text, 
	district text, 
	tin_of_the_participant text, 
	name_of_the_participant text, 
	identifier_md_participant text, 
	address text, 
	mnn text, 
	trade_name text, 
	gtin text, 
	series text, 
	best_before_date text, 
	type_of_disposal text, 
	the_volume_of_diving_units text, 
	source_of_financing text, 
	data_update_date text, 
	type_of_export text, 
	completeness_of_disposal text, 
	type_report text, 
	date_to text, 
	create_dttm text, 
	deleted_flag bool, 
	uuid_report text
	)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafka2:19092, kafka3:19092',
    kafka_topic_list = 'mdlp_general_report_on_disposal',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_num_consumers = 1;


drop VIEW kafka.mdlp_general_report_on_disposalon_mv on cluster cluster_2S_2R 
	
CREATE MATERIALIZED VIEW kafka.mdlp_general_report_on_disposal_mv on cluster cluster_2S_2R 
TO stg.mart_mdlp_general_report_on_disposal AS
SELECT * FROM kafka.mdlp_general_report_on_disposal;


drop table stg.mart_mdlp_general_report_on_disposalon on cluster cluster_2S_2R

create table stg.mart_mdlp_general_report_on_disposal on cluster cluster_2S_2R
(
	date_of_disposal text, 
	tin_to_the_issuer text, 
	the_name_of_the_issuer text, 
	code_of_the_subject_of_the_russian_federation text, 
	the_subject_of_the_russian_federation text, 
	settlement text, 
	district text, 
	tin_of_the_participant text, 
	name_of_the_participant text, 
	identifier_md_participant text, 
	address text, 
	mnn text, 
	trade_name text, 
	gtin text, 
	series text, 
	best_before_date text, 
	type_of_disposal text, 
	the_volume_of_diving_units text, 
	source_of_financing text, 
	data_update_date text, 
	type_of_export text, 
	completeness_of_disposal text, 
	type_report text, 
	date_to text, 
	create_dttm text, 
	deleted_flag bool, 
	uuid_report text
)
engine = ReplacingMergeTree()
order by (uuid_report, gtin, tin_to_the_issuer, deleted_flag)

select * from cluster('cluster_2S_2R', 'stg', 'mart_mdlp_general_report_on_disposalon')


CREATE VIEW stg.v_sn_mart_mdlp_general_report_on_disposal AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
		tin_to_the_issuer,
		mnn,
		gtin,
		address,
		date_of_disposal,
        
        argMax(the_name_of_the_issuer, toDateTime(create_dttm)) AS the_name_of_the_issuer,
        argMax(code_of_the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS code_of_the_subject_of_the_russian_federation,
        argMax(the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS the_subject_of_the_russian_federation,
        argMax(settlement, toDateTime(create_dttm)) AS settlement,
        argMax(district, toDateTime(create_dttm)) AS district,
        argMax(tin_of_the_participant, toDateTime(create_dttm)) AS tin_of_the_participant,
        argMax(name_of_the_participant, toDateTime(create_dttm)) AS name_of_the_participant,
        argMax(identifier_md_participant, toDateTime(create_dttm)) AS identifier_md_participant,
        argMax(trade_name, toDateTime(create_dttm)) AS trade_name,
        argMax(serial_number, toDateTime(create_dttm)) AS serial_number,
        argMax(best_before_date, toDateTime(create_dttm)) AS best_before_date,
        argMax(type_of_disposal, toDateTime(create_dttm)) AS type_of_disposal,
        argMax(the_volume_of_diving_units, toDateTime(create_dttm)) AS the_volume_of_diving_units,
        argMax(source_of_financing, toDateTime(create_dttm)) AS source_of_financing,
        argMax(data_update_date, toDateTime(create_dttm)) AS data_update_date,
        argMax(type_of_export, toDateTime(create_dttm)) AS type_of_export,
        argMax(completeness_of_disposal, toDateTime(create_dttm)) AS completeness_of_disposal,
        argMax(type_report, toDateTime(create_dttm)) AS type_report,
        argMax(date_to, toDateTime(create_dttm)) AS date_to,
        argMax(uuid_report, toDateTime(create_dttm)) AS uuid_report,

        argMax(deleted_flag, toDateTime(create_dttm)) AS deleted_flag,
        max(toDateTime(create_dttm)) AS create_dttm_max

    FROM stg.mart_mdlp_general_report_on_disposal
    WHERE toDateTime(create_dttm)
          BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY tin_to_the_issuer,
		mnn,
		gtin,
		address,
		date_of_disposal
    HAVING deleted_flag = false
) t1;


CREATE VIEW stg.v_sv_mart_mdlp_general_report_on_disposal AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
		tin_to_the_issuer,
		mnn,
		gtin,
		address,
		date_of_disposal,

        argMax(the_name_of_the_issuer, toDateTime(create_dttm)) AS the_name_of_the_issuer,
        argMax(code_of_the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS code_of_the_subject_of_the_russian_federation,
        argMax(the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS the_subject_of_the_russian_federation,
        argMax(settlement, toDateTime(create_dttm)) AS settlement,
        argMax(district, toDateTime(create_dttm)) AS district,
        argMax(tin_of_the_participant, toDateTime(create_dttm)) AS tin_of_the_participant,
        argMax(name_of_the_participant, toDateTime(create_dttm)) AS name_of_the_participant,
        argMax(identifier_md_participant, toDateTime(create_dttm)) AS identifier_md_participant,
        argMax(trade_name, toDateTime(create_dttm)) AS trade_name,
        argMax(serial_number, toDateTime(create_dttm)) AS serial_number,
        argMax(best_before_date, toDateTime(create_dttm)) AS best_before_date,
        argMax(type_of_disposal, toDateTime(create_dttm)) AS type_of_disposal,
        argMax(the_volume_of_diving_units, toDateTime(create_dttm)) AS the_volume_of_diving_units,
        argMax(source_of_financing, toDateTime(create_dttm)) AS source_of_financing,
        argMax(data_update_date, toDateTime(create_dttm)) AS data_update_date,
        argMax(type_of_export, toDateTime(create_dttm)) AS type_of_export,
        argMax(completeness_of_disposal, toDateTime(create_dttm)) AS completeness_of_disposal,
        argMax(type_report, toDateTime(create_dttm)) AS type_report,
        argMax(date_to, toDateTime(create_dttm)) AS date_to,
        argMax(uuid_report, toDateTime(create_dttm)) AS uuid_report,

        argMax(deleted_flag, toDateTime(create_dttm)) AS deleted_flag,
        max(toDateTime(create_dttm)) AS create_dttm_max

    FROM stg.mart_mdlp_general_report_on_disposal
    WHERE toDateTime(create_dttm) <= {p_processed_dttm_user:DateTime}
    GROUP BY tin_to_the_issuer,
		mnn,
		gtin,
		address,
		date_of_disposal
    HAVING deleted_flag = false
) t1;


CREATE VIEW stg.v_iv_mart_mdlp_general_report_on_disposal AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
		tin_to_the_issuer,
		mnn,
		gtin,
		address,
		date_of_disposal,

        argMax(the_name_of_the_issuer, toDateTime(create_dttm)) AS the_name_of_the_issuer,
        argMax(code_of_the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS code_of_the_subject_of_the_russian_federation,
        argMax(the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS the_subject_of_the_russian_federation,
        argMax(settlement, toDateTime(create_dttm)) AS settlement,
        argMax(district, toDateTime(create_dttm)) AS district,
        argMax(tin_of_the_participant, toDateTime(create_dttm)) AS tin_of_the_participant,
        argMax(name_of_the_participant, toDateTime(create_dttm)) AS name_of_the_participant,
        argMax(identifier_md_participant, toDateTime(create_dttm)) AS identifier_md_participant,
        argMax(trade_name, toDateTime(create_dttm)) AS trade_name,
        argMax(serial_number, toDateTime(create_dttm)) AS serial_number,
        argMax(best_before_date, toDateTime(create_dttm)) AS best_before_date,
        argMax(type_of_disposal, toDateTime(create_dttm)) AS type_of_disposal,
        argMax(the_volume_of_diving_units, toDateTime(create_dttm)) AS the_volume_of_diving_units,
        argMax(source_of_financing, toDateTime(create_dttm)) AS source_of_financing,
        argMax(data_update_date, toDateTime(create_dttm)) AS data_update_date,
        argMax(type_of_export, toDateTime(create_dttm)) AS type_of_export,
        argMax(completeness_of_disposal, toDateTime(create_dttm)) AS completeness_of_disposal,
        argMax(type_report, toDateTime(create_dttm)) AS type_report,
        argMax(date_to, toDateTime(create_dttm)) AS date_to,
        argMax(uuid_report, toDateTime(create_dttm)) AS uuid_report,

        argMax(deleted_flag, toDateTime(create_dttm)) AS deleted_flag,
        max(toDateTime(create_dttm)) AS create_dttm_max

    FROM stg.mart_mdlp_general_report_on_disposal
    WHERE toDateTime(create_dttm)
          BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY tin_to_the_issuer,
		mnn,
		gtin,
		address,
		date_of_disposal
    HAVING deleted_flag = false
) t1;
