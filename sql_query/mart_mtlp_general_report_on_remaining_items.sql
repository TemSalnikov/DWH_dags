drop table kafka.mdlp_general_report_on_remaining_items

create table kafka.mdlp_general_report_on_remaining_items
(
	uuid_report text,
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
	remains_in_the_market_units text, 
	remains_before_the_input_in_th_units text, 
	general_residues_units text, 
	source_of_financing text, 
	data_update_date text, 
	remains_in_the_market_excluding_705_unitary_enterprise text, 
	shipped text, 
	type_report text, 
	date_to text, 
	create_dttm text, 
	deleted_flag bool
)
engine = Kafka()
SETTINGS
    kafka_broker_list = 'kafka1:19092, kafka2:19092, kafka3:19092',
    kafka_topic_list = 'mdlp_general_report_on_remaining_items',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_num_consumers = 1;

drop VIEW kafka.mdlp_general_report_on_remaining_items_mv 

CREATE MATERIALIZED VIEW kafka.mdlp_general_report_on_remaining_items_mv 
TO stg.mart_mdlp_general_report_on_remaining_items AS
SELECT * FROM kafka.mdlp_general_report_on_remaining_items;

DROP table stg.mart_mdlp_general_report_on_remaining_items

create table stg.mart_mdlp_general_report_on_remaining_items
(
	uuid_report text,
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
	remains_in_the_market_units text, 
	remains_before_the_input_in_th_units text, 
	general_residues_units text, 
	source_of_financing text, 
	data_update_date text, 
	remains_in_the_market_excluding_705_unitary_enterprise text, 
	shipped text, 
	type_report text, 
	date_to text, 
	create_dttm text,
	deleted_flag bool
)
engine = ReplacingMergeTree()
order by (uuid_report, gtin, tin_to_the_issuer, tin_of_the_participant, deleted_flag)

select tin_to_the_issuer, tin_of_the_participant, gtin, count (*) from cluster('cluster_2S_2R', 'stg', 'mart_mdlp_general_report_on_remaining_items')
group by tin_to_the_issuer, tin_of_the_participant, gtin

select * from cluster('cluster_2S_2R', 'stg', 'mart_mdlp_general_report_on_remaining_items')
where tin_to_the_issuer = '2226002532' and tin_of_the_participant = '7734722725' and gtin = '4603679000324'

CREATE VIEW stg.v_sn_mart_mdlp_general_report_on_remaining_items AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
        tin_to_the_issuer,
        mnn,
        gtin,
        address,
        date_to,

        argMax(the_name_of_the_issuer, toDateTime(create_dttm)) AS the_name_of_the_issuer,
        argMax(code_of_the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS code_of_the_subject_of_the_russian_federation,
        argMax(the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS the_subject_of_the_russian_federation,
        argMax(settlement, toDateTime(create_dttm)) AS settlement,
        argMax(district, toDateTime(create_dttm)) AS district,
        argMax(tin_of_the_participant, toDateTime(create_dttm)) AS tin_of_the_participant,
        argMax(name_of_the_participant, toDateTime(create_dttm)) AS name_of_the_participant,
        argMax(identifier_md_participant, toDateTime(create_dttm)) AS identifier_md_participant,
        argMax(trade_name, toDateTime(create_dttm)) AS trade_name,
        argMax(series, toDateTime(create_dttm)) AS series,
        argMax(best_before_date, toDateTime(create_dttm)) AS best_before_date,
        argMax(remains_in_the_market_units, toDateTime(create_dttm)) AS remains_in_the_market_units,
        argMax(remains_before_the_input_in_th_units, toDateTime(create_dttm)) AS remains_before_the_input_in_th_units,
        argMax(general_residues_units, toDateTime(create_dttm)) AS general_residues_units,
        argMax(source_of_financing, toDateTime(create_dttm)) AS source_of_financing,
        argMax(data_update_date, toDateTime(create_dttm)) AS data_update_date,
        argMax(remains_in_the_market_excluding_705_unitary_enterprise, toDateTime(create_dttm)) AS remains_in_the_market_excluding_705_unitary_enterprise,
        argMax(shipped, toDateTime(create_dttm)) AS shipped,
        argMax(type_report, toDateTime(create_dttm)) AS type_report,
        argMax(uuid_report, toDateTime(create_dttm)) AS uuid_report,

        argMax(deleted_flag, toDateTime(create_dttm)) AS deleted_flag,
        max(toDateTime(create_dttm)) AS create_dttm_max

    FROM stg.mart_mdlp_general_report_on_remaining_items
    WHERE toDateTime(create_dttm)
          BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY tin_to_the_issuer,
        mnn,
        gtin,
        address,
        date_to
    HAVING deleted_flag = false
) t1;


CREATE VIEW stg.v_sv_mart_mdlp_general_report_on_remaining_items AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
        tin_to_the_issuer,
        mnn,
        gtin,
        address,
        date_to,

        argMax(the_name_of_the_issuer, toDateTime(create_dttm)) AS the_name_of_the_issuer,
        argMax(code_of_the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS code_of_the_subject_of_the_russian_federation,
        argMax(the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS the_subject_of_the_russian_federation,
        argMax(settlement, toDateTime(create_dttm)) AS settlement,
        argMax(district, toDateTime(create_dttm)) AS district,
        argMax(tin_of_the_participant, toDateTime(create_dttm)) AS tin_of_the_participant,
        argMax(name_of_the_participant, toDateTime(create_dttm)) AS name_of_the_participant,
        argMax(identifier_md_participant, toDateTime(create_dttm)) AS identifier_md_participant,
        argMax(trade_name, toDateTime(create_dttm)) AS trade_name,
        argMax(series, toDateTime(create_dttm)) AS series,
        argMax(best_before_date, toDateTime(create_dttm)) AS best_before_date,
        argMax(remains_in_the_market_units, toDateTime(create_dttm)) AS remains_in_the_market_units,
        argMax(remains_before_the_input_in_th_units, toDateTime(create_dttm)) AS remains_before_the_input_in_th_units,
        argMax(general_residues_units, toDateTime(create_dttm)) AS general_residues_units,
        argMax(source_of_financing, toDateTime(create_dttm)) AS source_of_financing,
        argMax(data_update_date, toDateTime(create_dttm)) AS data_update_date,
        argMax(remains_in_the_market_excluding_705_unitary_enterprise, toDateTime(create_dttm)) AS remains_in_the_market_excluding_705_unitary_enterprise,
        argMax(shipped, toDateTime(create_dttm)) AS shipped,
        argMax(type_report, toDateTime(create_dttm)) AS type_report,
        argMax(uuid_report, toDateTime(create_dttm)) AS uuid_report,

        argMax(deleted_flag, toDateTime(create_dttm)) AS deleted_flag,
        max(toDateTime(create_dttm)) AS create_dttm_max

    FROM stg.mart_mdlp_general_report_on_remaining_items
    WHERE toDateTime(create_dttm) <= {p_processed_dttm_user:DateTime}
    GROUP BY tin_to_the_issuer,
        mnn,
        gtin,
        address,
        date_to
    HAVING deleted_flag = false
) t1;


CREATE VIEW stg.v_iv_mart_mdlp_general_report_on_remaining_items AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
        tin_to_the_issuer,
        mnn,
        gtin,
        address,
        date_to,

        argMax(the_name_of_the_issuer, toDateTime(create_dttm)) AS the_name_of_the_issuer,
        argMax(code_of_the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS code_of_the_subject_of_the_russian_federation,
        argMax(the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS the_subject_of_the_russian_federation,
        argMax(settlement, toDateTime(create_dttm)) AS settlement,
        argMax(district, toDateTime(create_dttm)) AS district,
        argMax(tin_of_the_participant, toDateTime(create_dttm)) AS tin_of_the_participant,
        argMax(name_of_the_participant, toDateTime(create_dttm)) AS name_of_the_participant,
        argMax(identifier_md_participant, toDateTime(create_dttm)) AS identifier_md_participant,
        argMax(trade_name, toDateTime(create_dttm)) AS trade_name,
        argMax(series, toDateTime(create_dttm)) AS series,
        argMax(best_before_date, toDateTime(create_dttm)) AS best_before_date,
        argMax(remains_in_the_market_units, toDateTime(create_dttm)) AS remains_in_the_market_units,
        argMax(remains_before_the_input_in_th_units, toDateTime(create_dttm)) AS remains_before_the_input_in_th_units,
        argMax(general_residues_units, toDateTime(create_dttm)) AS general_residues_units,
        argMax(source_of_financing, toDateTime(create_dttm)) AS source_of_financing,
        argMax(data_update_date, toDateTime(create_dttm)) AS data_update_date,
        argMax(remains_in_the_market_excluding_705_unitary_enterprise, toDateTime(create_dttm)) AS remains_in_the_market_excluding_705_unitary_enterprise,
        argMax(shipped, toDateTime(create_dttm)) AS shipped,
        argMax(type_report, toDateTime(create_dttm)) AS type_report,
        argMax(uuid_report, toDateTime(create_dttm)) AS uuid_report,

        argMax(deleted_flag, toDateTime(create_dttm)) AS deleted_flag,
        max(toDateTime(create_dttm)) AS create_dttm_max

    FROM stg.mart_mdlp_general_report_on_remaining_items
    WHERE toDateTime(create_dttm)
          BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY tin_to_the_issuer,
        mnn,
        gtin,
        address,
        date_to
    HAVING deleted_flag = false
) t1;