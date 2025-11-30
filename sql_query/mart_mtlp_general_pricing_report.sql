drop table kafka.mdlp_general_pricing_report on cluster cluster_2S_2R

create table kafka.mdlp_general_pricing_report on cluster cluster_2S_2R
(
	
	tin_to_the_issuer text, 
	the_name_of_the_issuer text, 
	code_of_the_subject_of_the_russian_federation text, 
	the_subject_of_the_russian_federation text,
	tin_of_the_participant text, 
	name_of_the_participant text, 
	mnn text, 
	trade_name text, 
	gtin text, 
	the_number_of_points_of_sales text, 
	sales_volume_units text, 
	meduvented_price_rub text, 
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
    kafka_topic_list = 'mdlp_general_pricing_report',
    kafka_group_name = 'clickhouse',
    kafka_format = 'JSONColumns',
    kafka_num_consumers = 1;



drop VIEW kafka.mdlp_general_pricing_report_mv on cluster cluster_2S_2R 
	
CREATE MATERIALIZED VIEW kafka.mdlp_general_pricing_report_mv on cluster cluster_2S_2R 
TO stg.mart_mdlp_general_pricing_report AS
SELECT * FROM kafka.mdlp_general_pricing_report;


drop table stg.mart_mdlp_general_pricing_report on cluster cluster_2S_2R

create table stg.mart_mdlp_general_pricing_report on cluster cluster_2S_2R
(

	tin_to_the_issuer text, 
	the_name_of_the_issuer text, 
	code_of_the_subject_of_the_russian_federation text, 
	the_subject_of_the_russian_federation text,
	tin_of_the_participant text, 
	name_of_the_participant text, 
	mnn text, 
	trade_name text, 
	gtin text, 
	the_number_of_points_of_sales text, 
	sales_volume_units text, 
	meduvented_price_rub text, 
	source_of_financing text, 
	data_update_date text, 
	type_report text, 
	date_to text, 
	create_dttm text, 
	deleted_flag bool, 
	uuid_report text
)
engine = ReplacingMergeTree()
order by (uuid_report, gtin, tin_to_the_issuer, tin_of_the_participant, deleted_flag)

select * from cluster('cluster_2S_2R', 'stg', 'mart_mdlp_general_pricing_report')


CREATE VIEW stg.v_sn_mart_mdlp_general_pricing_report AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
        tin_to_the_issuer,
        mnn,
        gtin,
        tin_of_the_participant,
        
        argMax(the_name_of_the_issuer, toDateTime(create_dttm)) AS the_name_of_the_issuer,
        argMax(code_of_the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS code_of_the_subject_of_the_russian_federation,
        argMax(the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS the_subject_of_the_russian_federation,
        argMax(name_of_the_participant, toDateTime(create_dttm)) AS name_of_the_participant,
        argMax(trade_name, toDateTime(create_dttm)) AS trade_name,
        argMax(the_number_of_points_of_sales, toDateTime(create_dttm)) AS the_number_of_points_of_sales,
        argMax(sales_volume_units, toDateTime(create_dttm)) AS sales_volume_units,
        argMax(meduvented_price_rub, toDateTime(create_dttm)) AS meduvented_price_rub,
        argMax(source_of_financing, toDateTime(create_dttm)) AS source_of_financing,
        argMax(data_update_date, toDateTime(create_dttm)) AS data_update_date,
        argMax(type_report, toDateTime(create_dttm)) AS type_report,
        argMax(date_to, toDateTime(create_dttm)) AS date_to,
        argMax(uuid_report, toDateTime(create_dttm)) AS uuid_report,

        argMax(deleted_flag, toDateTime(create_dttm)) AS deleted_flag,
        max(toDateTime(create_dttm)) AS create_dttm_max

    FROM stg.mart_mdlp_general_pricing_report
    WHERE toDateTime(create_dttm)
          BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY tin_to_the_issuer,
        mnn,
        gtin,
        tin_of_the_participant
    HAVING deleted_flag = false
) t1;


CREATE VIEW stg.v_sv_mart_mdlp_general_pricing_report AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
        tin_to_the_issuer,
        mnn,
        gtin,
        tin_of_the_participant,

        argMax(the_name_of_the_issuer, toDateTime(create_dttm)) AS the_name_of_the_issuer,
        argMax(code_of_the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS code_of_the_subject_of_the_russian_federation,
        argMax(the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS the_subject_of_the_russian_federation,
        argMax(name_of_the_participant, toDateTime(create_dttm)) AS name_of_the_participant,
        argMax(trade_name, toDateTime(create_dttm)) AS trade_name,
        argMax(the_number_of_points_of_sales, toDateTime(create_dttm)) AS the_number_of_points_of_sales,
        argMax(sales_volume_units, toDateTime(create_dttm)) AS sales_volume_units,
        argMax(meduvented_price_rub, toDateTime(create_dttm)) AS meduvented_price_rub,
        argMax(source_of_financing, toDateTime(create_dttm)) AS source_of_financing,
        argMax(data_update_date, toDateTime(create_dttm)) AS data_update_date,
        argMax(type_report, toDateTime(create_dttm)) AS type_report,
        argMax(date_to, toDateTime(create_dttm)) AS date_to,
        argMax(uuid_report, toDateTime(create_dttm)) AS uuid_report,

        argMax(deleted_flag, toDateTime(create_dttm)) AS deleted_flag,
        max(toDateTime(create_dttm)) AS create_dttm_max

    FROM stg.mart_mdlp_general_pricing_report
    WHERE toDateTime(create_dttm) <= {p_processed_dttm_user:DateTime}
    GROUP BY tin_to_the_issuer,
        mnn,
        gtin,
        tin_of_the_participant
    HAVING deleted_flag = false
) t1;


CREATE VIEW stg.v_iv_mart_mdlp_general_pricing_report AS
SELECT t1.* EXCEPT (create_dttm_max), create_dttm_max AS create_dttm
FROM (
    SELECT
        tin_to_the_issuer,
        mnn,
        gtin,
        tin_of_the_participant,

        argMax(the_name_of_the_issuer, toDateTime(create_dttm)) AS the_name_of_the_issuer,
        argMax(code_of_the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS code_of_the_subject_of_the_russian_federation,
        argMax(the_subject_of_the_russian_federation, toDateTime(create_dttm)) AS the_subject_of_the_russian_federation,
        argMax(name_of_the_participant, toDateTime(create_dttm)) AS name_of_the_participant,
        argMax(trade_name, toDateTime(create_dttm)) AS trade_name,
        argMax(the_number_of_points_of_sales, toDateTime(create_dttm)) AS the_number_of_points_of_sales,
        argMax(sales_volume_units, toDateTime(create_dttm)) AS sales_volume_units,
        argMax(meduvented_price_rub, toDateTime(create_dttm)) AS meduvented_price_rub,
        argMax(source_of_financing, toDateTime(create_dttm)) AS source_of_financing,
        argMax(data_update_date, toDateTime(create_dttm)) AS data_update_date,
        argMax(type_report, toDateTime(create_dttm)) AS type_report,
        argMax(date_to, toDateTime(create_dttm)) AS date_to,
        argMax(uuid_report, toDateTime(create_dttm)) AS uuid_report,

        argMax(deleted_flag, toDateTime(create_dttm)) AS deleted_flag,
        max(toDateTime(create_dttm)) AS create_dttm_max

    FROM stg.mart_mdlp_general_pricing_report
    WHERE toDateTime(create_dttm)
          BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY tin_to_the_issuer,
        mnn,
        gtin,
        tin_of_the_participant
    HAVING deleted_flag = false
) t1;

