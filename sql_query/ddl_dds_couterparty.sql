-- CREATE database dds ON CLUSTER cluster_2S_2R

-- drop table if exists dds.hub_product

CREATE table if not exists dds.hub_counterparty
(counterparty_uuid String,
counterparty_id String,
src String,
effective_from_dttm DateTime,
effective_to_dttm DateTime,
processed_dttm DateTime,
deleted_flg bool
)
ENGINE = MergeTree
order by (counterparty_uuid)

CREATE VIEW dds.v_sn_hub_counterparty AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        counterparty_uuid,
        
        argMax(counterparty_id, processed_dttm) AS counterparty_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_counterparty
    WHERE processed_dttm BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY counterparty_uuid
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_sv_hub_counterparty AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        counterparty_uuid,

        argMax(counterparty_id, processed_dttm) AS counterparty_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_counterparty
    WHERE processed_dttm <= {p_processed_dttm_user:DateTime}
    GROUP BY counterparty_uuid
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_iv_hub_counterparty AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        counterparty_uuid,

        argMax(counterparty_id, processed_dttm) AS counterparty_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_counterparty
    WHERE processed_dttm BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY counterparty_uuid
    HAVING deleted_flg = false
) t1;

CREATE VIEW dds.v_sn_hub_counterparty_salepoint AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        counterparty_salepoint_uuid,
        
        argMax(counterparty_salepoint_id, processed_dttm) AS counterparty_salepoint_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_counterparty_salepoint
    WHERE processed_dttm BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY counterparty_salepoint_uuid
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_sv_hub_counterparty_salepoint AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        counterparty_salepoint_uuid,

        argMax(counterparty_salepoint_id, processed_dttm) AS counterparty_salepoint_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_counterparty_salepoint
    WHERE processed_dttm <= {p_processed_dttm_user:DateTime}
    GROUP BY counterparty_salepoint_uuid
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_iv_hub_counterparty_salepoint AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        counterparty_salepoint_uuid,

        argMax(counterparty_salepoint_id, processed_dttm) AS counterparty_salepoint_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_counterparty_salepoint
    WHERE processed_dttm BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY counterparty_salepoint_uuid
    HAVING deleted_flg = false
) t1;

CREATE table if not exists dds.hub_counterparty_salepoint
(counterparty_salepoint_uuid String,
counterparty_salepoint_id String,
src String,
effective_from_dttm DateTime,
effective_to_dttm DateTime,
processed_dttm DateTime,
deleted_flg bool
)
ENGINE = MergeTree
order by (counterparty_salepoint_uuid)

CREATE table if not exists dds.counterparty
(
counterparty_uuid String,
counterparty_salepoint_uuid String,
--бизнесс данные
inn_code String,
counterparty_name String,
the_subject_code String,
the_subject_name String,
settlement_name String,
district_name String,
address_name String,
md_system_code String,

-- обязательные поля
effective_from_dttm DateTime,
effective_to_dttm DateTime,
processed_dttm DateTime,
deleted_flg bool,
src String,
hash_diff String
)
ENGINE = ReplacingMergeTree
order by (counterparty_uuid, counterparty_salepoint_uuid, hash_diff)

CREATE VIEW dds.v_sn_counterparty AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        counterparty_uuid,
        counterparty_salepoint_uuid,
        hash_diff,
        
        argMax(inn_code, processed_dttm) AS inn_code,
        argMax(counterparty_name, processed_dttm) AS counterparty_name,
        argMax(the_subject_code, processed_dttm) AS the_subject_code,
        argMax(the_subject_name, processed_dttm) AS the_subject_name,
        argMax(settlement_name, processed_dttm) AS settlement_name,
        argMax(district_name, processed_dttm) AS district_name,
        argMax(address_name, processed_dttm) AS address_name,
        argMax(md_system_code, processed_dttm) AS md_system_code,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,
        argMax(src, processed_dttm) AS src,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.counterparty
    WHERE processed_dttm BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY counterparty_uuid, counterparty_salepoint_uuid, hash_diff
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_sv_counterparty AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        counterparty_uuid,
        counterparty_salepoint_uuid,
        hash_diff,

        argMax(inn_code, processed_dttm) AS inn_code,
        argMax(counterparty_name, processed_dttm) AS counterparty_name,
        argMax(the_subject_code, processed_dttm) AS the_subject_code,
        argMax(the_subject_name, processed_dttm) AS the_subject_name,
        argMax(settlement_name, processed_dttm) AS settlement_name,
        argMax(district_name, processed_dttm) AS district_name,
        argMax(address_name, processed_dttm) AS address_name,
        argMax(md_system_code, processed_dttm) AS md_system_code,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,
        argMax(src, processed_dttm) AS src,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.counterparty
    WHERE processed_dttm <= {p_processed_dttm_user:DateTime}
    GROUP BY counterparty_uuid, counterparty_salepoint_uuid, hash_diff
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_iv_counterparty AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        counterparty_uuid,
        counterparty_salepoint_uuid,
        hash_diff,

        argMax(inn_code, processed_dttm) AS inn_code,
        argMax(counterparty_name, processed_dttm) AS counterparty_name,
        argMax(the_subject_code, processed_dttm) AS the_subject_code,
        argMax(the_subject_name, processed_dttm) AS the_subject_name,
        argMax(settlement_name, processed_dttm) AS settlement_name,
        argMax(district_name, processed_dttm) AS district_name,
        argMax(address_name, processed_dttm) AS address_name,
        argMax(md_system_code, processed_dttm) AS md_system_code,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,
        argMax(src, processed_dttm) AS src,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.counterparty
    WHERE processed_dttm BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY counterparty_uuid, counterparty_salepoint_uuid, hash_diff
    HAVING deleted_flg = false
) t1;