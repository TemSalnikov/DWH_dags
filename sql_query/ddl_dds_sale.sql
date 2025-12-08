CREATE table if not exists dds.hub_sale
(sale_uuid String,
sale_id String,
src String,
effective_from_dttm DateTime,
effective_to_dttm DateTime,
processed_dttm DateTime,
deleted_flg bool
)
ENGINE = MergeTree
order by (sale_uuid);

CREATE VIEW dds.v_sn_hub_sale AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        sale_uuid,
        
        argMax(sale_id, processed_dttm) AS sale_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_sale
    GROUP BY sale_uuid
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_sv_hub_sale AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        sale_uuid,

        argMax(sale_id, processed_dttm) AS sale_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_sale
    WHERE processed_dttm <= {p_processed_dttm_user:DateTime}
    GROUP BY sale_uuid
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_iv_hub_sale AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        sale_uuid,

        argMax(sale_id, processed_dttm) AS sale_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_sale
    WHERE processed_dttm
          BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY sale_uuid
    HAVING deleted_flg = false
) t1;

CREATE table if not exists dds.sale
(
sale_uuid String,
-- бизнес данные
sale_date String,
counterparty_uuid String,
counterparty_salepoint_uuid String,
product_uuid String,
gtin_code String,
series_code String,
best_before_date String,
type_of_disposal_name String,
sale_sum Int32,
sale_cnt Int32,
source_of_financing_name String,
update_date DateTime,
type_of_export_name String,
completeness_of_disposal_name String,
-- обязательные поля
effective_from_dttm DateTime,
effective_to_dttm DateTime,
processed_dttm DateTime,
deleted_flg bool,
src String,
hash_diff String
)
ENGINE = MergeTree
order by (sale_uuid, hash_diff);

CREATE VIEW dds.v_sn_sale AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        sale_uuid,
        hash_diff,
        
        argMax(sale_date, processed_dttm) AS sale_date,
        argMax(counterparty_uuid, processed_dttm) AS counterparty_uuid,
        argMax(counterparty_salepoint_uuid, processed_dttm) AS counterparty_salepoint_uuid,
        argMax(product_uuid, processed_dttm) AS product_uuid,
        argMax(gtin_code, processed_dttm) AS gtin_code,
        argMax(series_code, processed_dttm) AS series_code,
        argMax(best_before_date, processed_dttm) AS best_before_date,
        argMax(type_of_disposal_name, processed_dttm) AS type_of_disposal_name,
        argMax(sale_sum, processed_dttm) AS sale_sum,
        argMax(sale_cnt, processed_dttm) AS sale_cnt,
        argMax(source_of_financing_name, processed_dttm) AS source_of_financing_name,
        argMax(update_date, processed_dttm) AS update_date,
        argMax(type_of_export_name, processed_dttm) AS type_of_export_name,
        argMax(completeness_of_disposal_name, processed_dttm) AS completeness_of_disposal_name,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,
        argMax(src, processed_dttm) AS src,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.sale
    GROUP BY sale_uuid, hash_diff
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_sv_sale AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        sale_uuid,
        hash_diff,

        argMax(sale_date, processed_dttm) AS sale_date,
        argMax(counterparty_uuid, processed_dttm) AS counterparty_uuid,
        argMax(counterparty_salepoint_uuid, processed_dttm) AS counterparty_salepoint_uuid,
        argMax(product_uuid, processed_dttm) AS product_uuid,
        argMax(gtin_code, processed_dttm) AS gtin_code,
        argMax(series_code, processed_dttm) AS series_code,
        argMax(best_before_date, processed_dttm) AS best_before_date,
        argMax(type_of_disposal_name, processed_dttm) AS type_of_disposal_name,
        argMax(sale_sum, processed_dttm) AS sale_sum,
        argMax(sale_cnt, processed_dttm) AS sale_cnt,
        argMax(source_of_financing_name, processed_dttm) AS source_of_financing_name,
        argMax(update_date, processed_dttm) AS update_date,
        argMax(type_of_export_name, processed_dttm) AS type_of_export_name,
        argMax(completeness_of_disposal_name, processed_dttm) AS completeness_of_disposal_name,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,
        argMax(src, processed_dttm) AS src,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.sale
    WHERE processed_dttm <= {p_processed_dttm_user:DateTime}
    GROUP BY sale_uuid, hash_diff
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_iv_sale AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        sale_uuid,
        hash_diff,

        argMax(sale_date, processed_dttm) AS sale_date,
        argMax(counterparty_uuid, processed_dttm) AS counterparty_uuid,
        argMax(counterparty_salepoint_uuid, processed_dttm) AS counterparty_salepoint_uuid,
        argMax(product_uuid, processed_dttm) AS product_uuid,
        argMax(gtin_code, processed_dttm) AS gtin_code,
        argMax(series_code, processed_dttm) AS series_code,
        argMax(best_before_date, processed_dttm) AS best_before_date,
        argMax(type_of_disposal_name, processed_dttm) AS type_of_disposal_name,
        argMax(sale_sum, processed_dttm) AS sale_sum,
        argMax(sale_cnt, processed_dttm) AS sale_cnt,
        argMax(source_of_financing_name, processed_dttm) AS source_of_financing_name,
        argMax(update_date, processed_dttm) AS update_date,
        argMax(type_of_export_name, processed_dttm) AS type_of_export_name,
        argMax(completeness_of_disposal_name, processed_dttm) AS completeness_of_disposal_name,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,
        argMax(src, processed_dttm) AS src,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.sale
    WHERE processed_dttm BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY sale_uuid, hash_diff
    HAVING deleted_flg = false
) t1;