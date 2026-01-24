-- CREATE database dds

-- drop table if exists dds.hub_product

CREATE table if not exists dds.hub_product
(product_uuid String,
product_id String,
src String,
effective_from_dttm DateTime,
effective_to_dttm DateTime,
processed_dttm DateTime,
deleted_flg bool
)
ENGINE = MergeTree
order by (product_uuid)

CREATE VIEW dds.v_sn_hub_product AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        product_uuid,
        
        argMax(product_id, processed_dttm) AS product_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_product
    GROUP BY product_uuid
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_sv_hub_product AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        product_uuid,

        argMax(product_id, processed_dttm) AS product_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_product
    WHERE processed_dttm <= {p_processed_dttm_user:DateTime}
    GROUP BY product_uuid
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_iv_hub_product AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        product_uuid,

        argMax(product_id, processed_dttm) AS product_id,
        argMax(src, processed_dttm) AS src,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.hub_product
    WHERE processed_dttm
          BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY product_uuid
    HAVING deleted_flg = false
) t1;

CREATE table if not exists dds.product
(
product_uuid String,
--бизнесс данные
mnn_code String,
full_trade_name String,
trade_name String,
brand_name String,
package_name String,
product_owner_name String,
corporation_name String,
dosage_name String,
weight_num Float32,
weight_unit_name String,
blister_cnt Int32,
blister_package_cnt Int32,
country_name String,
atc1_code String,
atc1_name String,
atc2_code String,
atc2_name String,
atc3_code String,
atc3_name String,
atc4_code String,
atc4_name String,
atc5_code String,
atc5_name String,
original_type String,
branded_type String,
recipereq_type String,
jnvlp_type String,
ephmra1_code String,
ephmra1_name String,
ephmra2_code String,
ephmra2_name String,
ephmra3_code String,
ephmra3_name String,
ephmra4_code String,
ephmra4_name String,
farm_group_name String,
localized_type String,
dosage_form_name String,
classification_bad1_name String,
compound_bad1_name String,
classification_bad2_name String,
compound_bad2_name String,
product_type String,
-- обязательные поля
effective_from_dttm DateTime,
effective_to_dttm DateTime,
processed_dttm DateTime,
deleted_flg bool,
src String,
hash_diff String
)
ENGINE = MergeTree
order by (product_uuid, hash_diff)

CREATE VIEW dds.v_sn_product AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        product_uuid,
        hash_diff,
        
        argMax(mnn_code, processed_dttm) AS mnn_code,
        argMax(full_trade_name, processed_dttm) AS full_trade_name,
        argMax(trade_name, processed_dttm) AS trade_name,
        argMax(brand_name, processed_dttm) AS brand_name,
        argMax(package_name, processed_dttm) AS package_name,
        argMax(product_owner_name, processed_dttm) AS product_owner_name,
        argMax(corporation_name, processed_dttm) AS corporation_name,
        argMax(dosage_name, processed_dttm) AS dosage_name,
        argMax(weight_num, processed_dttm) AS weight_num,
        argMax(weight_unit_name, processed_dttm) AS weight_unit_name,
        argMax(blister_cnt, processed_dttm) AS blister_cnt,
        argMax(blister_package_cnt, processed_dttm) AS blister_package_cnt,
        argMax(country_name, processed_dttm) AS country_name,
        argMax(atc1_code, processed_dttm) AS atc1_code,
        argMax(atc1_name, processed_dttm) AS atc1_name,
        argMax(atc2_code, processed_dttm) AS atc2_code,
        argMax(atc2_name, processed_dttm) AS atc2_name,
        argMax(atc3_code, processed_dttm) AS atc3_code,
        argMax(atc3_name, processed_dttm) AS atc3_name,
        argMax(atc4_code, processed_dttm) AS atc4_code,
        argMax(atc4_name, processed_dttm) AS atc4_name,
        argMax(atc5_code, processed_dttm) AS atc5_code,
        argMax(atc5_name, processed_dttm) AS atc5_name,
        argMax(original_type, processed_dttm) AS original_type,
        argMax(branded_type, processed_dttm) AS branded_type,
        argMax(recipereq_type, processed_dttm) AS recipereq_type,
        argMax(jnvlp_type, processed_dttm) AS jnvlp_type,
        argMax(ephmra1_code, processed_dttm) AS ephmra1_code,
        argMax(ephmra1_name, processed_dttm) AS ephmra1_name,
        argMax(ephmra2_code, processed_dttm) AS ephmra2_code,
        argMax(ephmra2_name, processed_dttm) AS ephmra2_name,
        argMax(ephmra3_code, processed_dttm) AS ephmra3_code,
        argMax(ephmra3_name, processed_dttm) AS ephmra3_name,
        argMax(ephmra4_code, processed_dttm) AS ephmra4_code,
        argMax(ephmra4_name, processed_dttm) AS ephmra4_name,
        argMax(farm_group_name, processed_dttm) AS farm_group_name,
        argMax(localized_type, processed_dttm) AS localized_type,
        argMax(dosage_form_name, processed_dttm) AS dosage_form_name,
        argMax(classification_bad1_name, processed_dttm) AS classification_bad1_name,
        argMax(compound_bad1_name, processed_dttm) AS compound_bad1_name,
        argMax(classification_bad2_name, processed_dttm) AS classification_bad2_name,
        argMax(compound_bad2_name, processed_dttm) AS compound_bad2_name,
        argMax(product_type, processed_dttm) AS product_type,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,
        argMax(src, processed_dttm) AS src,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.product
    GROUP BY product_uuid, hash_diff
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_sv_product AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        product_uuid,
        hash_diff,

        argMax(mnn_code, processed_dttm) AS mnn_code,
        argMax(full_trade_name, processed_dttm) AS full_trade_name,
        argMax(trade_name, processed_dttm) AS trade_name,
        argMax(brand_name, processed_dttm) AS brand_name,
        argMax(package_name, processed_dttm) AS package_name,
        argMax(product_owner_name, processed_dttm) AS product_owner_name,
        argMax(corporation_name, processed_dttm) AS corporation_name,
        argMax(dosage_name, processed_dttm) AS dosage_name,
        argMax(weight_num, processed_dttm) AS weight_num,
        argMax(weight_unit_name, processed_dttm) AS weight_unit_name,
        argMax(blister_cnt, processed_dttm) AS blister_cnt,
        argMax(blister_package_cnt, processed_dttm) AS blister_package_cnt,
        argMax(country_name, processed_dttm) AS country_name,
        argMax(atc1_code, processed_dttm) AS atc1_code,
        argMax(atc1_name, processed_dttm) AS atc1_name,
        argMax(atc2_code, processed_dttm) AS atc2_code,
        argMax(atc2_name, processed_dttm) AS atc2_name,
        argMax(atc3_code, processed_dttm) AS atc3_code,
        argMax(atc3_name, processed_dttm) AS atc3_name,
        argMax(atc4_code, processed_dttm) AS atc4_code,
        argMax(atc4_name, processed_dttm) AS atc4_name,
        argMax(atc5_code, processed_dttm) AS atc5_code,
        argMax(atc5_name, processed_dttm) AS atc5_name,
        argMax(original_type, processed_dttm) AS original_type,
        argMax(branded_type, processed_dttm) AS branded_type,
        argMax(recipereq_type, processed_dttm) AS recipereq_type,
        argMax(jnvlp_type, processed_dttm) AS jnvlp_type,
        argMax(ephmra1_code, processed_dttm) AS ephmra1_code,
        argMax(ephmra1_name, processed_dttm) AS ephmra1_name,
        argMax(ephmra2_code, processed_dttm) AS ephmra2_code,
        argMax(ephmra2_name, processed_dttm) AS ephmra2_name,
        argMax(ephmra3_code, processed_dttm) AS ephmra3_code,
        argMax(ephmra3_name, processed_dttm) AS ephmra3_name,
        argMax(ephmra4_code, processed_dttm) AS ephmra4_code,
        argMax(ephmra4_name, processed_dttm) AS ephmra4_name,
        argMax(farm_group_name, processed_dttm) AS farm_group_name,
        argMax(localized_type, processed_dttm) AS localized_type,
        argMax(dosage_form_name, processed_dttm) AS dosage_form_name,
        argMax(classification_bad1_name, processed_dttm) AS classification_bad1_name,
        argMax(compound_bad1_name, processed_dttm) AS compound_bad1_name,
        argMax(classification_bad2_name, processed_dttm) AS classification_bad2_name,
        argMax(compound_bad2_name, processed_dttm) AS compound_bad2_name,
        argMax(product_type, processed_dttm) AS product_type,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,
        argMax(src, processed_dttm) AS src,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.product
    WHERE processed_dttm <= {p_processed_dttm_user:DateTime}
    GROUP BY product_uuid, hash_diff
    HAVING deleted_flg = false
) t1;


CREATE VIEW dds.v_iv_product AS
SELECT t1.* EXCEPT (processed_dttm_max), processed_dttm_max AS processed_dttm
FROM (
    SELECT
        product_uuid,
        hash_diff,

        argMax(mnn_code, processed_dttm) AS mnn_code,
        argMax(full_trade_name, processed_dttm) AS full_trade_name,
        argMax(trade_name, processed_dttm) AS trade_name,
        argMax(brand_name, processed_dttm) AS brand_name,
        argMax(package_name, processed_dttm) AS package_name,
        argMax(product_owner_name, processed_dttm) AS product_owner_name,
        argMax(corporation_name, processed_dttm) AS corporation_name,
        argMax(dosage_name, processed_dttm) AS dosage_name,
        argMax(weight_num, processed_dttm) AS weight_num,
        argMax(weight_unit_name, processed_dttm) AS weight_unit_name,
        argMax(blister_cnt, processed_dttm) AS blister_cnt,
        argMax(blister_package_cnt, processed_dttm) AS blister_package_cnt,
        argMax(country_name, processed_dttm) AS country_name,
        argMax(atc1_code, processed_dttm) AS atc1_code,
        argMax(atc1_name, processed_dttm) AS atc1_name,
        argMax(atc2_code, processed_dttm) AS atc2_code,
        argMax(atc2_name, processed_dttm) AS atc2_name,
        argMax(atc3_code, processed_dttm) AS atc3_code,
        argMax(atc3_name, processed_dttm) AS atc3_name,
        argMax(atc4_code, processed_dttm) AS atc4_code,
        argMax(atc4_name, processed_dttm) AS atc4_name,
        argMax(atc5_code, processed_dttm) AS atc5_code,
        argMax(atc5_name, processed_dttm) AS atc5_name,
        argMax(original_type, processed_dttm) AS original_type,
        argMax(branded_type, processed_dttm) AS branded_type,
        argMax(recipereq_type, processed_dttm) AS recipereq_type,
        argMax(jnvlp_type, processed_dttm) AS jnvlp_type,
        argMax(ephmra1_code, processed_dttm) AS ephmra1_code,
        argMax(ephmra1_name, processed_dttm) AS ephmra1_name,
        argMax(ephmra2_code, processed_dttm) AS ephmra2_code,
        argMax(ephmra2_name, processed_dttm) AS ephmra2_name,
        argMax(ephmra3_code, processed_dttm) AS ephmra3_code,
        argMax(ephmra3_name, processed_dttm) AS ephmra3_name,
        argMax(ephmra4_code, processed_dttm) AS ephmra4_code,
        argMax(ephmra4_name, processed_dttm) AS ephmra4_name,
        argMax(farm_group_name, processed_dttm) AS farm_group_name,
        argMax(localized_type, processed_dttm) AS localized_type,
        argMax(dosage_form_name, processed_dttm) AS dosage_form_name,
        argMax(classification_bad1_name, processed_dttm) AS classification_bad1_name,
        argMax(compound_bad1_name, processed_dttm) AS compound_bad1_name,
        argMax(classification_bad2_name, processed_dttm) AS classification_bad2_name,
        argMax(compound_bad2_name, processed_dttm) AS compound_bad2_name,
        argMax(product_type, processed_dttm) AS product_type,
        argMax(effective_from_dttm, processed_dttm) AS effective_from_dttm,
        argMax(effective_to_dttm, processed_dttm) AS effective_to_dttm,
        argMax(src, processed_dttm) AS src,

        argMax(deleted_flg, processed_dttm) AS deleted_flg,
        max(processed_dttm) AS processed_dttm_max

    FROM dds.product
    WHERE processed_dttm BETWEEN {p_from_dttm:DateTime} AND {p_to_dttm:DateTime}
    GROUP BY product_uuid, hash_diff
    HAVING deleted_flg = false
) t1;