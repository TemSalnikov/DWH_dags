-- CREATE database dds

-- drop table if exists dds.hub_address

CREATE table if not exists dds.hub_address
(address_uuid String,
address_id String,
src String,
effective_from_dttm DateTime,
effective_to_dttm DateTime,
processed_dttm DateTime,
deleted_flg bool
)
ENGINE = MergeTree
order by (address_uuid)

CREATE table if not exists dds.address
(
address_uuid String,
--бизнесс данные
parent_address_uiid String,
region_name String,
lat_region_name String,
segment_code String,
segment_name String,
region_level_code Int32,
address_name String,
settlement_name String,
region_name String,
district_name
address_name
address_name
--
mnn_code String,
full_trade_name String,
trade_name String,
brand_name String,
package_name String,
address_owner_name String,
corporation_name String,
dosage_name String,
weight_num String,
weight_unit_name String,
blister_cnt String,
blister_package_cnt String,
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
address_type String,
-- обязательные поля
effective_from_dttm DateTime,
effective_to_dttm DateTime,
processed_dttm DateTime,
deleted_flg bool,
src String,
hash_diff String
)
ENGINE = ReplacingMergeTree
order by (address_uuid, hash_diff)

