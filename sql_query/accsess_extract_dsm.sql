/* 1. Аксессор получения актуального среза на конкретную дату обработки*/
CREATE VIEW stg.v_sn_mart_dsm_sale ON CLUSTER cluster_2S_2R
AS 
SELECT md.*
FROM stg.mart_dsm_sale md
INNER JOIN (
    SELECT 
        cd_reg,
        cd_u,
        stat_year,
        stat_month,
        sales_type_id,
        effective_dttm,
        MAX(processed_dttm) as processed_dttm
    FROM stg.mart_dsm_sale
    GROUP BY 
        cd_reg, cd_u, stat_year, stat_month, sales_type_id, effective_dttm
) t1 
USING (cd_reg, cd_u, stat_year, stat_month, sales_type_id, effective_dttm, processed_dttm)
WHERE md.deleted_flag = false;

/* 2. Аксессор получения актуального среза на конкретную версию-дату */
CREATE VIEW IF NOT EXISTS stg.v_sv_mart_dsm_sale ON CLUSTER cluster_2S_2R
AS 
SELECT md.*
FROM stg.mart_dsm_sale md
INNER JOIN (
    SELECT 
        cd_reg,
        cd_u,
        stat_year,
        stat_month,
        sales_type_id,
        effective_dttm,
        MAX(processed_dttm) as processed_dttm
    FROM stg.mart_dsm_sale
    WHERE processed_dttm <= {p_processed_dttm_user:DateTime}
    GROUP BY cd_reg, cd_u, stat_year, stat_month, sales_type_id, effective_dttm
) t1 
USING (cd_reg, cd_u, stat_year, stat_month, sales_type_id, effective_dttm, processed_dttm)
WHERE md.deleted_flag = false;

/* 3. Аксессор получения инкремента для */
CREATE VIEW IF NOT EXISTS stg.v_iv_mart_dsm_sale ON CLUSTER cluster_2S_2R
AS 
SELECT md.*
FROM stg.mart_dsm_sale md
INNER JOIN (
    SELECT 
        cd_reg,
        cd_u,
        stat_year,
        stat_month,
        sales_type_id,
        effective_dttm,
        MAX(processed_dttm) as processed_dttm
    FROM stg.mart_dsm_sale
    WHERE processed_dttm BETWEEN {p_from_dttm: DateTime} AND {p_to_dttm:DateTime}
    GROUP BY cd_reg, cd_u, stat_year, stat_month, sales_type_id, effective_dttm
) t1 
USING (cd_reg, cd_u, stat_year, stat_month, sales_type_id, effective_dttm, processed_dttm)
WHERE md.deleted_flag = false;

/* mart_dsm_stat_product */

/* 1. Аксессор получения актуального среза на конкретную дату обработки*/
CREATE VIEW IF NOT EXISTS stg.v_sn_mart_dsm_stat_product ON CLUSTER cluster_2S_2R
AS 
SELECT md.*
FROM stg.mart_dsm_stat_product md
INNER JOIN (
    SELECT 
        cd_u,
        MAX(processed_dttm) as processed_dttm
    FROM stg.mart_dsm_stat_product
    GROUP BY cd_u
) t1 
USING (cd_u, processed_dttm)
WHERE md.deleted_flag = false;

/* 2. Аксессор получения актуального среза на конкретную версию-дату */
CREATE VIEW IF NOT EXISTS stg.v_sv_mart_dsm_stat_product ON CLUSTER cluster_2S_2R
AS 
SELECT md.*
FROM stg.mart_dsm_stat_product md
INNER JOIN (
    SELECT 
        cd_u,
        MAX(processed_dttm) as processed_dttm
    FROM stg.mart_dsm_stat_product
    WHERE processed_dttm <= {p_processed_dttm_user:DateTime}
    GROUP BY cd_u
) t1 
USING (cd_u, processed_dttm)
WHERE md.deleted_flag = false;

/* 3. Аксессор получения инкремента для */
CREATE VIEW IF NOT EXISTS stg.v_iv_mart_dsm_stat_product ON CLUSTER cluster_2S_2R
AS 
SELECT md.*
FROM stg.mart_dsm_stat_product md
INNER JOIN (
    SELECT 
        cd_u,
        MAX(processed_dttm) as processed_dttm
    FROM stg.mart_dsm_stat_product
    WHERE processed_dttm BETWEEN {p_from_dttm: DateTime} AND {p_to_dttm:DateTime}
    GROUP BY cd_u
) t1 
USING (cd_u, processed_dttm)
WHERE md.deleted_flag = false;

/* mart_dsm_region */

/* 1. Аксессор получения актуального среза на конкретную дату обработки*/
CREATE VIEW if not exists stg.v_sn_mart_dsm_region ON CLUSTER cluster_2S_2R
AS 
SELECT md.*
FROM stg.mart_dsm_region md
INNER JOIN (
    SELECT 
        cd_reg,
        sales_type_id,
        MAX(processed_dttm) as processed_dttm
    FROM stg.mart_dsm_region
    GROUP BY cd_reg, sales_type_id
) t1 
USING (cd_reg, sales_type_id, processed_dttm)
WHERE md.deleted_flag = false;

/* 2. Аксессор получения актуального среза на конкретную версию-дату */
CREATE VIEW IF NOT EXISTS stg.v_sv_mart_dsm_region ON CLUSTER cluster_2S_2R
AS 
SELECT md.*
FROM stg.mart_dsm_region md
INNER JOIN (
    SELECT 
        cd_reg,
        sales_type_id,
        MAX(processed_dttm) as processed_dttm
    FROM stg.mart_dsm_region
    WHERE processed_dttm <= {p_processed_dttm_user:DateTime}
    GROUP BY cd_reg, sales_type_id
) t1 
USING (cd_reg, sales_type_id, processed_dttm)
WHERE md.deleted_flag = false;

/* 3. Аксессор получения инкремента для */
CREATE VIEW IF NOT EXISTS stg.v_iv_mart_dsm_region ON CLUSTER cluster_2S_2R
AS 
SELECT md.*
FROM stg.mart_dsm_region md
INNER JOIN (
    SELECT 
        cd_reg,
        sales_type_id,
        MAX(processed_dttm) as processed_dttm
    FROM stg.mart_dsm_region
    WHERE processed_dttm BETWEEN {p_from_dttm: DateTime} AND {p_to_dttm:DateTime}
    GROUP BY cd_reg, sales_type_id
) t1 
USING (cd_reg, sales_type_id, processed_dttm)
WHERE md.deleted_flag = false;

/* Удаление старых представлений */
DROP VIEW IF EXISTS stg.v_sn_mart_dsm_sale;
DROP VIEW IF EXISTS stg.v_sv_mart_dsm_sale ON CLUSTER cluster_2S_2R;
DROP VIEW IF EXISTS stg.v_iv_mart_dsm_sale ON CLUSTER cluster_2S_2R;

DROP VIEW IF EXISTS stg.v_sn_mart_dsm_stat_product ON CLUSTER cluster_2S_2R;
DROP VIEW IF EXISTS stg.v_sv_mart_dsm_stat_product ON CLUSTER cluster_2S_2R;
DROP VIEW IF EXISTS stg.v_iv_mart_dsm_stat_product ON CLUSTER cluster_2S_2R;

DROP VIEW IF EXISTS stg.v_sn_mart_dsm_region;
DROP VIEW IF EXISTS stg.v_sv_mart_dsm_region ON CLUSTER cluster_2S_2R;
DROP VIEW IF EXISTS stg.v_iv_mart_dsm_region ON CLUSTER cluster_2S_2R;

/* Тестовые запросы для проверки новых представлений */
-- SELECT * FROM stg.v_sn_mart_dsm_sale ON CLUSTER cluster_2S_2R;
-- SELECT * FROM stg.v_sv_mart_dsm_sale (p_processed_dttm_user = '2025-10-27');
-- SELECT * FROM stg.v_iv_mart_dsm_sale (p_from_dttm = '2025-10-26', p_to_dttm = '2025-10-27');

-- SELECT * FROM stg.v_sn_mart_dsm_stat_product ON CLUSTER cluster_2S_2R;
-- SELECT * FROM stg.v_sv_mart_dsm_stat_product (p_processed_dttm_user = '2025-10-27'); 
-- SELECT * FROM stg.v_iv_mart_dsm_stat_product (p_from_dttm = '2025-10-26', p_to_dttm = '2025-10-27');

-- SELECT * FROM stg.v_sn_mart_dsm_region ON CLUSTER cluster_2S_2R;
-- SELECT * FROM stg.v_sv_mart_dsm_region (p_processed_dttm_user = '2025-10-27'); 
-- SELECT * FROM stg.v_iv_mart_dsm_region (p_from_dttm = '2025-10-26', p_to_dttm = '2025-10-27');
