-- =========================================================
-- AI_PARSE_DOCUMENT タイムアウト時のクイック復旧スクリプト
-- =========================================================
-- AI_PARSE_DOCUMENTが遅い場合やタイムアウトした場合に使用してください。
-- このスクリプトはPRODUCT_MASTERテーブルのみを復旧します。
-- Part1の残りの処理は通常通り実行してください。
-- =========================================================

USE ROLE accountadmin;
USE DATABASE snowretail_db;
USE SCHEMA snowretail_schema;
USE WAREHOUSE compute_wh;

-- PRODUCT_MASTER テーブルをCSVから直接作成
CREATE OR REPLACE TABLE PRODUCT_MASTER AS
SELECT
    $1::string AS product_id,
    $2::string AS product_name,
    $3::integer AS unit_price
FROM @FILE/product_master.csv
(FILE_FORMAT => (TYPE = 'CSV' SKIP_HEADER = 1));

-- 確認
SELECT 'PRODUCT_MASTER created from CSV: ' || COUNT(*) || ' rows' AS status 
FROM PRODUCT_MASTER;

-- データサンプル表示
SELECT * FROM PRODUCT_MASTER LIMIT 10;

