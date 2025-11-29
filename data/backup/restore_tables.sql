-- =========================================================
-- Snowflake Cortex Handson バックアップ復旧スクリプト
-- =========================================================
-- このスクリプトは以下の場合に使用します:
-- 1. AI_PARSE_DOCUMENTがタイムアウトした場合
-- 2. Part1の処理をスキップしてPart2から開始したい場合
-- 3. デモ環境で事前にデータを準備したい場合
-- =========================================================

-- ロール、データベース、スキーマの設定
USE ROLE accountadmin;
USE DATABASE snowretail_db;
USE SCHEMA snowretail_schema;
USE WAREHOUSE compute_wh;

-- =========================================================
-- Step 1: 基本テーブルの復旧
-- =========================================================

-- PRODUCT_MASTER テーブル (AI_PARSE_DOCUMENTの結果と同等)
CREATE OR REPLACE TABLE PRODUCT_MASTER AS
SELECT
    $1::string AS product_id,
    $2::string AS product_name,
    $3::integer AS unit_price
FROM @FILE/product_master.csv
(FILE_FORMAT => (TYPE = 'CSV' SKIP_HEADER = 1));

SELECT 'PRODUCT_MASTER restored: ' || COUNT(*) || ' rows' AS status FROM PRODUCT_MASTER;

-- =========================================================
-- Step 2: Embedding テーブルの作成
-- =========================================================

-- PRODUCT_MASTER_EMBED テーブル
CREATE OR REPLACE TABLE PRODUCT_MASTER_EMBED AS 
SELECT 
    *, 
    SNOWFLAKE.CORTEX.EMBED_TEXT_1024('multilingual-e5-large', product_name) AS product_name_embed 
FROM PRODUCT_MASTER;

SELECT 'PRODUCT_MASTER_EMBED restored: ' || COUNT(*) || ' rows' AS status FROM PRODUCT_MASTER_EMBED;

-- RETAIL_DATA_EMBED テーブル
CREATE OR REPLACE TABLE RETAIL_DATA_EMBED AS 
SELECT 
    *, 
    SNOWFLAKE.CORTEX.EMBED_TEXT_1024('multilingual-e5-large', product_name) AS product_name_embed 
FROM RETAIL_DATA;

SELECT 'RETAIL_DATA_EMBED restored: ' || COUNT(*) || ' rows' AS status FROM RETAIL_DATA_EMBED;

-- =========================================================
-- Step 3: 正規化データテーブルの作成
-- =========================================================

-- NORMALIZED_EC_DATA_EMBED テーブル
CREATE OR REPLACE TABLE NORMALIZED_EC_DATA_EMBED AS 
WITH normalized_ec_data AS(
    SELECT
      *,
      UPPER(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(PRODUCT_NAME, '[\[\【\（\＜\［].*?[\]\】\）\＞\］]', ''),
            '(店頭|ネット)',
            ''
          ),
          '\\s+',
          ''
        )
      ) AS normalized_product_name
    FROM EC_DATA
)
SELECT 
    *, 
    SNOWFLAKE.CORTEX.EMBED_TEXT_1024('multilingual-e5-large', COLLATE(normalized_product_name, 'unicode-ci')) AS normalized_product_name_embed 
FROM normalized_ec_data;

SELECT 'NORMALIZED_EC_DATA_EMBED restored: ' || COUNT(*) || ' rows' AS status FROM NORMALIZED_EC_DATA_EMBED;

-- NORMALIZED_RETAIL_DATA_EMBED テーブル
CREATE OR REPLACE TABLE NORMALIZED_RETAIL_DATA_EMBED AS 
WITH normalized_retail_data AS(
    SELECT
      *,
      UPPER(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(PRODUCT_NAME, '[\[\【\（\＜\［].*?[\]\】\）\＞\］]', ''),
            '(店頭|ネット)',
            ''
          ),
          '\\s+',
          ''
        )
      ) AS normalized_product_name
    FROM RETAIL_DATA
)
SELECT 
    *, 
    SNOWFLAKE.CORTEX.EMBED_TEXT_1024('multilingual-e5-large', COLLATE(normalized_product_name, 'unicode-ci')) AS normalized_product_name_embed 
FROM normalized_retail_data;

SELECT 'NORMALIZED_RETAIL_DATA_EMBED restored: ' || COUNT(*) || ' rows' AS status FROM NORMALIZED_RETAIL_DATA_EMBED;

-- =========================================================
-- Step 4: LLM適用テーブルの作成
-- =========================================================

-- PRODUCT_MASTER_APPLIED_LLM テーブル（メーカー・商品名分離）
CREATE OR REPLACE TABLE PRODUCT_MASTER_APPLIED_LLM AS
WITH normalized_product_master AS(
    SELECT *, AI_COMPLETE(
        model => 'claude-haiku-4-5',
        prompt => CONCAT('入力値からメーカーと商品名を分離してください。
            なお、必ずしもメーカー名があるわけではなく、メーカー名がない場合は空欄で返してください。入力値は',product_name, 'です'),
        model_parameters => {
            'temperature': 0.7,
            'max_tokens': 8000
        },
       response_format => {
            'type':'json',
            'schema': {
                    'type': 'object',
                    'properties': {
                        'sub_maker_name': {
                            'type': 'string',
                            'description': 'メーカーの名前'
                        },
                        'sub_product_name': {
                            'type': 'string',
                            'description': '商品の名前'
                        }
                    },
                    'required': ['sub_maker_name','sub_product_name'],
                    'additionalProperties': false
                }
            }       
    ) AS result_json
    FROM PRODUCT_MASTER
),
normalized_product_master_2 AS (
    SELECT 
        *,
        result_json:sub_maker_name::varchar AS sub_maker_name,
        result_json:sub_product_name::varchar AS sub_product_name
    FROM normalized_product_master
)
SELECT * FROM normalized_product_master_2;

SELECT 'PRODUCT_MASTER_APPLIED_LLM restored: ' || COUNT(*) || ' rows' AS status FROM PRODUCT_MASTER_APPLIED_LLM;

-- PRODUCT_MASTER_LLM_EMBED テーブル
CREATE OR REPLACE TABLE PRODUCT_MASTER_LLM_EMBED AS 
SELECT 
    *, 
    SNOWFLAKE.CORTEX.EMBED_TEXT_1024('multilingual-e5-large', sub_product_name) AS product_name_embed 
FROM PRODUCT_MASTER_APPLIED_LLM;

SELECT 'PRODUCT_MASTER_LLM_EMBED restored: ' || COUNT(*) || ' rows' AS status FROM PRODUCT_MASTER_LLM_EMBED;

-- =========================================================
-- Step 5: 最終結果テーブルの作成
-- =========================================================

-- EC_DATA_WITH_PRODUCT_MASTER テーブル
CREATE OR REPLACE TABLE EC_DATA_WITH_PRODUCT_MASTER AS 
WITH combined_product_master AS (
    SELECT a.*, b.product_name_embed AS product_name_llm_embed 
    FROM PRODUCT_MASTER_EMBED AS a
    INNER JOIN PRODUCT_MASTER_LLM_EMBED AS b
    ON a.product_id = b.product_id
),
match_ec_product_master AS (
    SELECT 
        a.product_id AS product_id_master,
        a.product_name AS product_name_master,
        a.unit_price AS unit_price_master,
        b.* EXCLUDE(product_id, normalized_product_name, normalized_product_name_embed),
        VECTOR_COSINE_SIMILARITY(a.product_name_embed, b.normalized_product_name_embed) AS similarity_before,
        VECTOR_COSINE_SIMILARITY(a.product_name_llm_embed, b.normalized_product_name_embed) AS similarity_after,
        GREATEST(similarity_before, similarity_after) AS similarity,
        CASE WHEN a.product_id = b.product_id THEN 1 ELSE 0 END AS correct_flg
    FROM combined_product_master a, NORMALIZED_EC_DATA_EMBED b
), 
match_ec_product_master_2 AS (
    SELECT * EXCLUDE(similarity_before, similarity_after, correct_flg)
    FROM match_ec_product_master 
    WHERE similarity > 0.9
)
SELECT * FROM match_ec_product_master_2;

SELECT 'EC_DATA_WITH_PRODUCT_MASTER restored: ' || COUNT(*) || ' rows' AS status FROM EC_DATA_WITH_PRODUCT_MASTER;

-- RETAIL_DATA_WITH_PRODUCT_MASTER テーブル
CREATE OR REPLACE TABLE RETAIL_DATA_WITH_PRODUCT_MASTER AS 
WITH combined_product_master AS (
    SELECT a.*, b.product_name_embed AS product_name_llm_embed 
    FROM PRODUCT_MASTER_EMBED AS a
    INNER JOIN PRODUCT_MASTER_LLM_EMBED AS b
    ON a.product_id = b.product_id
),
match_retail_product_master AS (
    SELECT 
        a.product_id AS product_id_master,
        a.product_name AS product_name_master,
        a.unit_price AS unit_price_master,
        b.* EXCLUDE(product_id, normalized_product_name, normalized_product_name_embed),
        VECTOR_COSINE_SIMILARITY(a.product_name_embed, b.normalized_product_name_embed) AS similarity_before,
        VECTOR_COSINE_SIMILARITY(a.product_name_llm_embed, b.normalized_product_name_embed) AS similarity_after,
        GREATEST(similarity_before, similarity_after) AS similarity,
        CASE WHEN a.product_id = b.product_id THEN 1 ELSE 0 END AS correct_flg
    FROM combined_product_master a, NORMALIZED_RETAIL_DATA_EMBED b
), 
match_retail_product_master_2 AS (
    SELECT * EXCLUDE(similarity_before, similarity_after, correct_flg)
    FROM match_retail_product_master 
    WHERE similarity > 0.9
)
SELECT * FROM match_retail_product_master_2;

SELECT 'RETAIL_DATA_WITH_PRODUCT_MASTER restored: ' || COUNT(*) || ' rows' AS status FROM RETAIL_DATA_WITH_PRODUCT_MASTER;

-- =========================================================
-- Step 6: 顧客分析テーブルの作成（Part2 Step1用）
-- =========================================================

-- CUSTOMER_ANALYSIS テーブル（前処理用）
CREATE TABLE IF NOT EXISTS CUSTOMER_ANALYSIS (
    analysis_id NUMBER AUTOINCREMENT,
    review_id VARCHAR(20),
    product_id VARCHAR(10),
    customer_id VARCHAR(10),
    rating NUMBER(2,1),
    review_text TEXT,
    review_date TIMESTAMP_NTZ,
    purchase_channel VARCHAR(20),
    helpful_votes NUMBER(5),
    chunked_text TEXT,
    embedding VECTOR(FLOAT, 1024),
    sentiment_score FLOAT,
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

SELECT 'CUSTOMER_ANALYSIS table created' AS status;

-- =========================================================
-- Step 7: Cortex Search サービスの作成
-- =========================================================

-- Cortex Search ウェアハウスの作成
CREATE OR REPLACE WAREHOUSE cortex_search_wh WITH WAREHOUSE_SIZE='X-SMALL';

-- パラメータをオン
ALTER TABLE PRODUCT_MASTER SET CHANGE_TRACKING = TRUE;

-- Product Master 用 Cortex Search Service作成
CREATE OR REPLACE CORTEX SEARCH SERVICE product_master_service
  ON product_name
  ATTRIBUTES product_name
  WAREHOUSE = cortex_search_wh
  TARGET_LAG = '1 day'
  EMBEDDING_MODEL = 'voyage-multilingual-2'
  AS (
    SELECT * FROM PRODUCT_MASTER
  );

SELECT 'product_master_service created' AS status;

-- Social Retail Documents 用 Cortex Search Service作成
CREATE OR REPLACE CORTEX SEARCH SERVICE snow_retail_search_service
    ON content
    ATTRIBUTES title, document_type, department
    WAREHOUSE = cortex_search_wh
    TARGET_LAG = '1 day'
    EMBEDDING_MODEL = 'voyage-multilingual-2'
    AS (
        SELECT 
            document_id,
            title,
            content,
            document_type,
            department,
            created_at,
            updated_at,
            version
        FROM SNOW_RETAIL_DOCUMENTS
    );

SELECT 'snow_retail_search_service created' AS status;

-- =========================================================
-- 完了メッセージ
-- =========================================================
SELECT '========================================' AS message;
SELECT 'All backup tables and services restored successfully!' AS message;
SELECT '========================================' AS message;

