-- ============================================================
-- 05_file_formats_stages.sql
-- File formats, storage integration, and named stages
-- ============================================================

USE SCHEMA MARKETING_ANALYTICS.RAW;

-- ------------------------------------------------------------
-- File format
-- ------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT RAW.marketing_csv_format
    TYPE                         = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER                  = 1
    NULL_IF                      = ('NULL', 'null');


-- ------------------------------------------------------------
-- Storage integration (IAM role-based, no hard-coded keys)
-- Replace the ARN and bucket path with your actual values.
-- After creation, run:
--   DESC INTEGRATION s3_marketing_integration;
-- and grant the STORAGE_AWS_IAM_USER_ARN trust in your AWS role.
-- ------------------------------------------------------------
CREATE OR REPLACE STORAGE INTEGRATION s3_marketing_integration
    TYPE                      = EXTERNAL_STAGE
    STORAGE_PROVIDER          = 'S3'
    ENABLED                   = TRUE
    STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::<account-id>:role/snowflake-marketing-role'
    STORAGE_ALLOWED_LOCATIONS = ('s3://<your-bucket>/marketing-data/');


-- ------------------------------------------------------------
-- External stage (S3) — used by Snowpipe for auto-ingest
-- ------------------------------------------------------------
CREATE OR REPLACE STAGE RAW.marketing_s3_stage
    URL               = 's3://<your-bucket>/marketing-data/'
    STORAGE_INTEGRATION = s3_marketing_integration
    FILE_FORMAT       = RAW.marketing_csv_format;


-- ------------------------------------------------------------
-- Internal stage — used for manual / ad-hoc COPY INTO loads
-- ------------------------------------------------------------
CREATE OR REPLACE STAGE RAW.marketing_stage
    FILE_FORMAT = RAW.marketing_csv_format;
