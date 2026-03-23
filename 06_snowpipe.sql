-- ============================================================
-- 06_snowpipe.sql
-- Snowpipe definitions for automated, continuous data ingestion
-- from S3 into RAW tables via event notifications
--
-- Pre-requisites:
--   05_file_formats_stages.sql must be executed first
--
-- Post-deployment steps (once per pipe):
--   1. Run: SHOW PIPES IN SCHEMA MARKETING_ANALYTICS.RAW;
--   2. Copy the notification_channel (SQS ARN) for each pipe
--   3. In AWS S3 bucket → Properties → Event notifications:
--        - Event type : s3:ObjectCreated:*
--        - Prefix     : marketing-data/dim_campaign/
--        - Destination: SQS → paste the notification_channel ARN
--   Repeat step 3 for each pipe's prefix and SQS ARN.
-- ============================================================

USE SCHEMA MARKETING_ANALYTICS.RAW;


-- ------------------------------------------------------------
-- Pipe: DIM_CAMPAIGN
-- Trigger path: s3://<bucket>/marketing-data/dim_campaign/
-- ------------------------------------------------------------
CREATE OR REPLACE PIPE RAW.dim_campaign_pipe
    AUTO_INGEST = TRUE
    COMMENT     = 'Ingest campaign dimension CSV files from S3'
AS
COPY INTO RAW.DIM_CAMPAIGN (campaign_id, campaign_name, channel)
FROM (
    SELECT
        $1::INTEGER,  -- campaign_id
        $2::STRING,   -- campaign_name
        $3::STRING    -- channel
    FROM @RAW.marketing_s3_stage/dim_campaign/
)
FILE_FORMAT = (FORMAT_NAME = RAW.marketing_csv_format)
ON_ERROR    = 'CONTINUE';


-- ------------------------------------------------------------
-- Pipe: DIM_AD_GROUP
-- Trigger path: s3://<bucket>/marketing-data/dim_ad_group/
-- ------------------------------------------------------------
CREATE OR REPLACE PIPE RAW.dim_ad_group_pipe
    AUTO_INGEST = TRUE
    COMMENT     = 'Ingest ad group dimension CSV files from S3'
AS
COPY INTO RAW.DIM_AD_GROUP (ad_group_id, campaign_id, ad_group_name)
FROM (
    SELECT
        $1::INTEGER,  -- ad_group_id
        $2::INTEGER,  -- campaign_id
        $3::STRING    -- ad_group_name
    FROM @RAW.marketing_s3_stage/dim_ad_group/
)
FILE_FORMAT = (FORMAT_NAME = RAW.marketing_csv_format)
ON_ERROR    = 'CONTINUE';


-- ------------------------------------------------------------
-- Pipe: DIM_AD
-- Trigger path: s3://<bucket>/marketing-data/dim_ad/
-- ------------------------------------------------------------
CREATE OR REPLACE PIPE RAW.dim_ad_pipe
    AUTO_INGEST = TRUE
    COMMENT     = 'Ingest ad dimension CSV files from S3'
AS
COPY INTO RAW.DIM_AD (ad_id, ad_group_id, ad_name)
FROM (
    SELECT
        $1::INTEGER,  -- ad_id
        $2::INTEGER,  -- ad_group_id
        $3::STRING    -- ad_name
    FROM @RAW.marketing_s3_stage/dim_ad/
)
FILE_FORMAT = (FORMAT_NAME = RAW.marketing_csv_format)
ON_ERROR    = 'CONTINUE';


-- ------------------------------------------------------------
-- Pipe: FACT_AD_PERFORMANCE_DAILY
-- Trigger path: s3://<bucket>/marketing-data/fact_ad_performance/
-- ------------------------------------------------------------
CREATE OR REPLACE PIPE RAW.fact_ad_performance_pipe
    AUTO_INGEST = TRUE
    COMMENT     = 'Ingest daily ad performance fact CSV files from S3'
AS
COPY INTO RAW.FACT_AD_PERFORMANCE_DAILY
    (date, ad_id, country, city, device, impressions, clicks, conversions, cost)
FROM (
    SELECT
        $1::DATE,          -- date
        $2::INTEGER,       -- ad_id
        $3::STRING,        -- country
        $4::STRING,        -- city
        $5::STRING,        -- device
        $6::INTEGER,       -- impressions
        $7::INTEGER,       -- clicks
        $8::INTEGER,       -- conversions
        $9::NUMBER(12,2)   -- cost
    FROM @RAW.marketing_s3_stage/fact_ad_performance/
)
FILE_FORMAT = (FORMAT_NAME = RAW.marketing_csv_format)
ON_ERROR    = 'CONTINUE';


-- ------------------------------------------------------------
-- Useful operational queries
-- ------------------------------------------------------------

-- Check pipe status and lag
SHOW PIPES IN SCHEMA MARKETING_ANALYTICS.RAW;

-- Monitor ingestion history (last 24 hours)
SELECT *
FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    TABLE_NAME    => 'RAW.FACT_AD_PERFORMANCE_DAILY',
    START_TIME    => DATEADD('hours', -24, CURRENT_TIMESTAMP())
));

-- Pause / resume a pipe
-- ALTER PIPE RAW.fact_ad_performance_pipe PAUSE;
-- ALTER PIPE RAW.fact_ad_performance_pipe RESUME;

-- Manually refresh a pipe (catch files that arrived before the SQS notification was configured)
-- ALTER PIPE RAW.dim_campaign_pipe REFRESH;
