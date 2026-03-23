-- ============================================================
-- 08_tasks.sql
-- Task chain: RAW streams → STAGING → ANALYTICS
-- Execution order:
--   LOAD_DIM_CAMPAIGN_TASK  (root, scheduled)
--     └── LOAD_DIM_AD_GROUP_TASK
--           └── LOAD_DIM_AD_TASK
--                 └── LOAD_FACT_AD_PERFORMANCE_DAILY_TASK
--                       └── LOAD_ANALYTICS_FACT_CAMPAIGN_DAILY_TASK
-- ============================================================

USE SCHEMA MARKETING_ANALYTICS.STAGING;

-- ------------------------------------------------------------
-- Task 1 (root): Merge DIM_CAMPAIGN from stream
-- ------------------------------------------------------------
CREATE OR REPLACE TASK STAGING.LOAD_DIM_CAMPAIGN_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE  = 'USING CRON 0 2 * * * UTC'
AS
MERGE INTO STAGING.DIM_CAMPAIGN tgt
USING (
    SELECT campaign_id, campaign_name, channel
    FROM RAW.RAW_DIM_CAMPAIGN_STREAM
) src
ON tgt.campaign_id = src.campaign_id
WHEN MATCHED THEN UPDATE SET
    tgt.campaign_name = src.campaign_name,
    tgt.channel       = src.channel,
    tgt.updated_at    = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (campaign_id, campaign_name, channel)
VALUES
    (src.campaign_id, src.campaign_name, src.channel);


-- ------------------------------------------------------------
-- Task 2: Merge DIM_AD_GROUP from stream
-- ------------------------------------------------------------
CREATE OR REPLACE TASK STAGING.LOAD_DIM_AD_GROUP_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER     STAGING.LOAD_DIM_CAMPAIGN_TASK
AS
MERGE INTO STAGING.DIM_AD_GROUP tgt
USING (
    SELECT ad_group_id, campaign_id, ad_group_name
    FROM RAW.RAW_DIM_AD_GROUP_STREAM
) src
ON tgt.ad_group_id = src.ad_group_id
WHEN MATCHED THEN UPDATE SET
    tgt.campaign_id   = src.campaign_id,
    tgt.ad_group_name = src.ad_group_name,
    tgt.updated_at    = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (ad_group_id, campaign_id, ad_group_name)
VALUES
    (src.ad_group_id, src.campaign_id, src.ad_group_name);


-- ------------------------------------------------------------
-- Task 3: Merge DIM_AD from stream
-- ------------------------------------------------------------
CREATE OR REPLACE TASK STAGING.LOAD_DIM_AD_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER     STAGING.LOAD_DIM_AD_GROUP_TASK
AS
MERGE INTO STAGING.DIM_AD tgt
USING (
    SELECT ad_id, ad_group_id, ad_name
    FROM RAW.RAW_DIM_AD_STREAM
) src
ON tgt.ad_id = src.ad_id
WHEN MATCHED THEN UPDATE SET
    tgt.ad_group_id = src.ad_group_id,
    tgt.ad_name     = src.ad_name,
    tgt.updated_at  = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (ad_id, ad_group_id, ad_name)
VALUES
    (src.ad_id, src.ad_group_id, src.ad_name);


-- ------------------------------------------------------------
-- Task 4: Merge FACT_AD_PERFORMANCE_DAILY from stream
-- ------------------------------------------------------------
CREATE OR REPLACE TASK STAGING.LOAD_FACT_AD_PERFORMANCE_DAILY_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER     STAGING.LOAD_DIM_AD_TASK
AS
MERGE INTO STAGING.FACT_AD_PERFORMANCE_DAILY tgt
USING (
    SELECT date, ad_id, country, city, device,
           impressions, clicks, conversions, cost
    FROM RAW.RAW_FACT_AD_STREAM
) src
ON  tgt.date    = src.date
AND tgt.ad_id   = src.ad_id
AND tgt.country = src.country
AND tgt.city    = src.city
AND tgt.device  = src.device
WHEN MATCHED THEN UPDATE SET
    tgt.impressions  = src.impressions,
    tgt.clicks       = src.clicks,
    tgt.conversions  = src.conversions,
    tgt.cost         = src.cost,
    tgt.updated_at   = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
    (date, ad_id, country, city, device, impressions, clicks, conversions, cost)
VALUES
    (src.date, src.ad_id, src.country, src.city, src.device,
     src.impressions, src.clicks, src.conversions, src.cost);


-- ------------------------------------------------------------
-- Task 5: Rebuild ANALYTICS.FACT_CAMPAIGN_PERFORMANCE_DAILY
-- ------------------------------------------------------------
CREATE OR REPLACE TASK STAGING.LOAD_ANALYTICS_FACT_CAMPAIGN_DAILY_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER     STAGING.LOAD_FACT_AD_PERFORMANCE_DAILY_TASK
AS
INSERT OVERWRITE INTO ANALYTICS.FACT_CAMPAIGN_PERFORMANCE_DAILY
SELECT
    f.date,
    c.campaign_id,
    c.campaign_name,
    c.channel,
    f.device,
    f.country,
    f.city,
    SUM(f.impressions)  AS impressions,
    SUM(f.clicks)       AS clicks,
    SUM(f.conversions)  AS conversions,
    SUM(f.cost)         AS total_spend
FROM STAGING.FACT_AD_PERFORMANCE_DAILY f
JOIN STAGING.DIM_AD       a  ON f.ad_id       = a.ad_id
JOIN STAGING.DIM_AD_GROUP ag ON a.ad_group_id = ag.ad_group_id
JOIN STAGING.DIM_CAMPAIGN c  ON ag.campaign_id = c.campaign_id
GROUP BY
    f.date,
    c.campaign_id,
    c.campaign_name,
    c.channel,
    f.device,
    f.country,
    f.city;


-- ------------------------------------------------------------
-- Resume all tasks (child tasks first, root task last)
-- NOTE: In Snowflake, child tasks must be resumed before root
-- ------------------------------------------------------------
ALTER TASK STAGING.LOAD_ANALYTICS_FACT_CAMPAIGN_DAILY_TASK RESUME;
ALTER TASK STAGING.LOAD_FACT_AD_PERFORMANCE_DAILY_TASK     RESUME;
ALTER TASK STAGING.LOAD_DIM_AD_TASK                        RESUME;
ALTER TASK STAGING.LOAD_DIM_AD_GROUP_TASK                  RESUME;
ALTER TASK STAGING.LOAD_DIM_CAMPAIGN_TASK                  RESUME;
