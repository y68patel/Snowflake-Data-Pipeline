-- ============================================================
-- 10_sample_data.sql
-- Test inserts and manual task executions for dev/QA use only
-- Do NOT run in production
-- ============================================================

USE SCHEMA MARKETING_ANALYTICS.RAW;

-- Insert a test campaign and verify stream captures it
INSERT INTO RAW.DIM_CAMPAIGN (campaign_id, campaign_name, channel)
VALUES (11, 'Test Campaign 11', 'Social');

SELECT * FROM RAW.RAW_DIM_CAMPAIGN_STREAM;

-- Manually execute the campaign task and verify staging
EXECUTE TASK STAGING.LOAD_DIM_CAMPAIGN_TASK;
SELECT * FROM STAGING.DIM_CAMPAIGN WHERE campaign_id = 11;

-- Update a raw record to verify stream captures the change
UPDATE RAW.DIM_CAMPAIGN
SET campaign_id = 12
WHERE campaign_name = 'Test Campaign 11';

SELECT * FROM RAW.RAW_DIM_CAMPAIGN_STREAM;


-- Insert a test fact row and verify the full pipeline
INSERT INTO RAW.FACT_AD_PERFORMANCE_DAILY
    (date, ad_id, country, city, device, impressions, clicks, conversions, cost)
VALUES
    (CURRENT_DATE, 11, 'Canada', 'Toronto', 'Mobile', 100, 15, 3, 20.00);

EXECUTE TASK STAGING.LOAD_DIM_AD_GROUP_TASK;
EXECUTE TASK STAGING.LOAD_DIM_AD_TASK;
EXECUTE TASK STAGING.LOAD_FACT_AD_PERFORMANCE_DAILY_TASK;

SELECT * FROM STAGING.FACT_AD_PERFORMANCE_DAILY WHERE ad_id = 11;


-- Data quality check: rows where clicks exceed impressions (should be 0)
SELECT *
FROM RAW.FACT_AD_PERFORMANCE_DAILY
WHERE clicks > impressions;
