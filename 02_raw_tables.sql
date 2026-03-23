-- ============================================================
-- 02_raw_tables.sql
-- RAW layer tables — landing zone for ingested data
-- ============================================================

USE SCHEMA MARKETING_ANALYTICS.RAW;

CREATE OR REPLACE TABLE RAW.DIM_CAMPAIGN (
    campaign_id       INTEGER,
    campaign_name     STRING,
    channel           STRING
);

CREATE OR REPLACE TABLE RAW.DIM_AD_GROUP (
    ad_group_id       INTEGER,
    campaign_id       INTEGER,
    ad_group_name     STRING
);

CREATE OR REPLACE TABLE RAW.DIM_AD (
    ad_id             INTEGER,
    ad_group_id       INTEGER,
    ad_name           STRING
);

CREATE OR REPLACE TABLE RAW.FACT_AD_PERFORMANCE_DAILY (
    date              DATE,
    ad_id             INTEGER,
    country           STRING,
    city              STRING,
    device            STRING,
    impressions       INTEGER,
    clicks            INTEGER,
    conversions       INTEGER,
    cost              NUMBER(12,2)
);
