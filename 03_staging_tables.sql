-- ============================================================
-- 03_staging_tables.sql
-- STAGING layer tables — cleansed, keyed, with audit columns
-- ============================================================

USE SCHEMA MARKETING_ANALYTICS.STAGING;

CREATE OR REPLACE TABLE STAGING.DIM_CAMPAIGN (
    campaign_id       INTEGER       NOT NULL,
    campaign_name     STRING,
    channel           STRING,
    updated_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_campaign PRIMARY KEY (campaign_id)
);

CREATE OR REPLACE TABLE STAGING.DIM_AD_GROUP (
    ad_group_id       INTEGER       NOT NULL,
    campaign_id       INTEGER       NOT NULL,
    ad_group_name     STRING,
    updated_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_ad_group PRIMARY KEY (ad_group_id)
);

CREATE OR REPLACE TABLE STAGING.DIM_AD (
    ad_id             INTEGER       NOT NULL,
    ad_group_id       INTEGER       NOT NULL,
    ad_name           STRING,
    updated_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_dim_ad PRIMARY KEY (ad_id)
);

CREATE OR REPLACE TABLE STAGING.FACT_AD_PERFORMANCE_DAILY (
    date              DATE          NOT NULL,
    ad_id             INTEGER       NOT NULL,
    country           STRING        NOT NULL,
    city              STRING        NOT NULL,
    device            STRING        NOT NULL,
    impressions       INTEGER,
    clicks            INTEGER,
    conversions       INTEGER,
    cost              NUMBER(12,2),
    updated_at        TIMESTAMP     DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_fact_ad_perf PRIMARY KEY (date, ad_id, country, city, device)
);
