// ============================================================
// 02_dim_campaign.pq
// Campaign dimension — used as slicer for channel and campaign
// Source: MARKETING_ANALYTICS.STAGING.DIM_CAMPAIGN
// Load mode: Import (small, rarely changes)
// ============================================================

let
    Source = Snowflake.Databases(
        "your_account.snowflakecomputing.com",
        "COMPUTE_WH",
        [Database = "MARKETING_ANALYTICS"]
    ),
    Database    = Source{[Name = "MARKETING_ANALYTICS"]}[Data],
    Staging     = Database{[Schema = "STAGING"]}[Data],
    DimCampaign = Staging{[Name = "DIM_CAMPAIGN"]}[Data],

    // Keep only columns needed for reporting; drop audit column
    Selected    = Table.SelectColumns(DimCampaign, {
        "campaign_id",
        "campaign_name",
        "channel"
    }),

    TypedTable  = Table.TransformColumnTypes(Selected, {
        {"campaign_id",   Int64.Type},
        {"campaign_name", type text},
        {"channel",       type text}
    })
in
    TypedTable
