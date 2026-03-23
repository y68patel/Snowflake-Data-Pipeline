// ============================================================
// 03_dim_ad_group.pq
// Ad Group dimension
// Source: MARKETING_ANALYTICS.STAGING.DIM_AD_GROUP
// Load mode: Import (small, rarely changes)
// ============================================================

let
    Source = Snowflake.Databases(
        "your_account.snowflakecomputing.com",
        "COMPUTE_WH",
        [Database = "MARKETING_ANALYTICS"]
    ),
    Database     = Source{[Name = "MARKETING_ANALYTICS"]}[Data],
    Staging      = Database{[Schema = "STAGING"]}[Data],
    DimAdGroup   = Staging{[Name = "DIM_AD_GROUP"]}[Data],

    Selected     = Table.SelectColumns(DimAdGroup, {
        "ad_group_id",
        "campaign_id",
        "ad_group_name"
    }),

    TypedTable   = Table.TransformColumnTypes(Selected, {
        {"ad_group_id",   Int64.Type},
        {"campaign_id",   Int64.Type},
        {"ad_group_name", type text}
    })
in
    TypedTable
