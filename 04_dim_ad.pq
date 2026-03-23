// ============================================================
// 04_dim_ad.pq
// Ad dimension
// Source: MARKETING_ANALYTICS.STAGING.DIM_AD
// Load mode: Import (small, rarely changes)
// ============================================================

let
    Source = Snowflake.Databases(
        "your_account.snowflakecomputing.com",
        "COMPUTE_WH",
        [Database = "MARKETING_ANALYTICS"]
    ),
    Database   = Source{[Name = "MARKETING_ANALYTICS"]}[Data],
    Staging    = Database{[Schema = "STAGING"]}[Data],
    DimAd      = Staging{[Name = "DIM_AD"]}[Data],

    Selected   = Table.SelectColumns(DimAd, {
        "ad_id",
        "ad_group_id",
        "ad_name"
    }),

    TypedTable = Table.TransformColumnTypes(Selected, {
        {"ad_id",       Int64.Type},
        {"ad_group_id", Int64.Type},
        {"ad_name",     type text}
    })
in
    TypedTable
