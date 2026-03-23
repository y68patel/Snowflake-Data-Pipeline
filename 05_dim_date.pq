// ============================================================
// 05_dim_date.pq
// Generated date dimension table for time-intelligence DAX
// Built entirely in Power Query — no Snowflake table required.
// Covers the range 2020-01-01 → today.
// ============================================================

let
    StartDate  = #date(2020, 1, 1),
    EndDate    = Date.From(DateTime.LocalNow()),
    DayCount   = Duration.Days(EndDate - StartDate) + 1,

    DateList   = List.Dates(StartDate, DayCount, #duration(1, 0, 0, 0)),
    DateTable  = Table.FromList(DateList, Splitter.SplitByNothing(), {"Date"}),
    TypedDates = Table.TransformColumnTypes(DateTable, {{"Date", type date}}),

    // Add useful calendar columns
    WithYear        = Table.AddColumn(TypedDates,  "Year",        each Date.Year([Date]),                              Int64.Type),
    WithQuarter     = Table.AddColumn(WithYear,    "Quarter",     each "Q" & Text.From(Date.QuarterOfYear([Date])),   type text),
    WithMonth       = Table.AddColumn(WithQuarter, "MonthNumber", each Date.Month([Date]),                            Int64.Type),
    WithMonthName   = Table.AddColumn(WithMonth,   "MonthName",   each Date.MonthName([Date]),                        type text),
    WithWeek        = Table.AddColumn(WithMonthName,"WeekNumber", each Date.WeekOfYear([Date]),                       Int64.Type),
    WithDayName     = Table.AddColumn(WithWeek,    "DayName",     each Date.DayOfWeekName([Date]),                    type text),
    WithYearMonth   = Table.AddColumn(WithDayName, "YearMonth",   each Text.From([Year]) & "-" & Text.PadStart(Text.From([MonthNumber]),2,"0"), type text),
    WithIsWeekend   = Table.AddColumn(WithYearMonth,"IsWeekend",  each Date.DayOfWeek([Date]) >= 5,                   type logical)
in
    WithIsWeekend
