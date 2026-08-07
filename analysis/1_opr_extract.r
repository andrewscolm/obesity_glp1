library(opr)
library(dplyr)
library(arrow)


con <- connect_bq()


source(here::here("analysis", "design", "design.r"))
source(here::here("analysis", "functions", "utils.r"))

fs::dir_create(here("data"))

df_ccgs <- tbl(con, "ccgs") |>
  collect()

bnf_query <- tbl(con, "bnf")

df_bnf_tirzepatide <- filter_like_or_in(
  bnf_query,
  con,
  "presentation_code",
  "0601023AZ%"
) |>
  select(year_month, presentation_code, chemical) |>
  collect()


df_bnf_orlistat <- filter_like_or_in(
  bnf_query,
  con,
  "presentation_code",
  "0405010P0%"
) |>
  select(year_month, presentation_code, chemical) |>
  collect()

df_tirzepatide <- get_normalised_prescribing(
  con,
  bnf_codes = "0601023AZ%",
  start_date = start_date,
  end_date = end_date
) |>
  select(-ends_with("_cost"), -quantity) |>
  collect()

df_orlistat <- get_normalised_prescribing(
  con,
  bnf_codes = "0405010P0%",
  start_date = start_date,
  end_date = end_date
) |>
  select(-ends_with("_cost"), -quantity) |>
  collect()


df_gp_practices <- get_practices(
  con,
  add_setting_labels = TRUE,
  filter_setting = 4,
  add_status_code_labels = TRUE
) |>
  select(
    code,
    name,
    ccg_id,
    setting,
    setting_label,
    close_date,
    join_provider_date,
    leave_provider_date,
    open_date,
    status_code,
    status_code_label
  ) |>
  collect()

df_practice_statistics <- dplyr::tbl(con, "practice_statistics") |>
  filter(month >= start_date, month <= end_date) |>
  select(month, practice, total_list_size) |>
  collect()


df_regional_teams <- tbl(con, "regional_teams") |>
  filter(is.na(close_date)) |>
  collect()

df_stp_names <- tbl(con, "stps") |>
  collect()

### write as parquet

write_parquet(
  df_ccgs,
  here("data", "df_ccgs.parquet")
)

write_parquet(
  df_bnf_orlistat,
  here("data", "df_bnf_orlistat.parquet")
)

write_parquet(
  df_bnf_tirzepatide,
  here("data", "df_bnf_tirzepatide.parquet")
)

write_parquet(
  df_tirzepatide,
  here("data", "df_tirzepatide.parquet")
)

write_parquet(
  df_orlistat,
  here("data", "df_orlistat.parquet")
)

write_parquet(
  df_gp_practices,
  here("data", "df_gp_practices.parquet")
)

write_parquet(
  df_practice_statistics,
  here("data", "df_practice_statistics.parquet")
)

write_parquet(
  df_regional_teams,
  here("data", "df_regional_teams.parquet")
)

write_parquet(
  df_stp_names,
  here("data", "df_stp_names.parquet")
)
