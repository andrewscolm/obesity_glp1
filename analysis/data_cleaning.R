library(dplyr)
library(readr)
library(here)
library(glue)
library(tidyr)
library(stringr)
library(scales) ## format label_number
library(gt) ## gt table
library(ggplot2)
library(lubridate) # interval
library(patchwork)
library(tibble)
library(here)

here::here()

#Load data ----
#### practice level orlistat data
df_orlistat_practice <- read_csv(here::here(
  "data",
  "orlistat_practice.csv.gz"
)) %>%
  filter(month >= as.Date("2023-01-01"))

#### practice level tirzepatide data
df_tirzepatide_practice <- read_csv(here::here(
  "data",
  "tirzepatide_practice.csv.gz"
)) %>%
  filter(month >= as.Date("2023-01-01"))


### practice statistics data
df_practice_stats <- read_csv(here::here(
  "data",
  "practice_info_full.csv.gz"
)) %>% ### as far back as 2015
  filter(month >= as.Date("2023-01-01"))

df_normalised_prescribing <- read_csv(here::here(
  "data",
  "normalised_prescribing.csv.gz"
)) %>%
  filter(month >= as.Date("2023-01-01")) %>%
  select(stp, practice, month) %>%
  drop_na(stp)

nrow(df_normalised_prescribing)
table(df_normalised_prescribing$stp, useNA = "a")


table(df_normalised_prescribing$practice, df_normalised_prescribing$month)

### ccgs info
df_hscic_ccgs <- read_csv(here::here(
  "data",
  "practice_ccgs.csv.gz"
)) %>%
  select("code", "name", "stp_id", "regional_team_id", "org_type")

### region info
df_regions <- read_csv(here::here(
  "data",
  "NHS_England_Names_and_Codes_in_England.csv"
)) %>%
  rename(regional_team = NHSER24CDH, region = NHSER24NM) %>%
  dplyr::select(regional_team, region)


### QOF Obesity prevalence
df_qof_ob <- read_csv(here::here("data", "qof", "prevalence.csv"))

### QOF regions
df_qof_region <- read_csv(here::here("data", "qof", "geography.csv"))

df_qof_icbs <- df_qof_region %>%
  filter(year == "2024-25") %>%
  group_by(icb_ods_code) %>%
  summarise(icb_ods_code = last(icb_ods_code), icb_name = last(icb_name)) %>%
  mutate(
    icb_name = gsub("Integrated Care Board", "ICB", icb_name),
    # icb_name = gsub("ICB","",icb_name),
    #   icb_name = gsub("NHS ","",icb_name),
    icb_name = str_wrap(icb_name, width = 33)
  )

# End Load data----

# Join data ----

# NEED TO ADD REGION and REGIONAL TEAM
icb_stats_clean <-
  df_practice_stats %>%
  left_join(
    df_hscic_ccgs,
    by = join_by(pct_id == code)
  ) %>%
  summarise(
    total_list_size = sum(total_list_size, na.rm = TRUE),
    .by = c(stp_id, month)
  ) %>%
  drop_na(stp_id) %>%
  left_join(df_qof_icbs, by = join_by(stp_id == icb_ods_code)) %>%
  filter(month >= as.Date("2023-01-01"))

nrow(icb_stats_clean) #1512
table(icb_stats_clean$stp_id, icb_stats_clean$month, useNA = "a")
names(icb_stats_clean)


## Clean Orlistat ----

orlistat_clean <- df_orlistat_practice %>%
  mutate(
    year = lubridate::year(month),
    strength = gsub("\\D", "", bnf_name),
    strength = glue("{strength}mg")
  ) %>%
  left_join(
    icb_stats_clean,
    by = c("stp" = "stp_id", "month")
  ) %>%
  summarise(
    stp = last(stp),
    regional_team = last(regional_team),
    items = sum(items),
    list_size = last(total_list_size),
    rateper1000 = (items) / list_size * 1000,
    .by = c(stp, year, month, strength, total_list_size)
  )

names(orlistat_clean)
tapply(orlistat_clean$rateper1000, orlistat_clean$strength, summary)

table(orlistat_clean$stp, orlistat_clean$year, useNA = "a")

# ## Clean Tirzepatide ----

# tirzepatide_clean <- df_tirzepatide_practice %>%
#   mutate(
#     strength = gsub("[^0-9/.]", "", bnf_name),
#     strength = gsub("2.4", "", strength),
#     strength = gsub("/", "mg / ", strength),
#     strength = glue("{strength}ml")
#   ) %>%
#   filter(
#     strength %in%
#       c(
#         "5mg / 0.6ml",
#         "7.5mg / 0.6ml",
#         "10mg / 0.6ml",
#         "2.5mg / 0.6ml",
#         "15mg / 0.6ml",
#         "12.5mg / 0.6ml"
#       )
#   ) %>%
#   # Q: Why does orlistat have practice but not df_tirzepatide_practice, can we fix?
#   left_join(
#     df_normalised_prescribing,
#     by = c("month", "practice")
#   ) %>%
#   left_join(
#     icb_stats_clean,
#     by = c("stp" = "stp_id", "month")
#   )

# t <- Sys.time()
# d <- tirzepatide_clean %>%
#   summarize(
#     .by = c(month, icb_name, regional_team, total_list_size, ),
#     items = sum(items),
#     rateper1000 = items / last(total_list_size) * 1000
#   )
# Sys.time() - t
# summary(d$rateper1000)
