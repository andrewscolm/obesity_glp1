library(tidyverse)
library(readxl)
library(here)

# QOF archives by year - easy to add more datasets later
qof_sources <- list(
  "2024-25" = list(
    url = "https://files.digital.nhs.uk/95/4708D7/QOF2425.zip",
    datasets = list(
      prevalence = "PREVALENCE_2425.csv",
      geography = "MAPPING_NHS_GEOGRAPHIES_2425.csv"
    )
  ),
  "2023-24" = list(
    url = "https://files.digital.nhs.uk/DA/975A29/QOF2324.zip",
    datasets = list(
      prevalence = "PREVALENCE_2324.csv",
      geography = "MAPPING_NHS_GEOGRAPHIES_2324.csv"
    )
  ),
  "2022-23" = list(
    url = "https://files.digital.nhs.uk/32/40B931/QOF%202022-23%20Raw%20data%20.csv%20files.zip",
    datasets = list(
      prevalence = "PREVALENCE_2223.csv",
      geography = "MAPPING_NHS_GEOGRAPHIES_2223.csv"
    )
  ),
  "2021-22" = list(
    url = "https://files.digital.nhs.uk/90/6F833F/QOF_2122_V2.zip",
    datasets = list(
      prevalence = "PREVALENCE_2122_V2.csv",
      geography = "MAPPING_NHS_GEOGRAPHIES_2122.csv"
    )
  ),
  "2020-21" = list(
    url = "https://files.digital.nhs.uk/AC/3C964F/QOF2021_v2.zip",
    datasets = list(
      prevalence = "PREVALENCE_2021_v2.csv",
      geography = "MAPPING_NHS_GEOGRAPHIES_2021.csv"
    )
  ),
  "2019-20" = list(
    url = "https://files.digital.nhs.uk/E2/BF16AA/QOF_1920.zip",
    datasets = list(
      prevalence = "PREVALENCE_1920.csv",
      geography = "MAPPING_NHS_GEOGRAPHIES_1920.csv"
    )
  )
  # "2018-19" = list(
  #   url = "https://files.digital.nhs.uk/A8/491BAC/QOF_1819_v2.zip",
  #   datasets = list(
  #     prevalence = "PREVALENCE_1819_v2.csv",
  #     geography = "ORGANISATION_REFERENCE_1819_v2.csv"
  #   )
  # ),
  # "2017-18" = list(
  #   url = "https://files.digital.nhs.uk/72/CB2869/qof-1718-csv.zip",
  #   datasets = list(
  #     prevalence = "PREVALENCE.csv",
  #     geography = "ORGANISATION_REFERENCE.csv"
  #   )
  # ),
  # "2016-17" = list(
  #   url = "https://files.digital.nhs.uk/zip/p/i/qof-1617-csv.zip",
  #   datasets = list(
  #     prevalence = "PREVALENCE.csv",
  #     geography = "ORGANISATION_REFERENCE.csv"
  #   )
  # ),
  # "2015-16" = list(
  #   url = "https://files.digital.nhs.uk/publicationimport/pub22xxx/pub22266/qof-1516-csv.zip",
  #   datasets = list(
  #     prevalence = "PREVALENCE.csv",
  #     geography = "ORGANISATION_REFERENCE.csv"
  #   )
  # ),
  # "2014-15" = list(
  #   url = "https://files.digital.nhs.uk/publicationimport/pub18xxx/pub18887/qof-1415-csvfiles-v2.zip",
  #   datasets = list(
  #     prevalence = "PREVALENCE_BY_PRAC_v2.csv",
  #     geography = "PRAC_CONTROL.csv"
  #   )
  # ),
  # "2013-14" = list(
  #   url = "https://files.digital.nhs.uk/publicationimport/pub15xxx/pub15751/qof-1314-csvfilescqrsdata.zip",
  #   datasets = list(
  #     prevalence = "prevalencebyprac.csv",
  #     geography = "Prac_Control.csv"
  #   )
  # )
)

# List all files in a zip archive (helper to find correct filenames)
list_zip_contents <- function(url) {
  temp_zip <- tempfile(fileext = ".zip")
  on.exit(unlink(temp_zip))
  download.file(url, temp_zip, mode = "wb", quiet = TRUE)
  unzip(temp_zip, list = TRUE)$Name
}

# List contents of all QOF zip archives
list_all_qof_files <- function() {
  imap(
    qof_sources,
    ~ {
      message("Fetching: ", .y)
      list_zip_contents(.x$url)
    }
  )
}

# Run this to see all available files:
# qof_files <- list_all_qof_files()
# qof_files

# Read multiple CSVs from one zip
read_csvs_from_zip <- function(url, datasets, year) {
  temp_zip <- tempfile(fileext = ".zip")
  temp_dir <- tempfile()
  on.exit(unlink(c(temp_zip, temp_dir), recursive = TRUE))

  download.file(url, temp_zip, mode = "wb", quiet = TRUE)
  unzip(temp_zip, exdir = temp_dir)

  map(
    datasets,
    ~ {
      read_csv(
        file.path(temp_dir, .x),
        name_repair = janitor::make_clean_names,
        show_col_types = FALSE
      ) |>
        mutate(year = year, .before = 1)
    }
  )
}

# Read all years, returns nested list: year -> dataset -> data
qof_raw <- imap(
  qof_sources,
  ~ read_csvs_from_zip(.x$url, .x$datasets, .y)
)

# Check column names across all years
map(qof_raw, ~ map(.x, names))

# Prevalence: column renames (new_name = "old_name")
prevalence_col_renames <- c(
  practice_code = "practicecode",
  group_code = "indicator_group",
  group_code = "indicator_group_code",
  register = "disease_register_size",
  register = "register_size",
  practice_list_size = "practice_listsize",
  practice_list_size = "patient_list_size"
)

# Prevalence: columns to keep
prevalence_keep_cols <- c(
  "year",
  "practice_code",
  "group_code",
  "register",
  "practice_list_size"
)

# Geography: column renames (new_name = "old_name")
geography_col_renames <- c(
  practice_code = "practicecode",
  practice_name = "practicename",
  region_ods_code = "regioncode",
  region_ods_code = "region_code",
  region_name = "regionname",
  icb_ods_code = "stp_ods_code",
  icb_name = "stp_name"
)

# Geography: columns to keep
geography_keep_cols <- c(
  "year",
  "practice_code",
  "practice_name",
  "region_ods_code",
  "region_name",
  "icb_ods_code",
  "icb_name"
)

# Dataset-specific configs
dataset_configs <- list(
  prevalence = list(
    col_renames = prevalence_col_renames,
    keep_cols = prevalence_keep_cols
  ),
  geography = list(
    col_renames = geography_col_renames,
    keep_cols = geography_keep_cols
  )
)

# Standardise column names for a specific dataset type
standardise_dataset <- function(df, col_renames, keep_cols) {
  df |>
    rename(any_of(col_renames)) |>
    select(all_of(keep_cols))
}

# Transpose: year -> dataset becomes dataset -> year
qof_by_dataset <- qof_raw |>
  list_transpose()

# Apply standardisation to each dataframe using dataset-specific configs
qof_standardised <- imap(
  qof_by_dataset,
  ~ {
    config <- dataset_configs[[.y]]
    map(.x, ~ standardise_dataset(.x, config$col_renames, config$keep_cols))
  }
)

# Combine years within each dataset
qof_data <- qof_standardised |>
  map(list_rbind)

# Filter for heart failure
qof_prevalence <- qof_data$prevalence |>
  filter(group_code == "OB")

write_csv(
  qof_prevalence,
  here("data", "qof", "prevalence.csv")
)

qof_data$geography |>
  # group_by(year) |>
  # select(region_name) |>
  # distinct() |>
  # arrange(desc(year), region_name) |>
  write_csv(
    here("data", "qof", "geography.csv")
  )