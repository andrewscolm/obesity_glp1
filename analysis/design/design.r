# dates of the NICE guidances for tirzepatide
date_tirzepatide_ng <- as.Date("2024-12-23")
date_tirzepatide_diab <- as.Date("2023-10-25")

# study dates
start_date <- as.Date("2023-01-01")
end_date <- as.Date("2026-05-01")
start_date_qof <- as.Date("2025-04-01")
end_date_qof <- as.Date("2026-03-01")
rollout_start_date <- as.Date("2025-06-01")

icb_name_pretty <- function(icb_names) {
  icb_names %>%
    str_squish() %>%
    str_to_title() |>
    gsub(
      pattern = "Integrated Care Board",
      replace = "ICB"
    ) |>
    gsub(
      pattern = "Icb",
      replace = "ICB"
    ) |>
    gsub(
      pattern = " And",
      replace = " &"
    ) |>
    gsub(
      pattern = "Nhs",
      replace = "NHS"
    )
}
