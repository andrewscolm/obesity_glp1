df <- readRDS(here::here("data", "df_tirzepatide_practice_var_month"))

df <- df %>% filter(icb_name %in% unique(df$icb_name)[1:12])
