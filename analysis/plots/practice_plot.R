library(dplyr)
library(tidyr)
library(ggplot2)
library(forcats)
library(scales)
library(rio)

# Import data ----
df_tirzepatide_practice <- rio::import(file.path(
  "data",
  "df_tirzepatide_practice.parquet"
)) %>%
  filter(month == as.Date("2025-05-01")) %>%
  mutate(
    icb_name = as.factor(tools::toTitleCase(str_to_lower(gsub(
      "NHS | INTEGRATED CARE BOARD",
      "",
      icb_name
    )))),
    rate_plot = rateper1000, #pmin(rateper1000, max_rate_for_plot),
    icb_name = fct_reorder(icb_name, rate_plot, .desc = TRUE),
    icb_y = as.numeric(icb_name)
  )


# Settings ----

# max_rate_for_plot <- 25
max_rate_for_plot <- max(df_tirzepatide_practice$rateper1000)
bar_height <- 0.55
jitter_height <- 0.08

decile_cols <- c(
  "0-10%" = "white",
  "10-20%" = "#E1F0F8",
  "20-30%" = "#C8E4F3",
  "30-40%" = "#9BCDE8",
  "40-50%" = "#5A9DCA",
  "50-60%" = "#5A9DCA",
  "60-70%" = "#9BCDE8",
  "70-80%" = "#C8E4F3",
  "80-90%" = "#E1F0F8",
  "90-100%" = "white"
)


# 2. Create shared quantile boundaries for each icb ----

rect_df <- df_tirzepatide_practice %>%
  group_by(icb_name, icb_y) %>%
  summarise(
    q = list(quantile(
      rate_plot,
      probs = seq(0, 1, by = 0.1),
      na.rm = TRUE,
      names = FALSE,
      type = 7
    )),
    .groups = "drop"
  ) %>%
  unnest_wider(q, names_sep = "_") %>%
  rename(
    q0 = q_1,
    q10 = q_2,
    q20 = q_3,
    q30 = q_4,
    q40 = q_5,
    q50 = q_6,
    q60 = q_7,
    q70 = q_8,
    q80 = q_9,
    q90 = q_10,
    q100 = q_11
  ) %>%
  pivot_longer(
    cols = starts_with("q"),
    names_to = "q_name",
    values_to = "boundary"
  ) %>%
  mutate(
    q_num = as.numeric(gsub("q", "", q_name))
  ) %>%
  arrange(icb_name, q_num) %>%
  group_by(icb_name, icb_y) %>%
  mutate(
    xmin = boundary,
    xmax = lead(boundary),
    decile_label = decile_labels[row_number()]
  ) %>%
  filter(!is.na(xmax)) %>%
  ungroup() %>%
  mutate(
    decile_label = factor(decile_label, levels = decile_labels),
    ymin = icb_y - bar_height / 2,
    ymax = icb_y + bar_height / 2
  )

# 3. Plot ----

decile_labels <- names(decile_cols)
icb_levels <- levels(df_tirzepatide_practice$icb_name)

ggplot() +
  geom_rect(
    data = rect_df,
    aes(
      xmin = xmin,
      xmax = xmax,
      ymin = ymin,
      ymax = ymax,
      fill = decile_label
    ),
    alpha = 0.75,
    color = NA
  ) +
  geom_point(
    data = df_tirzepatide_practice,
    aes(x = rate_plot, y = icb_y),
    position = position_jitter(height = jitter_height, width = 0),
    size = 0.6,
    alpha = 0.35,
    color = "black"
  ) +
  scale_fill_manual(
    values = decile_cols,
    breaks = decile_labels,
    drop = FALSE
  ) +
  scale_y_continuous(
    breaks = seq_along(icb_levels),
    labels = icb_levels,
    expand = expansion(mult = c(0.01, 0.01))
  ) +
  scale_x_continuous(
    trans = pseudo_log_trans(sigma = 0.1),
    breaks = c(0, 0.1, 0.5, 1, 2, 5, 10, 20, 30),
    labels = number_format(accuracy = 0.1),
    # limits = c(0, max_rate_for_plot),
    expand = expansion(mult = c(0, 0.05))
  ) +
  labs(
    x = "Tirzepatide prescribing rate per 1000",
    y = NULL,
    fill = "Within-icb decile"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.major.y = element_blank(),
    panel.grid.minor = element_blank(),
    legend.position = "bottom",
    legend.title.position = "top",
  )

ggsave(
  here::here(
    "output",
    "protocol",
    "practice_spread_2025-05-01.png"
  ),
  height = 7,
  width = 8
)
