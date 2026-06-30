library(dplyr)
library(tidyr)
library(ggplot2)
library(forcats)
library(scales)

# ------------------------------------------------------------
# Settings
# ------------------------------------------------------------

max_rate_for_plot <- 25
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

decile_labels <- names(decile_cols)

# ------------------------------------------------------------
# 1. Clean data and order STPs
# ------------------------------------------------------------

df_plot <- practice_tirzepatide %>%
  filter(month == as.Date("2025-12-01")) %>%
  filter(is.finite(rate), !is.na(stp_id)) %>%
  mutate(
    rate_plot = pmin(rate, max_rate_for_plot),
    stp_id = fct_reorder(stp_id, rate_plot, median, .desc = TRUE),
    stp_y = as.numeric(stp_id)
  )

stp_levels <- levels(df_plot$stp_id)

# ------------------------------------------------------------
# 2. Create shared quantile boundaries for each STP
# ------------------------------------------------------------

rect_df <- df_plot %>%
  group_by(stp_id, stp_y) %>%
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
  arrange(stp_id, q_num) %>%
  group_by(stp_id, stp_y) %>%
  mutate(
    xmin = boundary,
    xmax = lead(boundary),
    decile_label = decile_labels[row_number()]
  ) %>%
  filter(!is.na(xmax)) %>%
  ungroup() %>%
  mutate(
    decile_label = factor(decile_label, levels = decile_labels),
    ymin = stp_y - bar_height / 2,
    ymax = stp_y + bar_height / 2
  )

# ------------------------------------------------------------
# 3. Plot
# ------------------------------------------------------------

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
    data = df_plot,
    aes(x = rate_plot, y = stp_y),
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
    breaks = seq_along(stp_levels),
    labels = stp_levels,
    expand = expansion(mult = c(0.01, 0.01))
  ) +
  scale_x_continuous(
    trans = pseudo_log_trans(sigma = 0.1),
    breaks = c(0, 0.1, 0.5, 1, 2, 5, 10, 25),
    labels = number_format(accuracy = 0.1),
    limits = c(0, max_rate_for_plot),
    expand = expansion(mult = c(0, 0.01))
  ) +
  labs(
    x = "Tirzepatide prescribing rate per 1,000",
    y = NULL,
    fill = "Within-STP decile"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    panel.grid.major.y = element_blank(),
    panel.grid.minor = element_blank(),
    legend.position = "bottom"
  )

ggsave(
  here::here(
    "output",
    "protocol",
    "practice_spread_2025-12-01.png"
  ),
  height = 11,
  width = 8.5
)
