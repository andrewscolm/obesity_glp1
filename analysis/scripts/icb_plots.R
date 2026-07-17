library(tidyverse)
library(patchwork)
library(glue)
library(scales)

# Load data ----

df_tirzepatide_icb_month <-
  readRDS(here::here("data", "df_tirzepatide_icb_month.rds"))

df_tirzepatide_icb_strength_month <-
  readRDS(here::here("data", "df_tirzepatide_icb_strength_month.rds")) %>%
  mutate(
    strength = factor(
      strength,
      levels = c(
        "2.5mg / 0.6ml",
        "5mg / 0.6ml",
        "7.5mg / 0.6ml",
        "10mg / 0.6ml",
        "12.5mg / 0.6ml",
        "15mg / 0.6ml"
      )
    )
  ) %>%
  summarise(
    .by = c(month, region, stp, total_list_size, icb_name, strength),
    items = sum(items),
    rateper1000 = items / total_list_size[1] * 1000
  )

date_tirzepatide_ng <- as.Date("2024-12-23")
date_tirzepatide_diab <- as.Date("2023-10-25")


# Create plots ----

prev_diff <- diff
t <- Sys.time()
plot_by_icb_sorted_by_region_with_background(
  df = df_tirzepatide_icb_month,
  plot_expr = expr(
    plot_icb_with_background_color(
      df = df,
      icb_name = icb_name,
      x_axis_text = add_x_axis_text,
      y_axis_text = add_y_axis_text
    )
  ),
  line_legend = get_col_pal_legend(palette_names = "icb_3"),
  save_png = T,
  png_filename = here::here("output", "protocol", "tirzepitide_icb_region.png")
)

diff <- Sys.time() - t
prev_diff
diff

# ============
prev_diff <- diff
t <- Sys.time()
plot_by_icb_sorted_by_region_with_background(
  df = df_tirzepatide_icb_strength_month,
  plot_expr = expr(
    plot_icb_tirzepatide_strength(
      df = df,
      icb_name = icb_name,
      x_axis_text = add_x_axis_text,
      y_axis_text = add_y_axis_text
    )
  ),
  line_legend = get_col_pal_legend(palette_names = "tirz_strength_5"),
  save_png = T,
  png_filename = here::here(
    "output",
    "protocol",
    "tirzepitide_icb_region_strength.png"
  )
)
diff <- Sys.time() - t
prev_diff
diff

=====================

# Plot by strength
df_tirzepatide_month <- df_tirzepatide_icb_month %>%
  summarise(
    .by = c(month),
    icb_name = "",
    region = "",
    items = sum(items),
    total_list_size = sum(total_list_size),
    rateper1000 = items / total_list_size * 1000
    # check_rows = n(),
    # check_icb_names = length(unique(icb_names)),
    # check = check_rows == check_icb_names
  )

# Plot all England
plot_by_icb_sorted_by_region_with_background(
  df = df_tirzepatide_month,
  plot_expr = expr(
    plot_icb_with_background_color(
      df = df,
      icb_name = icb_name,
      x_axis_text = add_x_axis_text,
      y_axis_text = add_y_axis_text
    )
  ),
  plot_line_legend = F,
  plot_region_legend = F,
  save_png = T,
  png_filename = here::here(
    "output",
    "protocol",
    "tirzepitide_icb_region_total.png"
  )
)

# #================
# All england plot by ICB
df_tirzepatide_icb_month_summary <- df_tirzepatide_icb_month %>%
  mutate(type = "ICB") %>%
  bind_rows(
    df_tirzepatide_icb_month %>%
      summarise(
        .by = month,
        icb_name = "",
        region = "",
        median = median(rateper1000, na.rm = TRUE),
        iqr_low = quantile(rateper1000, 0.25, na.rm = TRUE),
        iqr_high = quantile(rateper1000, 0.75, na.rm = TRUE)
      ) %>%
      pivot_longer(
        cols = c(median, iqr_low, iqr_high),
        names_to = "type",
        values_to = "rateper1000"
      ) %>%
      mutate(
        type = recode(
          type,
          median = "Median",
          iqr_low = "IQR lower",
          iqr_high = "IQR upper"
        )
      )
  )

#================
# All england deciles or mean,med,quartiles plot
legend_levels <- rev(c("Mean", "Median", "IQR Low", "IQR High", "ICB"))
text_size = 20

df_tirzepatide_icb_month_summary <- df_tirzepatide_icb_month %>%
  mutate(
    type = "ICB",
    color_group = factor(
      "ICB",
      levels = legend_levels
    )
  ) %>%
  bind_rows(
    df_tirzepatide_icb_month %>%
      summarise(
        .by = month,
        icb_name = "",
        region = "",
        mean = mean(rateper1000),
        median = median(rateper1000),
        iqr_low = quantile(rateper1000, 0.25),
        iqr_high = quantile(rateper1000, 0.75)
      ) %>%
      pivot_longer(
        cols = c(mean, median, iqr_low, iqr_high),
        names_to = "type",
        values_to = "rateper1000"
      ) %>%
      mutate(
        color_group = factor(
          recode(
            type,
            mean = "Mean",
            median = "Median",
            iqr_low = "IQR Low",
            iqr_high = "IQR High"
          ),
          levels = legend_levels
        )
      )
  )
levels(df_tirzepatide_icb_month_summary$color_group)

icb_summary_colors <- get_color_palette("icb_summary")

summary_plot <- ggplot(
  df_tirzepatide_icb_month_summary,
  aes(x = month, y = rateper1000, group = interaction(icb_name, color_group))
) +
  geom_line(
    aes(
      color = color_group,
      linetype = color_group,
      linewidth = color_group,
      alpha = color_group
    ),
  ) +
  add_date_lines() +
  scale_color_manual(
    values = icb_summary_colors,
  ) +
  scale_linetype_manual(
    values = c(
      "Mean" = 1,
      "Median" = 1,
      "IQR Low" = 2,
      "IQR High" = 2,
      "ICB" = 1
    ),
  ) +
  scale_linewidth_manual(
    values = c(
      "Mean" = 1.1,
      "Median" = 1.2,
      "IQR Low" = 1.1,
      "IQR High" = 1.1,
      "ICB" = 1
    )
  ) +
  scale_alpha_manual(
    values = c(
      "Mean" = 0.8,
      "Median" = 0.8,
      "IQR Low" = 1,
      "IQR High" = 1,
      "ICB" = 0.4
    )
  ) +
  theme_bw() +
  theme(
    legend.title = element_blank(),
    legend.text = element_text(size = text_size),
    legend.position = "bottom",
    legend.direction = "horizontal",
    legend.box = "horizontal",
    axis.text = element_text(size = text_size),
    axis.title.y = element_text(size = text_size)
  ) +
  guides(
    color = guide_legend(reverse = TRUE),
    linetype = guide_legend(reverse = TRUE),
    linewidth = guide_legend(reverse = TRUE),
    alpha = guide_legend(reverse = TRUE)
  ) +
  xlab("") +
  ylab("Rate per 1000 registered patients")

summary_plot

ggsave(
  summary_plot,
  filename = here::here("output", "protocol", "icb_england_summary.png"),
  width = 40,
  height = 25,
  units = "cm"
)
