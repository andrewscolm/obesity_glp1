library(tidyverse)
library(patchwork)
library(glue)
library(scales)
library(rio)

source(file.path("analysis", "functions", "plot_functions.R"))

# Set dates ----

date_tirzepatide_ng <- as.Date("2024-12-23")
date_tirzepatide_diab <- as.Date("2023-10-25")
plot_date_breaks = c(
  seq(as.Date("2023-01-01"), as.Date("2026-05-01"), "6 months"),
  as.Date("2026-05-01")
)

# Load data ----

df_regions <- rio::import(file.path("data", "op_df_regional_teams.parquet")) %>%
  mutate(
    region = tools::toTitleCase(str_to_lower(gsub(
      " COMMISSIONING REGION",
      "",
      name
    )))
  ) %>%
  select(code, region)

df_tirzepatide_icb_month <- rio::import(file.path(
  "data",
  "df_tirzepatide_icb.parquet"
)) %>%
  mutate(
    icb_name = tools::toTitleCase(str_to_lower(gsub(
      "NHS | INTEGRATED CARE BOARD",
      "",
      icb_name
    )))
  ) %>%
  left_join(df_regions, by = c("regional_team" = "code"))


df_tirzepatide_icb_month_qof <- rio::import(file.path(
  "data",
  "df_tirzepatide_icb_qof.parquet"
)) %>%
  mutate(
    icb_name = tools::toTitleCase(str_to_lower(gsub(
      "NHS | INTEGRATED CARE BOARD",
      "",
      icb_name
    )))
  ) %>%
  left_join(df_regions, by = c("regional_team" = "code"))


df_tirzepatide_icb_strength_month <- rio::import(file.path(
  "data",
  "df_tirzepatide_icb_strength.parquet"
)) %>%
  mutate(
    icb_name = tools::toTitleCase(str_to_lower(gsub(
      "NHS | INTEGRATED CARE BOARD",
      "",
      icb_name
    )))
  ) %>%
  left_join(df_regions, by = c("regional_team" = "code"))


df_tirzepatide_england_strength <- rio::import(file.path(
  "data",
  "df_tirzepatide_england_strength.parquet"
)) %>%
  mutate(
    icb_name = "",
    region = ""
  )

df_tirzepatide_england <- rio::import(file.path(
  "data",
  "df_tirzepatide_england.parquet"
)) %>%
  mutate(icb_name = "", region = "")

# Create plots ----

## Plot ICB region ----

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


## Plot ICB region strength ----

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


## Plot strength England ----
plot_by_icb_sorted_by_region_with_background(
  df = df_tirzepatide_england_strength,
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
    "tirzepitide_england_strength.png"
  )
)

## Plot England ----

plot_by_icb_sorted_by_region_with_background(
  df = df_tirzepatide_england,
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

## All england plot by ICB ----
### Prepare data with quantiles and mean ----
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

# # QOF England summary ---
# df_tirzepatide_icb_month_qof_summary <- df_tirzepatide_icb_month_qof %>%
#   mutate(type = "ICB") %>%
#   bind_rows(
#     df_tirzepatide_icb_month_qof %>%
#       summarise(
#         .by = month,
#         icb_name = "",
#         region = "",
#         median = median(rateper1000register, na.rm = TRUE),
#         iqr_low = quantile(rateper1000register, 0.25, na.rm = TRUE),
#         iqr_high = quantile(rateper1000register, 0.75, na.rm = TRUE)
#       ) %>%
#       pivot_longer(
#         cols = c(median, iqr_low, iqr_high),
#         names_to = "type",
#         values_to = "rateper1000register"
#       ) %>%
#       mutate(
#         type = recode(
#           type,
#           median = "Median",
#           iqr_low = "IQR lower",
#           iqr_high = "IQR upper"
#         )
#       )
#   )%>%
#       pivot_longer(
#         cols = c(mean, median, iqr_low, iqr_high),
#         names_to = "type",
#         values_to = "rateper1000"
#       ) %>%
#       mutate(
#         color_group = factor(
#           recode(
#             type,
#             mean = "Mean",
#             median = "Median",
#             iqr_low = "IQR Low",
#             iqr_high = "IQR High"
#           ),
#           levels = legend_levels
#         )
#       )
# levels(df_tirzepatide_icb_month_qo_summary$color_group)

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
  scale_x_date(
    breaks = plot_date_breaks,
    labels = date_format("%b %Y")
  ) +
  theme_bw() +
  theme(
    legend.title = element_blank(),
    legend.text = element_text(size = text_size),
    legend.position = "bottom",
    legend.direction = "horizontal",
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
  height = 30,
  units = "cm"
)


# QOF England plot ----
#================
# All england deciles or mean,med,quartiles plot
legend_levels <- rev(c("Mean", "Median", "IQR Low", "IQR High", "ICB"))
text_size = 20

df_tirzepatide_icb_month_qof_summary <- df_tirzepatide_icb_month_qof %>%
  mutate(
    rateper1000 = rateper1000register,
    type = "ICB",
    color_group = factor(
      "ICB",
      levels = legend_levels
    )
  ) %>%
  bind_rows(
    df_tirzepatide_icb_month_qof %>%
      mutate(rateper1000 = rateper1000register) %>%
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
levels(df_tirzepatide_icb_month_qof_summary$color_group)

icb_summary_colors <- get_color_palette("icb_summary")

summary_plot_qof <- ggplot(
  df_tirzepatide_icb_month_qof_summary,
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
  scale_x_date(
    breaks = plot_date_breaks,
    labels = date_format("%b %Y"),
    limits = c(min(plot_date_breaks), max(plot_date_breaks))
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

summary_plot_qof

ggsave(
  summary_plot_qof,
  filename = here::here("output", "protocol", "icb_england_summary_qof.png"),
  width = 40,
  height = 30,
  units = "cm"
)

# Combined England summary plot ----

summary_plots_no_legend <- summary_plot +
  theme(
    legend.position = "none",
    axis.text.x = element_blank(),
  )
combined_summary_plot <- patchwork::wrap_plots(
  plots = list(
    summary_plots_no_legend,
    summary_plot_qof
  ),
  nrow = 2,
  ncol = 1
) +
  plot_annotation(tag_levels = 'A')

combined_summary_plot

ggsave(
  combined_summary_plot,
  filename = here::here(
    "output",
    "protocol",
    "icb_england_summary_combined.png"
  ),
  width = 40,
  height = 40,
  units = "cm"
)
