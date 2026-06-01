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
