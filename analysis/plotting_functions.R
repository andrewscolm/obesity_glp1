library(paletter)
library(tidyverse)


get_light_palette <- function() {
  df_regions <- read_csv(here::here(
    "data",
    "NHS_England_Names_and_Codes_in_England.csv"
  )) %>%
    rename(regional_team = NHSER24CDH, region = NHSER24NM) %>%
    dplyr::select(regional_team, region) %>%
    mutate(
      col_fill = c(
        "#8dabd371",
        "#e7b6e1",
        "#ffffb3",
        "#eeb35a38",
        "#8aebe693",
        "#b6e59b8c",
        "#f344444d"
      )
    ) %>%
    arrange(region)

  deframe(df_regions[, c(2, 3)])
}

# Create a separate plot with the legend only
get_icb_with_regional_background_legend <- function(col_pal) {
  tibble(
    legend_label = factor(
      names(col_pal),
      levels = names(col_pal)
    )
  ) %>%
    ggplot(aes(x = 1, y = legend_label, color = legend_label)) +
    geom_line() +
    scale_color_manual(
      values = col_pal,
      guide = guide_legend(
        title = "",
        direction = "horizontal",
        nrow = 1,
        byrow = TRUE,
        override.aes = list(alpha = 1, linewidth = 1.8)
      )
    ) +
    theme_void() +
    theme(
      legend.position = "bottom",
      legend.direction = "horizontal",
      legend.box = "horizontal",
      legend.text = element_text(size = 22)
    )
}


plot_by_icb_sorted_by_region_with_background <- function(df) {
  # ICB plots with one plot per ICB, with all other icb lines plotted in grey for context.

  # 42 ICBs, so 6 rows and 7 columns for patchwork
  n_icbs <- 42
  nrows_icb_plot = 6
  # ICB names arranged alphabetically by region

  icb_names <- df %>%
    arrange(region, icb_name) %>%
    select(region, icb_name) %>%
    distinct() %>%
    pull(icb_name)

  icb_plot_names <- sapply(icb_names, function(x) {
    glue("plot_tirzepatide_icb_{x}")
  })

  region_palette_light <- get_light_palette()

  plots <- purrr::map(icb_names, function(icb_name) {
    plot_icb_with_background_color(
      df = df_tirzepatide_practice_month,
      icb_name = icb_name,
      col_pal = region_palette_light
    )
  })

  plot_tirzepatide_icb_patchwork <- patchwork::wrap_plots(
    plotlist = plots,
    nrow = nrows_icb_plot,
    ncol = ncols_icb_plot
  )

  region_legend <- get_icb_with_regional_background_legend(region_palette_light)

  y_label <- ggplot() +
    annotate(
      "text",
      x = 0.5,
      y = 0.5,
      label = "Rate per 1000 list size",
      angle = 90,
      size = 9
    ) +
    theme_void()

  plot_tirzepatide_icb_patchwork_with_legend_y_axis <-
    (y_label +
      plot_tirzepatide_icb_patchwork +
      plot_layout(widths = c(1, 48))) /
    plot_tirzepatide_icb_legend +
    plot_layout(heights = c(48, 1))
}


plot_icb_with_background_color <- function(df, icb_name, col_pal) {
  icbname <- icb_name

  #1. Create main plot

  icb_title_name <- icbname %>%
    str_remove_all("NHS | ICB") %>%
    str_squish() %>%
    replace_last_space_firstn(., n = 29)

  current_region <- df %>%
    filter(icb_name == icbname) %>%
    slice_head(n = 1) %>%
    pull(region)

  df <- df %>%
    mutate(
      priority = factor(
        case_when(
          icb_name == icbname ~ 1,
          region == current_region ~ 2,
          TRUE ~ 3
        ),
        levels = c(1, 2, 3)
      ),
      color_group = case_when(
        icb_name == icbname ~ "focus",
        region == current_region ~ "same_region",
        TRUE ~ "other"
      )
    )

  background_color = col_pal[current_region]

  ggplot(
    data = df,
    aes(x = month, y = rateper1000, group = icb_name)
  ) +
    # Plot groups in separate layers to allow different aesthetics for each group and to layer with focus on top, followed by same region.
    geom_line(
      data = filter(df, color_group %in% c("other")),
      aes(color = color_group, linewidth = priority, alpha = priority)
    ) +
    geom_line(
      data = filter(df, color_group %in% c("same_region")),
      aes(color = color_group, linewidth = priority, alpha = priority)
    ) +
    geom_line(
      data = filter(df, color_group == "focus"),
      aes(color = color_group, linewidth = priority, alpha = priority)
    ) +
    scale_color_manual(
      values = c(
        "focus" = "dodgerblue3",
        # "same_region" = region_icb_plot_colors[[current_region]],
        "same_region" = "violetred3",
        "other" = "grey80"
      ),
      guide = "none"
    ) +
    scale_linewidth_manual(
      values = c("1" = 1.1, "2" = 0.9, "3" = 0.7),
      guide = "none"
    ) +
    scale_alpha_manual(
      values = c("1" = 1, "2" = 0.6, "3" = 0.7),
      guide = "none"
    ) +
    theme_bw() +
    scale_y_continuous(
      labels = label_comma(),
      limits = c(0, max_y_tirzepatide)
    ) +
    ggtitle(icb_title_name) +
    theme(
      plot.background = element_rect(fill = background_color),
      text = element_text(size = 15),
      axis.text.x = element_blank(),
      axis.ticks.x = element_blank(),
      axis.text.y = element_blank(),
      axis.ticks.y = element_blank()
    ) +
    xlab("") +
    ylab("")
}
