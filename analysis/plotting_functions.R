library(tidyverse)

df_tirzepatide_practice_month <- readRDS(
  here::here("data", "df_tirzepatide_practice_month.rds")
)


theme_empty_icb <- theme_bw() +
  theme(
    text = element_text(size = 15),
    axis.text.x = element_blank(),
    axis.ticks.x = element_blank(),
    axis.text.y = element_blank(),
    axis.ticks.y = element_blank()
  )

get_region_light_palette <- function(list_names) {
  stop() # turn into named list
  col_pal <- c(
    "East of England" = "#b6e59b8c",
    "London" = "#8dabd371",
    "Midlands" = "#eeb35a38",
    "North East and Yorkshire" = "#8aebe693",
    "North West" = "#f344444d",
    "South East" = "#e7b6e1",
    "South West" = "#ffffb3"
  )

  if (!regions_only) {
    col_pal <- c(
      "Current ICB" = "violetred3",
      "ICB in region" = "dodgerblue3",
      "ICB other" = "grey80",
      col_pal
    )
  }

  return(col_pal)
}

# Create a separate plot with the legend only
get_col_pal_legend <- function(
  col_pal,
  line_widths = NULL
) {
  if (is.null(line_widths)) {
    line_widths <- 1
  }
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
        override.aes = list(alpha = 1, linewidth = line_widths)
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

patchwork_y_label <- function(text = "Rate per 1000 registered patients") {
  ggplot() +
    annotate(
      "text",
      x = 0.5,
      y = 0.5,
      label = text,
      angle = 90,
      size = 9
    ) +
    theme_void()
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

  background_color <- col_pal[current_region]

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
        icb_name == icbname ~ "Current ICB",
        region == current_region ~ "ICB in region",
        TRUE ~ "ICB other"
      )
    )

  ggplot(
    data = df,
    aes(x = month, y = rateper1000, group = icb_name)
  ) +
    # Plot groups in separate layers to allow different aesthetics for each group and to layer with focus on top, followed by same region.
    geom_line(
      data = filter(df, color_group == "ICB other"),
      aes(color = color_group),
      alpha = 0.6
    ) +
    geom_line(
      data = filter(df, color_group %in% c("ICB in region")),
      aes(color = color_group),
      alpha = 0.8
    ) +
    geom_line(
      data = filter(df, color_group %in% c("Current ICB")),
      aes(color = color_group),
      linewidth = 1.2
    ) +
    scale_color_manual(
      values = col_pal,
      guide = "none"
    ) +
    scale_y_continuous(
      labels = label_comma(),
      limits = c(0, max(df$rateper1000, na.rm = T))
    ) +
    ggtitle(icb_title_name) +
    theme_empty_icb +
    theme(plot.background = element_rect(fill = background_color)) +
    xlab("") +
    ylab("")
}

plot_icb_tirzepatide_strength <- function(df, icb_name, col_pal) {
  icbname <- icb_name

  icb_title_name <- icbname %>%
    str_remove_all("NHS | ICB") %>%
    str_squish() %>%
    replace_last_space_firstn(., n = 29)

  current_region <- df %>%
    filter(icb_name == icbname) %>%
    slice_head(n = 1) %>%
    pull(region)

  background_color <- col_pal[current_region]

  df_tirzepatide_practice_strength_month %>%
    filter(icb_name == icbname) %>%
    ggplot() +
    geom_vline(
      xintercept = as.POSIXct(date_tirzepatide_diab),
      linetype = "dashed"
    ) +
    geom_vline(
      xintercept = as.POSIXct(date_tirzepatide_ng),
      linetype = "dotted"
    ) +
    geom_line(aes(x = month, y = rateper1000, color = strength)) +
    scale_y_continuous(labels = label_comma()) +
    guides(color = "none") +
    ggtitle(icb_title_name) +
    xlab("") +
    ylab("") +
    theme_empty_icb +
    theme(plot.background = element_rect(fill = background_color))
}

plot_by_icb_sorted_by_region_with_background <- function(
  df,
  plot_expr,
  save_png = FALSE
) {
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

  region_light_palette <- get_region_light_palette()

  plots <- purrr::map(
    icb_names,
    function(icb_name) {
      # plot_icb_with_background_color(
      #   df = df_tirzepatide_practice_month,
      #   icb_name = icb_name,
      #   col_pal = col_pal
      # )
      eval(plot_expr)
    }
  )

  plot_patchwork <- patchwork::wrap_plots(
    plotlist = plots,
    nrow = nrows_icb_plot,
    ncol = ncols_icb_plot
  )

  icb_region_legend <- get_col_pal_legend(
    region_light_palette(list_names = legend_list_names)
  )

  y_label <- patchwork_y_label()

  plot_full_layout <-
    (y_label +
      plot_patchwork +
      plot_layout(widths = c(1, 48))) /
    icb_region_legend +
    plot_layout(heights = c(48, 1))

  if (save_png) {
    dir.create(here::here("output", "protocol"), showWarnings = FALSE)

    ggsave(
      plot_full_layout,
      filename = here::here("output", "protocol", "tirzepitide_icb_region.png"),
      dpi = 100,
      width = 80,
      height = 50,
      units = "cm"
    )
  }

  plot_full_layout
}

prev_diff <- diff
t <- Sys.time()
plot_by_icb_sorted_by_region_with_background(
  df = df_tirzepatide_practice_month,
  plot_expr = expr(
    plot_icb_with_background_color(
      df = df_tirzepatide_practice_month,
      icb_name = icb_name,
      col_pal = col_pal
    )
  ),
  save_png = F
)

diff <- Sys.time() - t

prev_diff
diff


# ============
prev_diff <- diff
t <- Sys.time()
plot_by_icb_sorted_by_region_with_background(
  df = df_tirzepatide_practice_month,
  plot_expr = expr(
    plot_icb_tirzepatide_strength(
      df = df_tirzepatide_practice_strength_month,
      icb_name = icb_name,
      col_pal = col_pal
    )
  ),
  save_png = F
)

diff <- Sys.time() - t

prev_diff
diff
