# Functions for plots called in icb_plots.R

library(tidyverse)
library(patchwork)
library(glue)
library(scales)

# # Load data ----

# df_tirzepatide_icb_month <-
#   readRDS(here::here("data", "df_tirzepatide_icb_month.rds"))

# df_tirzepatide_icb_strength_month <-
#   readRDS(here::here("data", "df_tirzepatide_icb_strength_month.rds")) %>%
#   mutate(
#     strength = factor(
#       strength,
#       levels = c(
#         "2.5mg / 0.6ml",
#         "5mg / 0.6ml",
#         "7.5mg / 0.6ml",
#         "10mg / 0.6ml",
#         "12.5mg / 0.6ml",
#         "15mg / 0.6ml"
#       )
#     )
#   ) %>%
#   summarise(
#     .by = c(month, region, stp, total_list_size, icb_name, strength),
#     items = sum(items),
#     rateper1000 = items / total_list_size[1] * 1000
#   )

date_tirzepatide_ng <- as.Date("2024-12-23")
date_tirzepatide_diab <- as.Date("2023-10-25")

# Helper functions ----

# Formats ICB names by replacing the last space in the first n characters of a string with a given string (default is 20 and "\n"). This is useful for formatting ICB names for plotting, ensuring that the last word in the first 20 characters is separated by an underscore for better readability in plot titles or labels.
replace_last_space_firstn <- function(x, n = 24, replacement = "\n") {
  sapply(
    x,
    function(str) {
      # Get first n characters (or full string if shorter)
      prefix <- substr(str, 1, n)

      # Find positions of whitespace in that prefix
      space_positions <- gregexpr("\\s", prefix)[[1]]

      # If fewer than n char or no whitespace found, return original string
      if (nchar(str) <= n || space_positions[1] == -1) {
        return(str)
      }

      # Get last whitespace position within first n chars
      last_space <- tail(space_positions, 1)

      # Replace that whitespace
      substr(prefix, last_space, last_space) <- replacement

      # Reconstruct full string
      paste0(prefix, substr(str, n + 1, nchar(str)))
    },
    USE.NAMES = FALSE
  )
}

icb_title_name <- function(icb_names, line_split_n = 1000) {
  icb_names %>%
    str_remove_all("NHS | ICB") %>%
    str_squish() %>%
    replace_last_space_firstn(., n = 30) %>%
    gsub(
      pattern = "Nottingham and Nottinghamshire",
      replace = "Nottingham and\nNottinghamshire"
    )
}

grid_dims <- function(n) {
  stopifnot(length(n) == 1, n >= 1)

  ncol <- ceiling(sqrt(n))
  nrow <- ceiling(n / ncol)

  c(nrow = nrow, ncol = ncol)
}

theme_no_axis_labels <- function() {
  list(
    guides(color = "none"),
    theme(
      text = element_text(size = 18),
      axis.text.x = element_blank(),
      axis.ticks.x = element_blank(),
      axis.text.y = element_blank(),
      axis.ticks.y = element_blank()
    ),
    labs(x = NULL, y = NULL)
  )
}

# default palette_names = TRUE returns all or pass list names to subset (eg, get_color_palette("regions"))
get_color_palette <- function(palette_names = TRUE) {
  col_pal <-
    list(
      regions = c(
        "East of England" = "#b6e59b8c",
        "London" = "#8dabd371",
        "Midlands" = "#eeb35a38",
        "North East and Yorkshire" = "#8aebe693",
        "North West" = "#f344444d",
        "South East" = "#e7b6e1",
        "South West" = "#ffffb3"
      ),
      icb_3 = c(
        "Title ICB" = "#e84a5f",
        "ICB in region" = "#2f9599",
        "ICB other" = "#c3cbcb"
      ),
      # https://www.flerlagetwins.com/2021/06/datafam-colors-color-palette.html
      tirz_strength_5 = c(
        "2.5mg / 0.6ml" = "#e84a5f",
        "5mg / 0.6ml" = "#58cdc7",
        "7.5mg / 0.6ml" = "#ffa51d",
        "10mg / 0.6ml" = "#5bd159",
        "12.5mg / 0.6ml" = "#f7d82d",
        "15mg / 0.6ml" = "#2f9599"
      ),
      icb_summary = c(
        Mean = "#e84a5f",
        Median = "#2f9599",
        "IQR Low" = "#58cdc7",
        "IQR High" = "#58cdc7",
        ICB = "#c3cbcb"
      )
    )

  do.call(c, unname(col_pal[c(palette_names)]))
}

# Create a separate plot with the legend only
get_col_pal_legend <- function(
  palette_names,
  line_widths = NULL,
  guide_title = NULL
) {
  if (is.null(line_widths)) {
    line_widths <- 1
  }

  col_pal <- get_color_palette(palette_names)

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
        title = guide_title,
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
      legend.text = element_text(size = 28),
      legend.title = element_text(size = 28, face = "bold")
    )
}

add_date_lines <- function() {
  list(
    # geom_vline(
    #   xintercept = as.POSIXct(date_tirzepatide_diab),
    #   linetype = "dotted"
    # ),
    geom_vline(
      xintercept = as.POSIXct(date_tirzepatide_ng),
      linetype = "dashed"
    )
  )
}

# get_col_pal_legend( palette_names = "icb_3")

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

# Plotting functions

plot_by_icb_sorted_by_region_with_background <- function(
  df,
  plot_expr,
  # line_palette_name, # See list in get_color_palette for options (or set new),
  plot_region_legend = TRUE,
  plot_line_legend = TRUE,
  line_legend = NULL, # Create using get_col_pal_legend and set getter
  save_png = FALSE,
  png_filename = NULL # Must be set if save_png == TRUE
) {
  # ICB plots with one plot per ICB, with all other icb lines plotted in grey for context.

  icb_names <- df %>%
    arrange(region, icb_name) %>%
    select(region, icb_name) %>%
    distinct() %>%
    pull(icb_name)

  icb_plot_names <- sapply(icb_names, function(x) {
    glue("plot_tirzepatide_icb_{x}")
  })

  n_icbs <- length(unique(df$icb_name))
  dims <- grid_dims(n_icbs)
  nrow_plot <- unname(dims["nrow"])
  ncol_plot <- unname(dims["ncol"])

  plots <- purrr::imap(
    icb_names,
    function(icb_name, i) {
      # plot_icb_with_background_color(
      #   df = df_tirzepatide_icb_month,
      #   icb_name = icb_name,
      #   add_x_axis_text = i > n_icbs - ncol_plot
      #   add_y_axis_text = i %in% seq(1, n_icbs, ncol_plot)
      # )
      add_x_axis_text = i > n_icbs - ncol_plot
      add_y_axis_text = i %in% seq(1, n_icbs, ncol_plot)
      eval(plot_expr)
    }
  )

  plot_patchwork <- patchwork::wrap_plots(
    plotlist = plots,
    nrow = nrow_plot,
    ncol = ncol_plot
  )

  region_legend <- get_col_pal_legend(
    palette_names = "regions",
    line_widths = rep(7, 7),
    guide_title = "Region"
  )

  y_label <- patchwork_y_label()

  plot_full_layout <-
    (y_label +
      plot_patchwork +
      plot_layout(widths = c(1, 48)))

  if (plot_line_legend & plot_region_legend) {
    plot_full_layout <- plot_full_layout /
      line_legend /
      region_legend +
      plot_layout(heights = c(48, 1, 1))
  }

  if (plot_line_legend & !plot_region_legend) {
    plot_full_layout <- plot_full_layout /
      line_legend +
      plot_layout(heights = c(48, 1))
  }

  if (save_png) {
    dir.create(here::here("output", "protocol"), showWarnings = FALSE)

    ggsave(
      plot_full_layout,
      filename = png_filename,
      dpi = 100,
      width = 66,
      height = 50,
      units = "cm"
    )
  }

  plot_full_layout
}


plot_icb_with_background_color <- function(
  df,
  icb_name,
  x_axis_text = TRUE, # can take an expression
  y_axis_text = TRUE # can take an expression
) {
  icbname <- icb_name

  col_pal <- get_color_palette()

  icb_title_name <- icb_title_name(icbname, 29)

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
        icb_name == icbname ~ "Title ICB",
        region == current_region ~ "ICB in region",
        TRUE ~ "ICB other"
      )
    )

  # 1. Create plot with no axis text or labels
  plot <- ggplot(
    data = df,
    aes(x = month, y = rateper1000, group = icb_name)
  ) +
    # Plot groups in separate layers to ensure layer with focus on top, followed by same region, then other icbs
    geom_line(
      data = filter(df, color_group == "ICB other"),
      aes(color = color_group),
      alpha = 0.7
    ) +
    geom_line(
      data = filter(df, color_group %in% c("ICB in region")),
      aes(color = color_group),
      alpha = 1
    ) +
    geom_line(
      data = filter(df, color_group %in% c("Title ICB")),
      aes(color = color_group),
      linewidth = 1.2
    ) +
    add_date_lines() +
    scale_color_manual(
      values = col_pal,
      guide = "none"
    ) +
    scale_y_continuous(
      labels = label_comma(),
      limits = c(0, max(df$rateper1000, na.rm = T))
    ) +
    scale_x_date(
      breaks = plot_date_breaks,
      labels = date_format("%b %Y")
    ) +
    ggtitle(icb_title_name) +
    theme_bw() +
    theme_no_axis_labels() +
    theme(plot.background = element_rect(fill = background_color))

  # 2. Add x-axis text if set (can take an expression)
  if (eval(x_axis_text)) {
    plot <- plot +
      theme(
        axis.text.x = element_text(angle = 90, size = 22),
        axis.ticks.x = element_line()
      )
  }

  # 3. Add y-axis text if set (can take an expression)
  if (eval(y_axis_text)) {
    plot <- plot +
      theme(
        axis.text.y = element_text(size = 22),
        axis.ticks.y = element_line()
      )
  }

  return(plot)
}

plot_icb_tirzepatide_strength <- function(
  df,
  icb_name,
  x_axis_text = TRUE,
  y_axis_text = TRUE
) {
  icbname <- icb_name
  col_pal <- get_color_palette()

  icb_title_name <- icbname %>%
    str_remove_all("NHS | ICB") %>%
    str_squish() %>%
    replace_last_space_firstn(., n = 30) %>%
    gsub(
      pattern = "Nottingham and Nottinghamshire",
      replace = "Nottingham and\nNottinghamshire"
    )

  current_region <- df %>%
    filter(icb_name == icbname) %>%
    slice_head(n = 1) %>%
    pull(region)

  background_color <- col_pal[current_region]

  # 1. Create plot with no x and y axis text
  plot <- df %>%
    filter(icb_name == icbname) %>%
    ggplot() +
    geom_line(
      aes(
        x = month,
        y = rateper1000,
        color = strength
      ),
      linewidth = 1.2,
      alpha = 0.9
    ) +
    scale_y_continuous(
      labels = label_comma(),
      limits = c(0, max(df$rateper1000, na.rm = T))
    ) +
    scale_x_date(
      breaks = plot_date_breaks,
      labels = date_format("%b %Y")
    ) +
    add_date_lines() +
    scale_color_manual(values = col_pal) +
    ggtitle(icb_title_name) +
    theme_bw() +
    theme_no_axis_labels() +
    theme(plot.background = element_rect(fill = background_color))

  # 2. Add x-axis text if set (can take an expression)
  if (eval(x_axis_text)) {
    plot <- plot +
      theme(
        axis.text.x = element_text(angle = 90, size = 22),
        axis.ticks.x = element_line()
      )
  }

  # 3. Add y-axis text if set (can take an expression)
  if (eval(y_axis_text)) {
    plot <- plot +
      theme(
        axis.text.y = element_text(size = 22),
        axis.ticks.y = element_line()
      )
  }

  return(plot)
}
