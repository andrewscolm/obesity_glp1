plot_qof_ob_icb <- df_qof_ob_icb %>%
  filter(measure == "register") %>%
  ggplot(aes(x = month, y = value)) +
  facet_wrap(vars(icb_name)) +
  geom_line() +
  scale_y_continuous(labels = label_comma()) +
  theme(axis.text.x = element_text(angle = 90))

ggsave(
  filename = here::here(
    "output",
    "icb",
    "qof_obesity_icb_measure.png"
  ),
  plot_qof_ob_icb,
  dpi = 800,
  width = 60,
  height = 30,
  units = "cm"
)


### plot with list size
plot_qof_ob_icb_measure <- df_qof_ob_icb %>%
  ggplot(aes(x = month, y = value, colour = measure)) +
  facet_wrap(vars(icb_name)) +
  geom_line() +
  scale_y_continuous(labels = label_comma()) +
  theme(axis.text.x = element_text(angle = 90))

ggsave(
  filename = here::here(
    "output",
    "icb",
    "qof_obesity_icb_measure.png"
  ),
  plot_qof_ob_icb,
  dpi = 800,
  width = 60,
  height = 30,
  units = "cm"
)
