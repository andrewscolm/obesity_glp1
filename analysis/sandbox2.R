# data checks  ----

## Practice list size NA = no row in df_hscic_ccgs for practice/month ----

tirzepatide_clean_na <- df_tirzepatide_practice %>%
  # Q: Why does orlistat have practice but not df_tirzepatide_practice, can we fix?
  # left_join(
  #   df_normalised_prescribing,
  #   by = c("month", "practice")
  # ) %>%
  left_join(
    practice_stats_clean,
    by = c("practice", "month")
  ) %>%
  mutate(
    rateper1000 = items / total_list_size * 1000
  ) %>%
  filter(
    is.na(total_list_size) #&
    # total_list_size < 20
    #   total_list_size >= 20000) &
    # items > 1
  ) %>%
  mutate(year = lubridate::year(month))


pna <- unique(tirzepatide_clean_na$practice)
psna <- practice_stats_clean %>%
  filter(practice %in% pna)
length(unique(psna$pct_id))
length(unique(psna$practice))
pctna <- psna %>%
  filter(is.na(pct_id))


missing_month_df_practice_stats <- df_practice_stats %>%
  filter(practice %in% pna)
nrow(missing_df_practice_stats)


## Missing list size in practice_stats_clean ----
sort(pna)
#  [1] "B81065" "B81675" "C81074" "C86606" "F81651" "G82653" "J82089" "K81638" "K83625" "L81101"
# [11] "L85050" "M92640" "Y02572" "Y02747" "Y02787" "Y02854" "Y03671" "Y05088" "Y05125" "Y05190"
# [21] "Y05257" "Y05258" "Y05960" "Y06125" "Y06153" "Y06247" "Y06311"

## Practice is in df_practice_stats but not for month match ----
sort(unique(missing_month_df_practice_stats$practice))
#  [1] "B81065" "B81675" "C81074" "C86606" "F81651" "G82653" "J82089" "K81638" "K83625" "L81101"
# [11] "L85050" "M92640" "Y02572" "Y02747" "Y02787" "Y03671"

## Missing from df_practice_stats ----
setdiff(
  sort(unique(pna)),
  sort(unique(missing_month_df_practice_stats$practice))
)
#  [1] "Y02854" "Y05088" "Y05125" "Y05190" "Y05257" "Y05258" "Y05960" "Y06125" "Y06153" "Y06247"
# [11] "Y06311"

missing_hscic_ccgs <- df_hscic_ccgs %>%
  filter(code %in% psna$pct_id)

length(unique(missing_hscic_ccgs$code))

# all there
setdiff(unique(missing_hscic_ccgs$code), unique(psna$pct_id))
setdiff(psna$pct_id, missing_hscic_ccgs$code)


missing_icbs <- df_qof_icbs %>%
  filter()


exp <- df_tirzepatide_practice %>%
  filter(practice %in% pna)
ggplot(exp) +
  # geom_line(
  #   aes(x = month, y = items, group = bnf_name),
  #   color = "black",
  # ) +
  geom_point(
    aes(x = month, y = quantity, color = bnf_name, group = bnf_name),
    size = 0.5
  ) +
  geom_point(
    data = df_practice_stats %>% filter(practice %in% pna),
    aes(x = month, y = total_list_size / 1000),
    # linetype = 3,
    color = "#fc1",
    size = 0.5
  ) +
  geom_point(
    data = practice_tirzepatide %>% filter(practice %in% pna),
    aes(x = month, y = rateper1000),
    color = "red",
    alpha = 0.6,
    size = 0.5
  ) +
  scale_y_continuous(limits = c(0, 40)) +
  scale_x_date(
    limits = c(
      as.Date("2024-01-01"),
      as.Date("2025-12-01")
    )
  ) +
  theme_bw() +
  facet_wrap(vars(practice)) +
  guides(color = "none")

ggsave(
  here::here(
    "output",
    "cleaning",
    "missing_list_size.png"
  )
)
x <- df_tirzepatide_practice %>%
  mutate(
    iqdiff = items - quantity,
    iqprop = items / quantity
  ) %>%
  filter(iqdiff < 0)
summary(x$iqdiff)
summary(x$iqprop)
hist(x$iqdiff, breaks = unique(x$iqdiff))


## Practice belongs to more than one icb ----

practice_2_icbs <- practice_stats_clean %>%
  summarize(
    .by = (practice),
    stp_ids = length(unique(stp_id))
  ) %>%
  filter(stp_ids > 1)


check <- practice_stats_clean %>%
  filter(practice %in% practice_2_icbs) %>%
  arrange(practice, month) %>%
  select(month, practice, practice_name, stp_id, total_list_size) %>%
  mutate(m = lubridate::month(month), y = lubridate::year(month))

m88 <- df_practice_stats %>%
  filter(practice %in% c("M88015"))
