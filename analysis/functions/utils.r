# Filter query by a column against a vector of values, some of which may be
# "%" wildcard patterns. If any value contains "%", all values are matched
# with SQL LIKE (an exact value like "300" behaves the same under LIKE as
# under ==); otherwise a plain %in% filter is used. Values are quoted with
# DBI so they can't be used to inject SQL.
filter_like_or_in <- function(query, con, column, values) {
  if (any(grepl("%", values, fixed = TRUE))) {
    like_sql <- paste0(
      column,
      " LIKE ",
      DBI::dbQuoteString(con, values),
      collapse = " OR "
    )
    dplyr::filter(query, dbplyr::sql(like_sql))
  } else {
    dplyr::filter(query, .data[[column]] %in% !!values)
  }
}


## format date
formatted_date <- function(date) {
  format(as.Date(date), format = "%B %Y")
}
formatted_num <- function(number) {
  format(number, big.mark = ",", scientific = FALSE)
}

formatted_decimal <- function(number) {
  format(number, big.mark = ",", scientific = FALSE, digits = 3)
}

formatted_decimal <- function(number) {
  format(number, big.mark = ",", scientific = FALSE, digits = 3)
}
