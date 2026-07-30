table_query <- function(
  con,
  tab = NULL,
  bnf_code = NULL,
  start_date = NULL,
  end_date = NULL
) {
  start_date <- check_date(start_date)
  end_date <- check_date(end_date)

  if (!is.null(start_date) && !is.null(end_date) && start_date > end_date) {
    cli::cli_abort(
      "{.arg start_date} ({.val {start_date}}) must not be after {.arg end_date} ({.val {end_date}})."
    )
  }

  query <- dplyr::tbl(con, tab)

  if (!is.null(bnf_code)) {
    if (any(grepl("%", bnf_code, fixed = TRUE))) {
      like_sql <- paste0(
        "bnf_code LIKE ",
        DBI::dbQuoteString(con, bnf_code),
        collapse = " OR "
      )
      query <- dplyr::filter(query, dbplyr::sql(like_sql))
    } else {
      query <- dplyr::filter(query, .data$bnf_code %in% !!bnf_code)
    }
  }

  if (!is.null(start_date)) {
    query <- dplyr::filter(query, .data$month >= !!start_date)
  }

  if (!is.null(end_date)) {
    query <- dplyr::filter(query, .data$month <= !!end_date)
  }

  query
}
