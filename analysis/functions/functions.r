label_number_n <- label_number(1, 1, big.mark = ",")
label_perc_n <- label_percent(accuracy = .0001)


collapse_and <- function(x) {
  x <- unlist(x, use.names = FALSE)
  n <- length(x)

  if (n == 0) {
    return("")
  }
  if (n == 1) {
    return(x)
  }
  if (n == 2) {
    return(paste(x, collapse = " and "))
  }

  paste0(
    paste(x[seq_len(n - 1)], collapse = ", "),
    ", and ",
    x[n]
  )
}


no_or_num <- function(x) {
  n <- nrow(x)

  if (n == 0) {
    return("No ICB")
  }
  if (n == 1) {
    return("1 ICB")
  }
  if (n > 1) {
    return(glue("{n} ICBs"))
  }
}
