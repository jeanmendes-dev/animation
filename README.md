# animation

has_aval  <- "AVAL"  %in% names(df)
has_avalc <- "AVALC" %in% names(df)

has_any_value <- has_aval | has_avalc

has_aval = has_any_value
