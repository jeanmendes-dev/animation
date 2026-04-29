# ============================================================
# GALAXI-3 ADaM -> ML modular extraction pipeline
# Common notes:
# - Keep modeling grain: USUBJID + AVISIT + AVISITN
# - Output directory defaults to /mnt; change CONFIG below if needed.
# - Works with .sas7bdat, .xpt, .csv, and converted names like adcdai.sas7bdat.csv.
# ============================================================

suppressPackageStartupMessages({
  library(data.table)
  library(dplyr)
  library(tidyr)
  library(stringr)
  library(purrr)
})

CONFIG <- list(
  data_path = "/domino/datasets/local/clinical-trial-data/CNTO1959CRD3001-GALAXI-GAL3-WK48/load-2491/Data",
  output_dir = "/mnt",
  value_separator = " | ",
  key_vars = c("USUBJID", "AVISIT", "AVISITN")
)

log_info <- function(...) message("[INFO] ", paste(..., collapse = ""))
log_warn <- function(...) warning("[WARNING] ", paste(..., collapse = ""), call. = FALSE)

normalize_dataset_name <- function(file_path) {
  nm <- tolower(basename(file_path))
  nm <- str_replace(nm, "\\.sas7bdat\\.csv$", "")
  nm <- str_replace(nm, "\\.xpt\\.csv$", "")
  nm <- str_replace(nm, "\\.sas7bdat$", "")
  nm <- str_replace(nm, "\\.xpt$", "")
  nm <- str_replace(nm, "\\.csv$", "")
  nm
}

safe_name <- function(x) {
  x <- toupper(as.character(x))
  x <- str_replace_all(x, "[^A-Z0-9]+", "_")
  x <- str_replace_all(x, "^_+|_+$", "")
  ifelse(is.na(x) | x == "", "UNKNOWN", x)
}

collapse_distinct <- function(x) {
  x <- x[!is.na(x)]
  x <- x[as.character(x) != ""]
  if (length(x) == 0) return(NA_character_)
  ux <- sort(unique(as.character(x)))
  paste(ux, collapse = CONFIG$value_separator)
}

`%||%` <- function(x, y) if (is.null(x)) y else x

read_one_dataset <- function(path, dataset_name) {
  files <- list.files(path, full.names = TRUE, recursive = TRUE)
  if (length(files) == 0) return(NULL)
  normalized <- vapply(files, normalize_dataset_name, character(1))
  hit <- files[normalized == dataset_name]
  if (length(hit) == 0) return(NULL)
  f <- hit[1]
  fl <- tolower(f)
  if (str_detect(fl, "\\.sas7bdat$")) {
    if (!requireNamespace("haven", quietly = TRUE)) stop("Install haven to read sas7bdat files.")
    as.data.frame(haven::read_sas(f))
  } else if (str_detect(fl, "\\.xpt$")) {
    if (!requireNamespace("haven", quietly = TRUE)) stop("Install haven to read xpt files.")
    as.data.frame(haven::read_xpt(f))
  } else {
    as.data.frame(data.table::fread(f, sep = "auto", showProgress = FALSE))
  }
}

ensure_visit_keys <- function(df, dataset_name) {
  if (is.null(df) || !("USUBJID" %in% names(df))) return(df)
  if ("AVISITN" %in% names(df)) {
    if (!("AVISIT" %in% names(df))) df$AVISIT <- paste0("VISIT_", df$AVISITN)
    return(df)
  }
  if (dataset_name == "adsl") {
    df$AVISIT <- "BASELINE"
    df$AVISITN <- 0L
    return(df)
  }
  order_map <- list(
    adae = c("ASTDT", "AESEQ"), adbdc = c("ASTDT", "ASTDTC", "PARAMCD"),
    addisp = c("ASTDT", "ASTDTC", "PARAMCD"), adice = c("ADT", "ADY", "PARAMCD"),
    adrxfail = c("PARAMCD"), adrxhist = c("ASEQ", "CMSEQ"), admace = c("ADT", "ASTDT", "PARAMCD")
  )
  order_cols <- intersect(order_map[[dataset_name]] %||% c("ADT", "ASTDT", "ASTDTC", "PARAMCD"), names(df))
  if (length(order_cols) > 0) df <- df %>% arrange(USUBJID, across(all_of(order_cols)))
  df %>% group_by(USUBJID) %>% mutate(AVISITN = row_number(), AVISIT = paste0("PSEUDO_", AVISITN)) %>% ungroup()
}

make_coverage_row <- function(dataset, df_source, df_output, extracted_cols, notes = "") {
  tibble::tibble(
    dataset = dataset,
    source_rows = ifelse(is.null(df_source), 0L, nrow(df_source)),
    source_cols = ifelse(is.null(df_source), 0L, ncol(df_source)),
    output_rows = ifelse(is.null(df_output), 0L, nrow(df_output)),
    output_cols = ifelse(is.null(df_output), 0L, ncol(df_output)),
    source_unique_usubjid = ifelse(!is.null(df_source) && "USUBJID" %in% names(df_source), dplyr::n_distinct(df_source$USUBJID), NA_integer_),
    output_unique_usubjid = ifelse(!is.null(df_output) && "USUBJID" %in% names(df_output), dplyr::n_distinct(df_output$USUBJID), NA_integer_),
    source_unique_keys = ifelse(!is.null(df_source) && all(CONFIG$key_vars %in% names(df_source)), nrow(dplyr::distinct(df_source, across(all_of(CONFIG$key_vars)))), NA_integer_),
    output_unique_keys = ifelse(!is.null(df_output) && all(CONFIG$key_vars %in% names(df_output)), nrow(dplyr::distinct(df_output, across(all_of(CONFIG$key_vars)))), NA_integer_),
    extracted_feature_cols = length(extracted_cols),
    extracted_feature_names = paste(extracted_cols, collapse = ", "),
    warning_notes = notes
  )
}

# ============================================================
# SCRIPT 03 - Event-based feature extraction
# Output: galaxi3_event_features.csv + galaxi3_event_coverage_report.csv
# ============================================================

EVENT_DATASETS <- c("adae", "adex", "addisp", "adrxhist", "admace")

yn_to_int <- function(x) as.integer(toupper(trimws(as.character(x))) %in% c("Y", "YES", "1", "TRUE"))
sum_if_exists <- function(df, col) if (col %in% names(df)) sum(suppressWarnings(as.numeric(df[[col]])), na.rm = TRUE) else NA_real_
any_flag <- function(df, col) if (col %in% names(df)) as.integer(any(yn_to_int(df[[col]]) == 1, na.rm = TRUE)) else NA_integer_
n_distinct_if_exists <- function(df, col) if (col %in% names(df)) n_distinct(df[[col]], na.rm = TRUE) else NA_integer_
min_if_exists <- function(df, col) if (col %in% names(df)) suppressWarnings(min(as.numeric(df[[col]]), na.rm = TRUE)) else NA_real_
max_if_exists <- function(df, col) if (col %in% names(df)) suppressWarnings(max(as.numeric(df[[col]]), na.rm = TRUE)) else NA_real_
fix_inf <- function(x) ifelse(is.infinite(x), NA_real_, x)

extract_adae <- function(df) {
  df %>% group_by(USUBJID, AVISIT, AVISITN) %>% group_modify(~ tibble::tibble(
    STC_ADAE_N_EVENTS = nrow(.x),
    STC_ADAE_N_UNIQUE_TERMS = n_distinct_if_exists(.x, "AETERM"),
    STC_ADAE_N_UNIQUE_DECODED = n_distinct_if_exists(.x, "AEDECOD"),
    STC_ADAE_ANY_SERIOUS = any_flag(.x, "AESER"),
    STC_ADAE_ANY_TRTEM = any_flag(.x, "TRTEMFL"),
    STC_ADAE_ANY_DEATH = any_flag(.x, "AESDTH"),
    STC_ADAE_ANY_HOSPITALIZATION = any_flag(.x, "AESHOSP"),
    STC_ADAE_ANY_SEVERE = if ("AESEV" %in% names(.x)) as.integer(any(toupper(.x$AESEV) %in% c("SEVERE", "GRADE 3", "GRADE 4", "GRADE 5"), na.rm = TRUE)) else NA_integer_,
    STC_ADAE_FIRST_EVENT_DAY = fix_inf(min_if_exists(.x, "ASTDY")),
    STC_ADAE_LAST_EVENT_DAY = fix_inf(max_if_exists(.x, "AENDY")),
    STC_ADAE_BODY_SYSTEM_LIST = if ("AEBODSYS" %in% names(.x)) collapse_distinct(.x$AEBODSYS) else NA_character_,
    STC_ADAE_DECODED_TERM_LIST = if ("AEDECOD" %in% names(.x)) collapse_distinct(.x$AEDECOD) else NA_character_
  )) %>% ungroup()
}

extract_adex <- function(df) {
  df %>% group_by(USUBJID, AVISIT, AVISITN) %>% group_modify(~ tibble::tibble(
    AVAL_ADEX_N_EXPOSURE_RECORDS = nrow(.x),
    AVAL_ADEX_TOTAL_DOSE = sum_if_exists(.x, "EXDOSE"),
    AVAL_ADEX_N_DOSE_LEVELS = n_distinct_if_exists(.x, "EXDOSE"),
    AVAL_ADEX_N_LOTS = n_distinct_if_exists(.x, "EXLOT"),
    AVAL_ADEX_FIRST_DAY = fix_inf(min_if_exists(.x, "ASTDY")),
    AVAL_ADEX_LAST_DAY = fix_inf(max_if_exists(.x, "AENDY")),
    AVAL_ADEX_ROUTE_LIST = if ("EXROUTE" %in% names(.x)) collapse_distinct(.x$EXROUTE) else NA_character_,
    AVAL_ADEX_TREATMENT_LIST = if ("EXTRT" %in% names(.x)) collapse_distinct(.x$EXTRT) else NA_character_
  )) %>% ungroup()
}

extract_addisp <- function(df) {
  df %>% group_by(USUBJID, AVISIT, AVISITN) %>% group_modify(~ tibble::tibble(
    STC_ADDISP_N_RECORDS = nrow(.x),
    STC_ADDISP_STATUS_LIST = if ("AVALC" %in% names(.x)) collapse_distinct(.x$AVALC) else NA_character_,
    STC_ADDISP_PARAM_LIST = if ("PARAM" %in% names(.x)) collapse_distinct(.x$PARAM) else NA_character_,
    STC_ADDISP_REASON_LIST = if ("AVALCSP" %in% names(.x)) collapse_distinct(.x$AVALCSP) else NA_character_,
    STC_ADDISP_ANY_CRIT1 = any_flag(.x, "CRIT1FL"),
    STC_ADDISP_ANY_CRIT2 = any_flag(.x, "CRIT2FL")
  )) %>% ungroup()
}

extract_adrxhist <- function(df) {
  df %>% group_by(USUBJID, AVISIT, AVISITN) %>% group_modify(~ {
    txt <- toupper(paste(c(if ("ACMTRT" %in% names(.x)) .x$ACMTRT else NULL,
                           if ("ACAT1" %in% names(.x)) .x$ACAT1 else NULL), collapse = " "))
    tibble::tibble(
      STC_ADRXHIST_N_PRIOR_TX_RECORDS = nrow(.x),
      STC_ADRXHIST_N_PRIOR_TX = n_distinct_if_exists(.x, "ACMTRT"),
      STC_ADRXHIST_ANY_INTOLERANCE = any_flag(.x, "INTOLFL"),
      STC_ADRXHIST_ANY_DEPENDENCE = any_flag(.x, "DEPENDFL"),
      STC_ADRXHIST_ANY_NON_RESPONSE = any_flag(.x, "NRESPFL"),
      STC_ADRXHIST_ANY_BIOLOGIC_TEXT = as.integer(str_detect(txt, "BIO|TNF|ADAL|INFLIX|VEDO|USTE|RISAN|GOLIM|CERTOL")),
      STC_ADRXHIST_TX_LIST = if ("ACMTRT" %in% names(.x)) collapse_distinct(.x$ACMTRT) else NA_character_,
      STC_ADRXHIST_CATEGORY_LIST = if ("ACAT1" %in% names(.x)) collapse_distinct(.x$ACAT1) else NA_character_
    )
  }) %>% ungroup()
}

extract_generic_event <- function(df, dataset_name) {
  value_vars <- setdiff(names(df), CONFIG$key_vars)
  df %>% group_by(USUBJID, AVISIT, AVISITN) %>%
    summarise(across(all_of(value_vars), collapse_distinct), .groups = "drop") %>%
    rename_with(~ paste0("STC_", toupper(dataset_name), "_", .x), all_of(value_vars))
}

extract_event_one <- function(dataset_name) {
  df0 <- read_one_dataset(CONFIG$data_path, dataset_name)
  if (is.null(df0)) return(list(data = NULL, coverage = make_coverage_row(dataset_name, NULL, NULL, character(), "dataset not found")))
  df <- ensure_visit_keys(df0, dataset_name)
  out <- switch(dataset_name,
                adae = extract_adae(df),
                adex = extract_adex(df),
                addisp = extract_addisp(df),
                adrxhist = extract_adrxhist(df),
                extract_generic_event(df, dataset_name))
  out_cols <- setdiff(names(out), CONFIG$key_vars)
  list(data = out, coverage = make_coverage_row(dataset_name, df, out, out_cols, "event records aggregated as counts/flags/lists at modeling grain"))
}

run_event_extraction <- function() {
  log_info("Starting event feature extraction")
  res <- lapply(EVENT_DATASETS, extract_event_one); names(res) <- EVENT_DATASETS
  features <- bind_rows(lapply(res, `[[`, "data")) %>% distinct()
  coverage <- bind_rows(lapply(res, `[[`, "coverage"))
  fwrite(features, file.path(CONFIG$output_dir, "galaxi3_event_features.csv"))
  fwrite(coverage, file.path(CONFIG$output_dir, "galaxi3_event_coverage_report.csv"))
  print(coverage)
  invisible(list(features = features, coverage = coverage))
}
run_event_extraction()
