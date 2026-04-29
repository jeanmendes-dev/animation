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
# SCRIPT 02 - Subject-level/static extraction
# Output: galaxi3_static_features.csv + galaxi3_static_coverage_report.csv
# ============================================================

STATIC_DATASETS <- c("adsl")

extract_static_one <- function(dataset_name) {
  df0 <- read_one_dataset(CONFIG$data_path, dataset_name)
  if (is.null(df0)) return(list(data = NULL, coverage = make_coverage_row(dataset_name, NULL, NULL, character(), "dataset not found")))
  df <- ensure_visit_keys(df0, dataset_name)
  value_vars <- setdiff(names(df), c("USUBJID", "AVISIT", "AVISITN"))

  out <- df %>%
    select(USUBJID, any_of(value_vars)) %>%
    group_by(USUBJID) %>%
    summarise(across(everything(), collapse_distinct), .groups = "drop")

  names(out) <- ifelse(names(out) == "USUBJID", "USUBJID", paste0("STC_", toupper(dataset_name), "_", names(out)))
  out <- out %>% mutate(AVISIT = "BASELINE", AVISITN = 0L) %>% select(USUBJID, AVISIT, AVISITN, everything())
  out_cols <- setdiff(names(out), CONFIG$key_vars)
  list(data = out, coverage = make_coverage_row(dataset_name, df, out, out_cols, "one row per subject; baseline/static features"))
}

run_static_extraction <- function() {
  log_info("Starting static/ADSL extraction")
  res <- lapply(STATIC_DATASETS, extract_static_one); names(res) <- STATIC_DATASETS
  features <- bind_rows(lapply(res, `[[`, "data")) %>% distinct()
  coverage <- bind_rows(lapply(res, `[[`, "coverage"))
  fwrite(features, file.path(CONFIG$output_dir, "galaxi3_static_features.csv"))
  fwrite(coverage, file.path(CONFIG$output_dir, "galaxi3_static_coverage_report.csv"))
  print(coverage)
  invisible(list(features = features, coverage = coverage))
}
run_static_extraction()
