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
# SCRIPT 04 - Merge modular feature files into final ARD
# Inputs: galaxi3_bds_features.csv, galaxi3_static_features.csv, galaxi3_event_features.csv
# Outputs: galaxi3_ml_dataset_modular.csv + galaxi3_final_merge_coverage_report.csv
# ============================================================

feature_files <- list(
  bds = file.path(CONFIG$output_dir, "galaxi3_bds_features.csv"),
  static = file.path(CONFIG$output_dir, "galaxi3_static_features.csv"),
  event = file.path(CONFIG$output_dir, "galaxi3_event_features.csv")
)

read_feature_file <- function(name, path) {
  if (!file.exists(path)) {
    log_warn("Missing feature file: ", path)
    return(NULL)
  }
  df <- fread(path, showProgress = FALSE) %>% as.data.frame()
  if (!all(CONFIG$key_vars %in% names(df))) stop("Feature file missing key vars: ", path)
  df <- df %>% distinct()
  dup <- df %>% count(across(all_of(CONFIG$key_vars))) %>% filter(n > 1)
  if (nrow(dup) > 0) log_warn(name, " has duplicated keys: ", nrow(dup), " combinations")
  df
}

build_spine_from_features <- function(parts) {
  bind_rows(lapply(parts, function(df) if (!is.null(df)) df[, CONFIG$key_vars, drop = FALSE] else NULL)) %>%
    distinct() %>%
    arrange(USUBJID, AVISITN, AVISIT)
}

run_merge <- function() {
  log_info("Starting final merge")
  parts <- imap(feature_files, read_feature_file)
  parts <- parts[!sapply(parts, is.null)]
  if (length(parts) == 0) stop("No feature files found. Run scripts 01, 02 and 03 first.")

  spine <- build_spine_from_features(parts)
  ard <- spine
  coverage_rows <- list()

  for (nm in names(parts)) {
    df <- parts[[nm]]
    feat_cols <- setdiff(names(df), CONFIG$key_vars)
    before <- nrow(ard)
    ard <- left_join(ard, df, by = CONFIG$key_vars)
    coverage_rows[[nm]] <- tibble::tibble(
      extraction_layer = nm,
      input_file = feature_files[[nm]],
      input_rows = nrow(df),
      input_cols = ncol(df),
      input_feature_cols = length(feat_cols),
      final_rows_before_join = before,
      final_rows_after_join = nrow(ard),
      unique_keys_input = nrow(distinct(df, across(all_of(CONFIG$key_vars)))),
      unique_keys_final = nrow(distinct(ard, across(all_of(CONFIG$key_vars))))
    )
  }

  dup <- ard %>% count(across(all_of(CONFIG$key_vars))) %>% filter(n > 1)
  if (nrow(dup) > 0) log_warn("Final ARD duplicated keys: ", nrow(dup))

  fwrite(ard, file.path(CONFIG$output_dir, "galaxi3_ml_dataset_modular.csv"))
  fwrite(bind_rows(coverage_rows), file.path(CONFIG$output_dir, "galaxi3_final_merge_coverage_report.csv"))
  print(bind_rows(coverage_rows))
  log_info("Final ARD: ", nrow(ard), " rows x ", ncol(ard), " columns")
  invisible(ard)
}
run_merge()
