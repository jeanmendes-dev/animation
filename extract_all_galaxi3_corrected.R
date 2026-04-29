# ============================================================
# CLINICAL ADaM -> ML-READY ARD PIPELINE
# STUDY: CNTO1959CRD3001-GALAXI-GAL3-WK48
# FILE: extract_all_galaxi3_corrected.R
# ============================================================
# MAIN FIX IN THIS VERSION
# ------------------------------------------------------------
# The previous version grouped rows only by USUBJID + AVISITN and then
# summarized each variable using first() for character variables and mean()
# for numeric variables. This caused loss of information in BDS-like ADaM
# datasets where multiple PARAMCD/PARAM records exist for the same subject
# and visit. Example: ADCDAI may have many PARAMCD values for the same
# USUBJID + AVISITN, but AVAL_ADCDAI_PARAM kept only the first PARAM.
#
# This version preserves all records by:
#   1) detecting PARAMCD-based ADaM/BDS datasets;
#   2) pivoting parameter-level variables by PARAMCD;
#   3) also creating *_LIST columns with all distinct values per key;
#   4) improving coverage so it reports value preservation diagnostics,
#      not only whether a source column was touched.
# ============================================================

# ===============================
# LIBRARIES
# ===============================
library(data.table)
library(dplyr)
library(stringr)
library(purrr)
library(tidyr)

# ===============================
# CONFIG
# ===============================
DATA_PATH <- "/domino/datasets/local/clinical-trial-data/CNTO1959CRD3001-GALAXI-GAL3-WK48/load-2491/Data"

OUTPUT_ARD      <- "/mnt/galaxi3_ml_dataset.csv"
OUTPUT_COVERAGE <- "/mnt/galaxi3_ml_coverage_report.csv"

# Datasets expected for GALAXI-3. ADMACE is kept here because it was part of
# the requested scope; if absent in the folder, the loader logs a warning and
# continues safely.
EXPECTED_DATASETS <- c(
  "adae", "adbdc", "adcdai", "addisp", "adex", "adice", "adlbef",
  "admace", "adrxfail", "adrxhist", "adsaf", "adsl", "advscdai"
)

# Datasets that usually do not have real AVISITN and need pseudo-visits.
# If a dataset already contains AVISITN, it is not modified.
DATASETS_SEM_AVISITN <- c(
  "adae", "adbdc", "addisp", "adice", "adrxfail", "adrxhist", "adsl", "admace"
)

# Datasets with real longitudinal visit structure.
DATASETS_COM_AVISITN <- c("adcdai", "adex", "adlbef", "adsaf", "advscdai")

# Values are concatenated only when multiple distinct values truly exist for
# the same modeling grain. This preserves information without multiplying rows.
VALUE_SEPARATOR <- " | "

# Variables usually useful to pivot by PARAMCD in BDS-like ADaM datasets.
# Variables absent from a given dataset are ignored automatically.
PARAM_LEVEL_VARS <- c(
  "PARAM", "PARAMCD", "AVAL", "AVALC", "BASE", "CHG", "PCHG", "DTYPE",
  "ABLFL", "APOBLFL", "ANL01FL", "ANL02FL", "ANL03FL", "ANL04FL", "ANL05FL",
  "ANL06FL", "ANL07FL", "AVALCAT1", "AVALCAT2", "PARCAT1", "PARCAT2",
  "CRIT1", "CRIT1FL", "CRIT2", "CRIT2FL", "SRCDOM", "SRCVAR", "SRCSEQ"
)

KEY_VARS <- c("USUBJID", "AVISIT", "AVISITN")

# ===============================
# LOGGING
# ===============================
log_warn <- function(msg) warning(paste0("[WARNING] ", msg), call. = FALSE)
log_info <- function(msg) message(paste0("[INFO] ", msg))

# ===============================
# HELPERS
# ===============================
normalize_dataset_name <- function(file_path) {
  nm <- tolower(basename(file_path))
  nm <- str_replace(nm, "\\.sas7bdat\\.csv$", "")
  nm <- str_replace(nm, "\\.xpt\\.csv$", "")
  nm <- str_replace(nm, "\\.sas7bdat$", "")
  nm <- str_replace(nm, "\\.xpt$", "")
  nm <- str_replace(nm, "\\.csv$", "")
  nm
}

`%||%` <- function(x, y) if (is.null(x)) y else x

safe_colname <- function(x) {
  x <- toupper(as.character(x))
  x <- str_replace_all(x, "[^A-Z0-9]+", "_")
  x <- str_replace_all(x, "^_+|_+$", "")
  ifelse(is.na(x) | x == "", "UNKNOWN", x)
}

# Preserve all distinct non-missing values. For one value, keep scalar; for
# multiple values, concatenate deterministically. Numeric columns remain numeric
# only when a single distinct numeric value exists per key; otherwise they are
# converted to a distinct-value string to avoid silent averaging.
collapse_distinct <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA)

  ux <- unique(x)

  if (length(ux) == 1) {
    return(as.character(ux[1]))
  }

  ux_chr <- sort(as.character(ux))
  paste(ux_chr, collapse = VALUE_SEPARATOR)
}

has_real_visit <- function(df) {
  all(c("USUBJID", "AVISITN") %in% names(df))
}

is_param_dataset <- function(df) {
  all(c("USUBJID", "PARAMCD", "PARAM") %in% names(df))
}

# ===============================
# 1. LOAD DATASETS
# ===============================
load_datasets <- function(path) {

  sas_files <- list.files(path, pattern = "\\.sas7bdat$", full.names = TRUE, recursive = TRUE)
  csv_files <- list.files(path, pattern = "\\.csv$",      full.names = TRUE, recursive = TRUE)

  # If SAS and CSV coexist, prefer SAS7BDAT. Otherwise use CSV.
  if (length(sas_files) > 0) {
    if (!requireNamespace("haven", quietly = TRUE)) {
      stop("Pacote 'haven' necessário para ler SAS7BDAT. Instale com: install.packages('haven')")
    }
    files <- sas_files
    read_fn <- function(f) as.data.frame(haven::read_sas(f))
    log_info("Lendo arquivos SAS7BDAT via haven")
  } else {
    files <- csv_files
    read_fn <- function(f) as.data.frame(data.table::fread(f, sep = "auto"))
    log_info("Lendo arquivos CSV via data.table")
  }

  if (length(files) == 0) stop(paste("Nenhum arquivo encontrado em:", path))

  datasets <- lapply(files, function(f) {
    tryCatch(read_fn(f), error = function(e) {
      log_warn(paste("Falha ao ler:", f, "—", conditionMessage(e)))
      NULL
    })
  })

  names(datasets) <- vapply(files, normalize_dataset_name, character(1))
  datasets <- datasets[!sapply(datasets, is.null)]

  # Keep only the requested/expected datasets when present.
  datasets <- datasets[names(datasets) %in% EXPECTED_DATASETS]

  missing_ds <- setdiff(EXPECTED_DATASETS, names(datasets))
  if (length(missing_ds) > 0) {
    log_warn(paste("Datasets esperados não encontrados:", paste(missing_ds, collapse = ", ")))
  }

  log_info(paste("Carregados", length(datasets), "datasets dentro do escopo"))
  datasets
}

# ===============================
# 2. ADD PSEUDO VISIT
# ===============================
add_pseudo_visit <- function(df, dataset_name) {

  if (!("USUBJID" %in% names(df))) return(df)
  if ("AVISITN" %in% names(df)) {
    if (!("AVISIT" %in% names(df))) df$AVISIT <- paste0("VISIT_", df$AVISITN)
    return(df)
  }

  log_warn(paste("Criando pseudo AVISITN para:", dataset_name))

  if (dataset_name == "adsl") {
    df <- df %>% mutate(AVISITN = 0L, AVISIT = "BASELINE")
    return(df)
  }

  order_map <- list(
    adae     = c("ASTDT", "AESEQ"),
    adbdc    = c("ASTDT", "ASTDTC", "PARAMCD"),
    addisp   = c("ASTDT", "ASTDTC", "PARAMCD"),
    adice    = c("ADT", "ADY", "PARAMCD"),
    adrxfail = c("PARAMCD"),
    adrxhist = c("ASEQ", "CMSEQ"),
    admace   = c("ADT", "ASTDT", "PARAMCD")
  )

  order_cols <- intersect(order_map[[dataset_name]] %||% c("ADT", "ASTDT", "ASTDTC", "PARAMCD"), names(df))

  if (length(order_cols) > 0) {
    df <- df %>%
      arrange(USUBJID, across(all_of(order_cols))) %>%
      group_by(USUBJID) %>%
      mutate(AVISITN = row_number(), AVISIT = paste0("PSEUDO_", AVISITN)) %>%
      ungroup()
  } else {
    df <- df %>%
      group_by(USUBJID) %>%
      mutate(AVISITN = row_number(), AVISIT = paste0("PSEUDO_", AVISITN)) %>%
      ungroup()
  }

  df
}

# ===============================
# 3. BUILD SPINE
# ===============================
build_spine <- function(datasets) {

  spine_parts <- list()

  for (nm in c("advscdai", "adcdai", "adlbef", "adex", "adsaf")) {
    if (nm %in% names(datasets) && has_real_visit(datasets[[nm]])) {
      spine_parts[[nm]] <- datasets[[nm]] %>%
        select(any_of(KEY_VARS)) %>%
        distinct()
      log_info(paste("Complementando spine com:", nm))
    }
  }

  if (length(spine_parts) == 0) {
    stop("Nenhum dataset com USUBJID + AVISITN disponível para criar a spine.")
  }

  spine <- bind_rows(spine_parts) %>% distinct()

  if ("adsl" %in% names(datasets)) {
    subj_adsl <- datasets[["adsl"]] %>%
      select(USUBJID) %>%
      distinct() %>%
      mutate(AVISIT = "BASELINE", AVISITN = 0L)

    spine <- bind_rows(spine, subj_adsl) %>% distinct()
  }

  spine <- spine %>% arrange(USUBJID, AVISITN, AVISIT)

  log_info(paste("Spine criada com", nrow(spine), "linhas e", n_distinct(spine$USUBJID), "sujeitos únicos"))
  spine
}

# ===============================
# 4. VARIABLE CLASSIFICATION
# ===============================
classify_variable <- function(df, var) {

  if (!("USUBJID" %in% names(df))) return("STATIC")
  if (var %in% c("AVISIT", "AVISITN")) return("LONGITUDINAL")

  tmp <- df %>% select(USUBJID, all_of(var)) %>% distinct()

  variability <- tmp %>%
    group_by(USUBJID) %>%
    summarise(n = n_distinct(.data[[var]], na.rm = TRUE), .groups = "drop")

  if (any(variability$n > 1, na.rm = TRUE)) "LONGITUDINAL" else "STATIC"
}

# ===============================
# 5. GENERIC NON-PARAM EXTRACTION
# ===============================
extract_generic_dataset <- function(df, dataset_name) {

  vars <- setdiff(names(df), KEY_VARS)
  extracted <- list()
  detail <- list()

  for (v in vars) {
    class_type <- classify_variable(df, v)

    if (class_type == "LONGITUDINAL") {
      key_cols <- c("USUBJID", "AVISITN")
      tmp <- df %>%
        select(all_of(key_cols), value = all_of(v)) %>%
        group_by(across(all_of(key_cols))) %>%
        summarise(value = collapse_distinct(value), .groups = "drop")

      colname <- paste0("AVAL_", toupper(dataset_name), "_", v)
      names(tmp)[names(tmp) == "value"] <- colname

    } else {
      key_cols <- "USUBJID"
      tmp <- df %>%
        select(all_of(key_cols), value = all_of(v)) %>%
        group_by(USUBJID) %>%
        summarise(value = collapse_distinct(value), .groups = "drop")

      colname <- paste0("STC_", toupper(dataset_name), "_", v)
      names(tmp)[names(tmp) == "value"] <- colname
    }

    extracted[[colname]] <- tmp
    detail[[v]] <- tibble(
      source_var = v,
      output_cols = colname,
      extraction_strategy = "generic_distinct_collapse",
      class_type = class_type,
      source_distinct_non_missing = n_distinct(df[[v]], na.rm = TRUE),
      output_distinct_non_missing = n_distinct(tmp[[colname]], na.rm = TRUE),
      source_rows = nrow(df),
      output_rows = nrow(tmp)
    )
  }

  list(extracted = extracted, detail = bind_rows(detail), all_vars = vars)
}

# ===============================
# 6. PARAMCD-BASED EXTRACTION
# ===============================
extract_param_dataset <- function(df, dataset_name) {

  vars <- setdiff(names(df), KEY_VARS)
  extracted <- list()
  detail <- list()

  # Determine modeling grain. If the dataset has real/pseudo AVISITN, use it;
  # otherwise use subject-level grain.
  key_cols <- if ("AVISITN" %in% names(df)) c("USUBJID", "AVISITN") else "USUBJID"
  prefix <- if ("AVISITN" %in% names(df)) "AVAL" else "STC"

  # PARAMCD must be safe for column names.
  df <- df %>% mutate(.PARAMCD_SAFE = safe_colname(PARAMCD))

  param_vars <- intersect(PARAM_LEVEL_VARS, vars)
  non_param_vars <- setdiff(vars, c(param_vars, "PARAMCD"))

  # A) Preserve each PARAMCD as separate wide columns for PARAM-level variables.
  # Example output: AVAL_ADCDAI_CDAI_AVAL, AVAL_ADCDAI_CD201_PARAM.
  for (v in param_vars) {

    long_sum <- df %>%
      select(all_of(key_cols), .PARAMCD_SAFE, value = all_of(v)) %>%
      group_by(across(all_of(c(key_cols, ".PARAMCD_SAFE")))) %>%
      summarise(value = collapse_distinct(value), .groups = "drop")

    wide <- long_sum %>%
      mutate(.out_col = paste0(prefix, "_", toupper(dataset_name), "_", .PARAMCD_SAFE, "_", v)) %>%
      select(-.PARAMCD_SAFE) %>%
      tidyr::pivot_wider(names_from = .out_col, values_from = value)

    # B) Also create a compact LIST column preserving all distinct values across
    # PARAMCDs per key. This directly fixes cases such as AVAL_ADCDAI_PARAM.
    list_col <- paste0(prefix, "_", toupper(dataset_name), "_", v)
    list_df <- df %>%
      select(all_of(key_cols), value = all_of(v)) %>%
      group_by(across(all_of(key_cols))) %>%
      summarise(value = collapse_distinct(value), .groups = "drop")
    names(list_df)[names(list_df) == "value"] <- list_col

    # Return both: param-specific wide columns + the original-style LIST column.
    extracted[[paste0(list_col, "__LIST")]] <- list_df
    extracted[[paste0(list_col, "__PARAM_WIDE")]] <- wide

    output_cols <- c(list_col, setdiff(names(wide), key_cols))
    detail[[v]] <- tibble(
      source_var = v,
      output_cols = paste(output_cols, collapse = ", "),
      extraction_strategy = "paramcd_pivot_wide_plus_distinct_list",
      class_type = ifelse(prefix == "AVAL", "LONGITUDINAL", "STATIC"),
      source_distinct_non_missing = n_distinct(df[[v]], na.rm = TRUE),
      output_distinct_non_missing = max(
        n_distinct(list_df[[list_col]], na.rm = TRUE),
        n_distinct(unlist(wide[setdiff(names(wide), key_cols)], use.names = FALSE), na.rm = TRUE)
      ),
      source_rows = nrow(df),
      output_rows = nrow(wide),
      n_paramcd_source = n_distinct(df$PARAMCD, na.rm = TRUE),
      n_paramcd_output_columns = length(setdiff(names(wide), key_cols))
    )
  }

  # C) Non-PARAM-level variables are extracted once per key using safe distinct
  # collapse, not first()/mean(). This avoids silent loss in repeated records.
  for (v in non_param_vars) {

    class_type <- classify_variable(df, v)
    out_prefix <- ifelse(class_type == "LONGITUDINAL", "AVAL", "STC")
    key_cols_v <- if (class_type == "LONGITUDINAL" && "AVISITN" %in% names(df)) c("USUBJID", "AVISITN") else "USUBJID"

    tmp <- df %>%
      select(all_of(key_cols_v), value = all_of(v)) %>%
      group_by(across(all_of(key_cols_v))) %>%
      summarise(value = collapse_distinct(value), .groups = "drop")

    colname <- paste0(out_prefix, "_", toupper(dataset_name), "_", v)
    names(tmp)[names(tmp) == "value"] <- colname

    extracted[[colname]] <- tmp
    detail[[v]] <- tibble(
      source_var = v,
      output_cols = colname,
      extraction_strategy = "non_param_distinct_collapse",
      class_type = class_type,
      source_distinct_non_missing = n_distinct(df[[v]], na.rm = TRUE),
      output_distinct_non_missing = n_distinct(tmp[[colname]], na.rm = TRUE),
      source_rows = nrow(df),
      output_rows = nrow(tmp),
      n_paramcd_source = n_distinct(df$PARAMCD, na.rm = TRUE),
      n_paramcd_output_columns = NA_integer_
    )
  }

  list(extracted = extracted, detail = bind_rows(detail), all_vars = vars)
}

# ===============================
# 7. DISPATCH EXTRACTION
# ===============================
extract_dataset <- function(df, dataset_name) {

  if (!("USUBJID" %in% names(df))) {
    log_warn(paste("Dataset ignorado por não conter USUBJID:", dataset_name))
    return(list(extracted = list(), detail = tibble(), all_vars = names(df)))
  }

  if (is_param_dataset(df)) {
    log_info(paste("Extração PARAMCD-aware aplicada em:", dataset_name))
    extract_param_dataset(df, dataset_name)
  } else {
    log_info(paste("Extração genérica aplicada em:", dataset_name))
    extract_generic_dataset(df, dataset_name)
  }
}

# ===============================
# 8. MERGE ARD
# ===============================
merge_ard <- function(spine, extracted_all) {

  ard <- spine

  for (ds_name in names(extracted_all)) {
    ds <- extracted_all[[ds_name]]

    for (obj_name in names(ds$extracted)) {
      obj <- ds$extracted[[obj_name]]

      if (nrow(obj) == 0) next

      join_keys <- if (all(c("USUBJID", "AVISITN") %in% names(obj))) c("USUBJID", "AVISITN") else "USUBJID"

      # Safety check: every object must be unique at its join grain before merge.
      dup_check <- obj %>% count(across(all_of(join_keys))) %>% filter(n > 1)
      if (nrow(dup_check) > 0) {
        stop(paste("Objeto extraído com duplicatas antes do merge:", ds_name, obj_name,
                   "| grain:", paste(join_keys, collapse = "+")))
      }

      ard <- left_join(ard, obj, by = join_keys)
    }
  }

  dup_check <- ard %>% count(USUBJID, AVISITN) %>% filter(n > 1)
  if (nrow(dup_check) > 0) {
    log_warn(paste("Duplicatas USUBJID x AVISITN detectadas no ARD:", nrow(dup_check), "combinações"))
  }

  log_info(paste("ARD final:", nrow(ard), "linhas x", ncol(ard), "colunas"))
  ard
}

# ===============================
# 9. COVERAGE REPORT
# ===============================
build_coverage <- function(extracted_all) {

  report <- imap_dfr(extracted_all, function(ds, name) {

    total <- length(ds$all_vars)
    detail <- ds$detail

    if (nrow(detail) == 0) {
      return(tibble(
        dataset = name,
        total_vars = total,
        extracted_vars = 0L,
        missing_vars = total,
        extraction_rate = 0,
        source_rows = NA_integer_,
        output_rows_max = NA_integer_,
        n_paramcd_source_max = NA_integer_,
        n_paramcd_output_columns_sum = NA_integer_,
        possible_value_loss_flags = total,
        warning_notes = "Dataset not extracted or missing USUBJID"
      ))
    }

    missing <- setdiff(ds$all_vars, detail$source_var)

    # Conservative diagnostic: flag variables where the output has fewer
    # distinct non-missing values than the source. This is not always wrong
    # because output grain is intentionally different, but it highlights cases
    # that deserve review. PARAMCD-aware columns should reduce this risk.
    value_loss_flags <- sum(
      detail$output_distinct_non_missing < detail$source_distinct_non_missing,
      na.rm = TRUE
    )

    tibble(
      dataset = name,
      total_vars = total,
      extracted_vars = n_distinct(detail$source_var),
      missing_vars = length(missing),
      extraction_rate = round(100 * n_distinct(detail$source_var) / total, 2),
      source_rows = max(detail$source_rows, na.rm = TRUE),
      output_rows_max = max(detail$output_rows, na.rm = TRUE),
      n_paramcd_source_max = suppressWarnings(max(detail$n_paramcd_source, na.rm = TRUE)),
      n_paramcd_output_columns_sum = suppressWarnings(sum(detail$n_paramcd_output_columns, na.rm = TRUE)),
      possible_value_loss_flags = value_loss_flags,
      missing_var_names = paste(missing, collapse = ", "),
      warning_notes = ifelse(value_loss_flags > 0,
                             "Review variables where distinct values were compressed at target grain",
                             "OK")
    )
  })

  data.table::fwrite(report, OUTPUT_COVERAGE)
  log_info(paste("Coverage report salvo em:", OUTPUT_COVERAGE))
  print(report)

  report
}

# ===============================
# 10. MAIN PIPELINE
# ===============================
run_pipeline <- function() {

  log_info("=== GALAXI-3 ARD PIPELINE INICIADO ===")

  datasets <- load_datasets(DATA_PATH)
  datasets <- imap(datasets, add_pseudo_visit)

  spine <- build_spine(datasets)

  extracted_all <- list()
  for (name in names(datasets)) {
    log_info(paste("Processando dataset:", name))
    extracted_all[[name]] <- tryCatch(
      extract_dataset(datasets[[name]], name),
      error = function(e) {
        log_warn(paste("Erro ao processar", name, ":", conditionMessage(e)))
        list(extracted = list(), detail = tibble(), all_vars = setdiff(names(datasets[[name]]), KEY_VARS))
      }
    )
  }

  ard <- merge_ard(spine, extracted_all)

  data.table::fwrite(ard, OUTPUT_ARD)
  log_info(paste("ARD salvo em:", OUTPUT_ARD))

  coverage <- build_coverage(extracted_all)

  log_info("=== PIPELINE CONCLUÍDO COM SUCESSO ===")

  invisible(list(ard = ard, coverage = coverage))
}

# ===============================
# RUN
# ===============================
run_pipeline()
