# ============================================================
# CLINICAL ADaM → ML-READY ARD PIPELINE
# STUDY: CNTO1959CRD3001-GALAXI-GAL3-WK48
# ============================================================
#
# HISTÓRICO DE CORREÇÕES
#
# v1 — original
#   Problema: aggregate_value() retorna first() para colunas character.
#   Em datasets tall (adcdai: 26 PARAMCDs/visita), todos os valores
#   exceto o primeiro eram perdidos silenciosamente. Coverage report
#   indicava 100% porque só media presença de colunas, não completude.
#
# v2 — pivot por PARAMCD
#   Solução: detect_tall_format() + pivot_wide_by_paramcd() para
#   datasets BDS com PARAMCD+AVAL. Coverage report ganhou is_tall,
#   n_params_pivoted e na_pct_in_ard.
#   Problema residual: addisp não estava em DATASETS_TALL_FORMAT.
#   Como consequência, recebia pseudo-AVISITN (1, 2, 3...) que nunca
#   coincide com os AVISITN reais da spine (ex: 20000, 32000...).
#   O left_join por USUBJID+AVISITN produzia 100% NULL para todas as
#   colunas AVAL_ADDISP_*. O problema não era detectado pelo coverage
#   report porque a coluna existia — estava só vazia.
#
# v3 — esta versão
#
#   [BUG-4] addisp: tall com só AVALC (sem AVAL numérico), sem AVISITN
#     Adicionado a DATASETS_TALL_FORMAT. pivot_wide_by_paramcd() agora
#     funciona com apenas AVALC presente (sem AVAL). Resultado:
#     AVALC_ADDISP_DCSREAS, AVALC_ADDISP_EOSSTT, etc. Como não tem
#     AVISITN, join final é só por USUBJID (colunas ficam como STC_
#     subject-level, sem mismatch de visita).
#
#   [BUG-5] Pseudo-AVISITN em datasets tall sem AVISITN real
#     add_pseudo_visit() agora pula datasets tall. Eles não precisam
#     de pseudo-visita porque o pivot os reduz a 1 linha por USUBJID
#     e o join é feito exclusivamente por USUBJID.
#
#   [BUG-6] adae/adex/adrxhist: removidos de DATASETS_TALL_FORMAT
#     São OCCDS/custom (múltiplos eventos por sujeito, sem PARAMCD BDS).
#     O pivot retornava 0 colunas — falso alarme no coverage report.
#     Esses datasets voltam à lógica de pseudo-AVISITN + wide.
#
#   [MELHORIA] Coverage report com métricas de células preenchidas
#     Novas colunas:
#     - src_rows/cols/cells_total/cells_filled/fill_pct: origem
#     - ard_cols_count/cells_total/cells_filled/fill_pct: ARD
#     - fill_efficiency: (ard_cells_filled / src_cells_filled) × 100
#       → 100% = dados chegaram sem perda
#       → > 100% = colunas STC_ replicadas em cada visita (esperado)
#       → << 100% = perda de dados (join nulo, pivot falho, etc.)
#
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
DATA_PATH    <- "/domino/datasets/local/clinical-trial-data/CNTO1959CRD3001-GALAXI-GAL3-WK48/load-2491/Data"

OUTPUT_ARD      <- "/mnt/galaxi3_ml_dataset.csv"
OUTPUT_COVERAGE <- "/mnt/galaxi3_ml_coverage_report.csv"

EXPECTED_DATASETS <- c(
  "adae",       # Adverse Events       (103 cols, 1718 rows)
  "adbdc",      # Baseline Disease     (39 cols,  21540 rows)
  "adcdai",     # CDAI                 (40 cols,  159900 rows)
  "addisp",     # Disposition          (42 cols,  505 rows)
  "adex",       # Exposure             (56 cols,  14871 rows)
  "adice",      # ICE                  (39 cols,  450 rows)
  "adlbef",     # Efficacy Labs        (47 cols,  60351 rows)
  "adrxfail",   # Prior Tx Failure     (33 cols,  20475 rows)
  "adrxhist",   # Prior Tx History     (35 cols,  7365 rows)
  "adsaf",      # Safety Exposure      (36 cols,  18709 rows)
  "adsl",       # Subject-Level        (70 cols,  525 rows)
  "advscdai"    # Longitudinal CDAI    (53 cols,  309550 rows)
)

DATASETS_SEM_AVISITN <- c(
  "adae",
  "adbdc",
  "addisp",
  "adice",
  "adrxfail",
  "adrxhist",
  "adsl"
)

DATASETS_COM_AVISITN <- c(
  "adcdai",
  "adex",
  "adlbef",
  "adsaf",
  "advscdai"
)

# ---------------------------------------------------------------
# DATASETS_TALL_FORMAT
#
# Regra: entra todo dataset BDS paramétrico com PARAMCD + AVAL e/ou
# AVALC, onde cada linha representa um parâmetro distinto.
#
# NÃO entram datasets OCCDS/custom com múltiplos registros por sujeito
# mas sem PARAMCD no sentido BDS:
#   adae    → OCCDS: AEs com AETERM/AESEQ, sem PARAMCD BDS
#   adex    → exposição: registros de dose, sem PARAMCD BDS
#   adrxhist→ histórico de tratamento: sem PARAMCD BDS
#
# [v3] addisp adicionado: tem PARAMCD + AVALC (sem AVAL), sem AVISITN
# [v3] adae/adex/adrxhist removidos: OCCDS, não BDS paramétrico
# ---------------------------------------------------------------
DATASETS_TALL_FORMAT <- c(
  "adcdai",    # 26 PARAMCDs/visita — AVAL numérico
  "advscdai",  # múltiplos PARAMCDs/visita — AVAL + AVALC
  "adlbef",    # múltiplos params lab/visita — AVAL + AVALC
  "adsaf",     # múltiplos params safety/visita — AVAL
  "adbdc",     # múltiplos scores doença/sujeito — AVAL + AVALC
  "adrxfail",  # múltiplos registros tx failure — AVAL + AVALC
  "adice",     # múltiplos eventos ICE — AVAL + AVALC
  "addisp"     # [v3] 10 PARAMCDs/sujeito — apenas AVALC, sem AVISITN
)

# ===============================
# LOGGING
# ===============================
log_warn <- function(msg) warning(paste0("[WARNING] ", msg))
log_info <- function(msg) message(paste0("[INFO] ", msg))

# ===============================
# 1. LOAD DATASETS
# ===============================
# SOURCE_SHAPES guarda shape e preenchimento de cada dataset de origem
# para uso no coverage report (métricas de células).
SOURCE_SHAPES <- list()

load_datasets <- function(path) {

  sas_files <- list.files(path, pattern = "\\.sas7bdat$",
                          full.names = TRUE, recursive = TRUE)
  csv_files <- list.files(path, pattern = "\\.csv$",
                          full.names = TRUE, recursive = TRUE)

  if (length(sas_files) > 0) {
    if (!requireNamespace("haven", quietly = TRUE)) {
      stop("Pacote 'haven' necessário para ler SAS7BDAT. Instale com: install.packages('haven')")
    }
    files   <- sas_files
    read_fn <- function(f) as.data.frame(haven::read_sas(f))
    log_info("Lendo arquivos SAS7BDAT via haven")
  } else {
    files   <- csv_files
    read_fn <- function(f) as.data.frame(fread(f, sep = "auto"))
    log_info("Lendo arquivos CSV via data.table")
  }

  if (length(files) == 0) stop(paste("Nenhum arquivo encontrado em:", path))

  datasets <- lapply(files, function(f) {
    tryCatch(read_fn(f), error = function(e) {
      log_warn(paste("Falha ao ler:", f, "—", conditionMessage(e)))
      return(NULL)
    })
  })

  names(datasets) <- tolower(tools::file_path_sans_ext(basename(files)))
  datasets <- datasets[!sapply(datasets, is.null)]

  # [MELHORIA v3] Registrar shape e preenchimento para o coverage report
  for (nm in names(datasets)) {
    df <- datasets[[nm]]
    SOURCE_SHAPES[[nm]] <<- list(
      rows         = nrow(df),
      cols         = ncol(df),
      cells_total  = nrow(df) * ncol(df),
      cells_filled = sum(!is.na(df))
    )
  }

  missing_ds <- setdiff(EXPECTED_DATASETS, names(datasets))
  if (length(missing_ds) > 0) {
    log_warn(paste("Datasets esperados não encontrados:", paste(missing_ds, collapse = ", ")))
  }

  extra_ds <- setdiff(names(datasets), EXPECTED_DATASETS)
  if (length(extra_ds) > 0) {
    log_warn(paste("Datasets encontrados fora do escopo:", paste(extra_ds, collapse = ", ")))
  }

  log_info(paste("Carregados", length(datasets), "datasets"))
  return(datasets)
}

# ===============================
# 2. ADD PSEUDO VISIT
# ===============================
# [BUG-5 v3] Datasets tall sem AVISITN NÃO recebem pseudo-visita.
# O pivot os reduz a 1 linha/USUBJID e o join é feito só por USUBJID
# — pseudo-AVISITN seria inútil e causaria mismatch.
add_pseudo_visit <- function(df, dataset_name) {

  if (!("USUBJID" %in% names(df))) return(df)
  if ("AVISITN"  %in% names(df)) return(df)

  if (dataset_name %in% DATASETS_TALL_FORMAT) {
    log_info(paste0("[", dataset_name, "] Tall sem AVISITN — sem pseudo-visita (join por USUBJID)"))
    return(df)
  }

  log_warn(paste("Criando pseudo AVISITN para:", dataset_name))

  order_map <- list(
    adae     = c("ASTDT", "AESEQ"),
    adbdc    = c("ASTDT", "ASTDTC"),
    addisp   = c("ASTDT", "ASTDTC"),
    adice    = c("ADT", "ADY"),
    adrxfail = c("PARAMCD"),
    adrxhist = c("ASEQ", "CMSEQ")
  )

  if (dataset_name == "adsl") {
    df <- df %>% mutate(AVISITN = 0L, AVISIT = "BASELINE")
    return(df)
  }

  preferred  <- order_map[[dataset_name]]
  if (is.null(preferred)) preferred <- c("ADT", "ASTDT", "ASTDTC")
  order_cols <- intersect(preferred, names(df))

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

  return(df)
}

# ===============================
# 3. BUILD SPINE
# ===============================
build_spine <- function(datasets) {

  spine_parts <- list()

  if ("advscdai" %in% names(datasets)) {
    spine_parts[["advscdai"]] <- datasets[["advscdai"]] %>%
      select(any_of(c("USUBJID", "AVISIT", "AVISITN"))) %>%
      distinct()
    log_info("Usando advscdai como âncora principal da spine")
  }

  for (nm in c("adcdai", "adlbef", "adex", "adsaf")) {
    if (nm %in% names(datasets)) {
      df <- datasets[[nm]]
      if (all(c("USUBJID", "AVISITN") %in% names(df))) {
        spine_parts[[nm]] <- df %>%
          select(any_of(c("USUBJID", "AVISIT", "AVISITN"))) %>%
          distinct()
        log_info(paste("Complementando spine com:", nm))
      }
    }
  }

  spine <- bind_rows(spine_parts) %>% distinct()

  if ("adsl" %in% names(datasets)) {
    subj_adsl <- datasets[["adsl"]] %>%
      select(USUBJID) %>%
      distinct() %>%
      mutate(AVISITN = 0L, AVISIT = "BASELINE")
    spine <- bind_rows(spine, subj_adsl) %>% distinct()
  }

  log_info(paste("Spine criada com", nrow(spine), "linhas e",
                 n_distinct(spine$USUBJID), "sujeitos únicos"))
  return(spine)
}

# ===============================
# 4. CLASSIFICATION (datasets wide)
# ===============================
classify_variable <- function(df, var) {

  tmp <- df %>%
    select(USUBJID, all_of(var)) %>%
    distinct()

  variability <- tmp %>%
    group_by(USUBJID) %>%
    summarise(n = n_distinct(.data[[var]]), .groups = "drop")

  if (any(variability$n > 1, na.rm = TRUE)) {
    return("LONGITUDINAL")
  } else {
    return("STATIC")
  }
}

# ===============================
# 5. SMART AGGREGATION (datasets wide)
# ===============================
aggregate_value <- function(x) {
  if (is.numeric(x))                   return(mean(x, na.rm = TRUE))
  if (is.character(x) || is.factor(x)) return(first(na.omit(x)))
  return(first(x))
}

# ===============================
# 6A. DETECT TALL FORMAT
# ===============================
detect_tall_format <- function(df, dataset_name) {

  if (dataset_name %in% DATASETS_TALL_FORMAT) return(TRUE)

  # Safety net: PARAMCD presente + múltiplas linhas por chave
  key_cols <- intersect(c("USUBJID", "AVISITN"), names(df))
  if ("PARAMCD" %in% names(df) && length(key_cols) >= 1) {
    rows_per_key <- df %>% count(across(all_of(key_cols))) %>% pull(n)
    if (median(rows_per_key) > 1) {
      log_warn(paste(dataset_name, "detectado como tall automaticamente"))
      return(TRUE)
    }
  }

  return(FALSE)
}

# ===============================
# 6B. PIVOT WIDE BY PARAMCD
# ===============================
# Converte dataset BDS paramétrico (tall) para wide, 1 linha por
# USUBJID (+ AVISITN se existir no dataset de origem).
#
# AVAL numérico  → AVAL_{DS}_{PARAMCD}   (média se duplicatas)
# AVALC character→ AVALC_{DS}_{PARAMCD}  (concat " | " se duplicatas)
#
# [BUG-4 v3] Funciona mesmo quando só AVALC existe (caso addisp):
#   sem AVAL → só o bloco AVALC é executado.
#   PARAMCD sanitizado para nome de coluna válido.
#
# [BUG-5 v3] key_cols = intersect(c("USUBJID","AVISITN"), names(df))
#   → datasets sem AVISITN usam só USUBJID como chave, sem pseudo.
pivot_wide_by_paramcd <- function(df, dataset_name) {

  DS <- toupper(dataset_name)

  # Chaves do pivot: só AVISITN se realmente existir no df
  key_cols    <- intersect(c("USUBJID", "AVISITN"), names(df))
  has_avisitn <- "AVISITN" %in% key_cols

  has_paramcd <- "PARAMCD" %in% names(df)
  has_aval    <- "AVAL"    %in% names(df)
  has_avalc   <- "AVALC"   %in% names(df)

  non_pivot_meta <- c("USUBJID", "AVISIT", "AVISITN",
                      "PARAMCD", "PARAM", "AVAL", "AVALC",
                      "PARAMN", "DTYPE")

  # ---- Colunas demográficas/metadados: STC_/AVAL_ normal ----
  other_cols <- setdiff(names(df), non_pivot_meta)
  extracted  <- list()
  used_vars  <- c()

  for (v in other_cols) {
    class_type <- classify_variable(df, v)
    if (class_type == "LONGITUDINAL" && has_avisitn) {
      tmp <- df %>%
        select(USUBJID, AVISITN, value = all_of(v)) %>%
        group_by(USUBJID, AVISITN) %>%
        summarise(value = aggregate_value(value), .groups = "drop")
      colname <- paste0("AVAL_", DS, "_", v)
      names(tmp)[3] <- colname
      extracted[[colname]] <- tmp
    } else {
      tmp <- df %>%
        select(USUBJID, value = all_of(v)) %>%
        group_by(USUBJID) %>%
        summarise(value = aggregate_value(value), .groups = "drop")
      colname <- paste0("STC_", DS, "_", v)
      names(tmp)[2] <- colname
      extracted[[colname]] <- tmp
    }
    used_vars <- c(used_vars, v)
  }

  pivot_cols_generated <- character(0)

  # ---- Pivot AVAL numérico ----
  if (has_paramcd && has_aval) {
    df_p <- df %>%
      mutate(PARAMCD_clean = str_replace_all(toupper(PARAMCD), "[^A-Z0-9_]", "_"))

    aval_wide <- df_p %>%
      select(all_of(key_cols), PARAMCD_clean, AVAL) %>%
      group_by(across(all_of(c(key_cols, "PARAMCD_clean")))) %>%
      summarise(AVAL = mean(AVAL, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(
        id_cols     = all_of(key_cols),
        names_from  = PARAMCD_clean,
        values_from = AVAL,
        names_prefix = paste0("AVAL_", DS, "_")
      )

    pivot_cols_generated <- c(pivot_cols_generated,
                               setdiff(names(aval_wide), key_cols))
    extracted[["__aval_pivot__"]] <- aval_wide
    used_vars <- c(used_vars, "AVAL")
  }

  # ---- Pivot AVALC character ----
  # [BUG-4 v3] Bloco independente — executa mesmo sem AVAL.
  # Duplicatas PARAMCD/sujeito (ex: addisp DCTREAS com 2 registros)
  # são concatenadas com " | " para não perder nenhum valor.
  if (has_paramcd && has_avalc) {
    df_p <- df %>%
      mutate(PARAMCD_clean = str_replace_all(toupper(PARAMCD), "[^A-Z0-9_]", "_"))

    concat_unique <- function(x) {
      x <- na.omit(x)
      if (length(x) == 0) return(NA_character_)
      vals <- unique(x)
      if (length(vals) == 1) return(vals)
      return(paste(sort(vals), collapse = " | "))
    }

    avalc_wide <- df_p %>%
      select(all_of(key_cols), PARAMCD_clean, AVALC) %>%
      group_by(across(all_of(c(key_cols, "PARAMCD_clean")))) %>%
      summarise(AVALC = concat_unique(AVALC), .groups = "drop") %>%
      pivot_wider(
        id_cols     = all_of(key_cols),
        names_from  = PARAMCD_clean,
        values_from = AVALC,
        names_prefix = paste0("AVALC_", DS, "_")
      )

    pivot_cols_generated <- c(pivot_cols_generated,
                               setdiff(names(avalc_wide), key_cols))
    extracted[["__avalc_pivot__"]] <- avalc_wide
    used_vars <- c(used_vars, "AVALC")
  }

  for (dcol in intersect(c("PARAM", "PARAMCD", "DTYPE", "PARAMN"), names(df))) {
    used_vars <- c(used_vars, dcol)
  }

  all_vars <- setdiff(names(df), c("USUBJID", "AVISIT", "AVISITN"))

  log_info(paste0("[", dataset_name, "] Pivot tall→wide: ",
                  length(pivot_cols_generated), " colunas PARAMCD geradas"))

  return(list(
    extracted            = extracted,
    used_vars            = unique(used_vars),
    all_vars             = all_vars,
    n_params_pivoted     = length(pivot_cols_generated),
    is_tall              = TRUE,
    pivot_cols_generated = pivot_cols_generated
  ))
}

# ===============================
# 6C. EXTRACT WIDE DATASET
# ===============================
extract_wide_dataset <- function(df, dataset_name) {

  vars <- setdiff(names(df), c("USUBJID", "AVISIT", "AVISITN"))
  DS   <- toupper(dataset_name)

  extracted  <- list()
  used_vars  <- c()

  for (v in vars) {
    class_type <- classify_variable(df, v)

    if (class_type == "LONGITUDINAL") {
      tmp <- df %>%
        select(USUBJID, AVISITN, value = all_of(v)) %>%
        group_by(USUBJID, AVISITN) %>%
        summarise(value = aggregate_value(value), .groups = "drop")
      colname <- paste0("AVAL_", DS, "_", v)
      names(tmp)[3] <- colname
      extracted[[colname]] <- tmp
    } else {
      tmp <- df %>%
        select(USUBJID, value = all_of(v)) %>%
        group_by(USUBJID) %>%
        summarise(value = aggregate_value(value), .groups = "drop")
      colname <- paste0("STC_", DS, "_", v)
      names(tmp)[2] <- colname
      extracted[[colname]] <- tmp
    }
    used_vars <- c(used_vars, v)
  }

  return(list(
    extracted            = extracted,
    used_vars            = used_vars,
    all_vars             = vars,
    n_params_pivoted     = 0L,
    is_tall              = FALSE,
    pivot_cols_generated = character(0)
  ))
}

# ===============================
# 6. EXTRACT DATASET (dispatcher)
# ===============================
extract_dataset <- function(df, dataset_name) {

  if (detect_tall_format(df, dataset_name)) {
    log_info(paste0("[", dataset_name, "] Tall/BDS paramétrico → pivot_wide_by_paramcd()"))
    return(pivot_wide_by_paramcd(df, dataset_name))
  } else {
    log_info(paste0("[", dataset_name, "] Wide → extract_wide_dataset()"))
    return(extract_wide_dataset(df, dataset_name))
  }
}

# ===============================
# 7. MERGE ARD
# ===============================
merge_ard <- function(spine, extracted_all) {

  ard <- spine

  for (ds in extracted_all) {
    for (obj in ds$extracted) {
      has_avisitn <- "AVISITN" %in% names(obj)
      if (has_avisitn) {
        ard <- left_join(ard, obj, by = c("USUBJID", "AVISITN"))
      } else {
        ard <- left_join(ard, obj, by = "USUBJID")
      }
    }
  }

  dup_check <- ard %>% count(USUBJID, AVISITN) %>% filter(n > 1)
  if (nrow(dup_check) > 0) {
    log_warn(paste("Duplicatas USUBJID × AVISITN:", nrow(dup_check), "combinações"))
  }

  log_info(paste("ARD final:", nrow(ard), "linhas ×", ncol(ard), "colunas"))
  return(ard)
}

# ===============================
# 8. COVERAGE REPORT v3
# ===============================
# Compara o preenchimento do dataset de ORIGEM com o preenchimento no
# ARD final — permite detectar perda de dados no pipeline.
#
# fill_efficiency = (ard_cells_filled / src_cells_filled) × 100
#   = 100% → sem perda
#   > 100% → colunas STC_ replicadas por visita na spine (esperado)
#   << 100% → indica join nulo, pivot falho ou colapso de valores
build_coverage <- function(extracted_all, dataset_names, ard = NULL) {

  report <- map2_df(extracted_all, dataset_names, function(ds, name) {

    total       <- length(ds$all_vars)
    n_extracted <- length(ds$used_vars)
    missing     <- setdiff(ds$all_vars, ds$used_vars)
    src         <- SOURCE_SHAPES[[name]]

    tibble(
      dataset           = name,
      total_vars        = total,
      extracted_vars    = n_extracted,
      missing_vars      = total - n_extracted,
      extraction_rate   = round(100 * n_extracted / total, 2),
      is_tall_format    = ds$is_tall,
      n_params_pivoted  = ds$n_params_pivoted,
      src_rows          = if (!is.null(src)) src$rows         else NA_integer_,
      src_cols          = if (!is.null(src)) src$cols         else NA_integer_,
      src_cells_total   = if (!is.null(src)) src$cells_total  else NA_integer_,
      src_cells_filled  = if (!is.null(src)) src$cells_filled else NA_integer_,
      src_fill_pct      = if (!is.null(src)) round(100 * src$cells_filled / src$cells_total, 2) else NA_real_,
      missing_var_names = paste(missing, collapse = ", ")
    )
  })

  if (!is.null(ard)) {
    ard_metrics <- map2_df(extracted_all, dataset_names, function(ds, name) {
      DS      <- toupper(name)
      ds_cols <- names(ard)[
        str_starts(names(ard), paste0("STC_",   DS, "_")) |
        str_starts(names(ard), paste0("AVAL_",  DS, "_")) |
        str_starts(names(ard), paste0("AVALC_", DS, "_"))
      ]

      if (length(ds_cols) == 0) {
        return(tibble(
          dataset          = name,
          ard_cols_count   = 0L,
          ard_cells_total  = NA_integer_,
          ard_cells_filled = NA_integer_,
          ard_fill_pct     = NA_real_,
          fill_efficiency  = NA_real_
        ))
      }

      ard_sub          <- ard %>% select(all_of(ds_cols))
      ard_cells_total  <- as.integer(nrow(ard) * length(ds_cols))
      ard_cells_filled <- as.integer(sum(!is.na(ard_sub)))
      ard_fill_pct     <- round(100 * ard_cells_filled / ard_cells_total, 2)

      src      <- SOURCE_SHAPES[[name]]
      fill_eff <- if (!is.null(src) && src$cells_filled > 0) {
        round(100 * ard_cells_filled / src$cells_filled, 2)
      } else { NA_real_ }

      tibble(
        dataset          = name,
        ard_cols_count   = length(ds_cols),
        ard_cells_total  = ard_cells_total,
        ard_cells_filled = ard_cells_filled,
        ard_fill_pct     = ard_fill_pct,
        fill_efficiency  = fill_eff
      )
    })

    report <- left_join(report, ard_metrics, by = "dataset")
  }

  print(report, width = 250)
  fwrite(report, OUTPUT_COVERAGE)
  log_info(paste("Coverage report salvo em:", OUTPUT_COVERAGE))

  # Alerta automático: fill_efficiency muito baixo indica perda de dados
  if (!is.null(ard)) {
    low_eff <- report %>%
      filter(!is.na(fill_efficiency), fill_efficiency < 10, src_cells_filled > 100)
    if (nrow(low_eff) > 0) {
      log_warn(paste(
        "Datasets com fill_efficiency < 10% — possível join nulo ou pivot falho:",
        paste(low_eff$dataset, collapse = ", ")
      ))
    }
  }

  return(report)
}

# ===============================
# 9. MAIN PIPELINE
# ===============================
run_pipeline <- function() {

  log_info("=== GALAXI-3 ARD PIPELINE v3 INICIADO ===")

  # 1. Carregar (preenche SOURCE_SHAPES)
  datasets <- load_datasets(DATA_PATH)

  # 2. Pseudo-visita — tall sem AVISITN são pulados automaticamente
  datasets <- imap(datasets, add_pseudo_visit)

  # 3. Spine
  spine <- build_spine(datasets)

  # 4. Extração
  extracted_all <- list()
  for (name in names(datasets)) {
    log_info(paste("Processando dataset:", name))
    tryCatch({
      extracted_all[[name]] <- extract_dataset(datasets[[name]], name)
    }, error = function(e) {
      log_warn(paste("Erro ao processar", name, ":", conditionMessage(e)))
    })
  }

  # 5. Montar ARD
  ard <- merge_ard(spine, extracted_all)

  # 6. Salvar ARD
  fwrite(ard, OUTPUT_ARD)
  log_info(paste("ARD salvo em:", OUTPUT_ARD))

  # 7. Coverage report com métricas de células
  build_coverage(extracted_all, names(datasets), ard = ard)

  log_info("=== PIPELINE v3 CONCLUÍDO ===")
  invisible(ard)
}

# ===============================
# RUN
# ===============================
run_pipeline()
