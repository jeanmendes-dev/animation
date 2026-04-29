# ============================================================
# CLINICAL ADaM → ML-READY ARD PIPELINE
# STUDY: CNTO1275UCO3001-UNIFI / load-1899
# ============================================================
#
# Baseado em: extract_all_galaxi3_v3.R
# Adaptado para UNIFI em 2025
#
# DIFERENÇAS ESTRUTURAIS vs GALAXI-3:
#
#   1. DATASETS CSV (não SAS7BDAT)
#      Os arquivos têm extensão .sas7bdat mas são lidos como CSV
#      via fread(sep="auto") — o nome do arquivo é preservado do
#      source SAS mas o conteúdo foi exportado como CSV.
#      A função load_datasets() prioriza .csv e o naming tolower()
#      do basename sem extensão resolve isso automaticamente.
#
#   2. ÂNCORA DA SPINE: admayo (não advscdai)
#      admayo é o dataset longitudinal principal do UNIFI:
#      790.256 linhas, 45 PARAMCDs de Mayo Score cobrindo todas
#      as visitas. É a âncora que define o universo de
#      USUBJID × AVISITN da spine.
#      Complementos: adchem, adhema, adlbef, adsaf, advs.
#
#   3. 33 DATASETS vs 12 do GALAXI-3
#      O UNIFI tem escopo muito maior, incluindo datasets de:
#      - Qualidade de vida (adeq5d, adibdq, adsf36)
#      - Imunogenicidade (adis)
#      - PK/farmacocinética (adpc)
#      - Resultados econômicos (adecon)
#      - Cirurgia (adsg), hospitalização (adho, adhist*)
#      - Histórico médico (admh), desvios (addv)
#      - Mortalidade (addeath — apenas 2 linhas)
#
#   4. ADCORT: wide sem PARAMCD, 306.112 linhas
#      Dataset de dose diária de corticosteroide. Sem AVISITN real,
#      sem PARAMCD — tratado como wide com pseudo-AVISITN.
#
#   5. ADTTE: tall sem AVISITN
#      Time-to-event com PARAMCD+AVAL mas sem AVISITN —
#      AVAL representa tempo até evento, não uma medida por visita.
#      Incluso em DATASETS_TALL_FORMAT; join por USUBJID.
#
#   6. ADEFF: tall sem AVISITN, 51 PARAMCDs
#      Endpoints de eficácia derivados, sem AVISITN. Pivot por
#      PARAMCD gera ~51 colunas STC_ subject-level.
#
# LÓGICA GERAL (idêntica ao galaxi3_v3):
#   - Datasets BDS paramétricos (PARAMCD + AVAL/AVALC):
#     → pivot_wide_by_paramcd() → 1 linha/USUBJID(+AVISITN)
#   - Datasets wide (sem PARAMCD):
#     → extract_wide_dataset() → classify STC_/AVAL_
#   - Coverage report com fill_efficiency para detectar perda de dados
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
DATA_PATH <- "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1899/data"

OUTPUT_ARD      <- "/mnt/unifi_ml_dataset.csv"
OUTPUT_COVERAGE <- "/mnt/unifi_ml_coverage_report.csv"

# ---------------------------------------------------------------
# EXPECTED_DATASETS — 33 datasets do UNIFI / load-1899
# ---------------------------------------------------------------
EXPECTED_DATASETS <- c(
  "adae",      # Adverse Events              (102 cols,  3765 rows)   — OCCDS, sem PARAMCD
  "adbdc",     # Baseline Disease Char.      (45 cols,  52839 rows)   — BDS tall, 56 PARAMCDs, sem AVISITN
  "adchem",    # Chemistry Labs              (83 cols, 312889 rows)   — BDS tall, 19 PARAMCDs, AVISITN real
  "adcm",      # Concurrent Medications      (32 cols,  24025 rows)   — BDS tall, 25 PARAMCDs, só AVALC, sem AVISITN
  "adcort",    # Corticosteroid Dose (daily) (34 cols, 306112 rows)   — wide, sem PARAMCD, sem AVISITN
  "addeath",   # Deaths                      (38 cols,      2 rows)   — wide, sem PARAMCD (só 2 linhas)
  "adds",      # Disposition/Study Status    (64 cols,   5422 rows)   — wide, sem PARAMCD
  "addv",      # Protocol Deviations         (53 cols,    244 rows)   — wide, sem PARAMCD
  "adecon",    # Economic Outcomes           (56 cols,  71860 rows)   — BDS tall, 10 PARAMCDs, AVISITN real
  "adeff",     # Efficacy Endpoints          (41 cols,  40716 rows)   — BDS tall, 51 PARAMCDs, só AVAL+AVALC, sem AVISITN
  "adeq5d",    # EQ-5D Quality of Life       (56 cols, 100548 rows)   — BDS tall, 14 PARAMCDs, AVISITN real
  "adex",      # Exposure/Dosing             (89 cols,   7427 rows)   — wide, AVISITN real, sem PARAMCD BDS
  "adhema",    # Hematology                  (80 cols, 339219 rows)   — BDS tall, 42 PARAMCDs, AVISITN real
  "adhist",    # Hospitalization History     (33 cols,   4698 rows)   — BDS tall, 6 PARAMCDs, só AVALC, sem AVISITN
  "adhist1",   # Medical History 1           (33 cols,   4197 rows)   — BDS tall, 6 PARAMCDs, só AVAL, sem AVISITN
  "adhist2",   # Medical History 2           (34 cols,   9396 rows)   — BDS tall, 12 PARAMCDs, AVAL+AVALC, sem AVISITN
  "adho",      # Hospitalizations            (45 cols,    154 rows)   — wide, sem PARAMCD
  "adibdq",    # IBDQ Questionnaire          (52 cols,  84746 rows)   — BDS tall, 13 PARAMCDs, AVISITN real
  "adis",      # Immunogenicity Serology     (56 cols,  22941 rows)   — BDS tall, 10 PARAMCDs, AVISITN real
  "adlbef",    # Efficacy Labs               (57 cols, 152994 rows)   — BDS tall, 18 PARAMCDs, AVISITN real
  "admayo",    # Mayo Score (âncora spine)   (57 cols, 790256 rows)   — BDS tall, 45 PARAMCDs, AVISITN real
  "admh",      # Medical History             (52 cols,  27869 rows)   — wide, sem PARAMCD
  "adpc",      # PK Concentrations           (66 cols,  12566 rows)   — BDS tall, 1 PARAMCD (CONC), AVISITN real
  "adsaf",     # Safety Exposure             (56 cols,  10174 rows)   — BDS tall, 9 PARAMCDs, AVISITN real
  "adsf36",    # SF-36 QoL                   (52 cols, 154740 rows)   — BDS tall, 22 PARAMCDs, AVISITN real
  "adsg",      # Surgery                     (44 cols,   6454 rows)   — wide, sem PARAMCD
  "adsl",      # Subject-Level Baseline      (107 cols,  1331 rows)   — wide, sem PARAMCD
  "adtfi",     # Treatment Failure Induction (41 cols,      8 rows)   — wide, AVISITN real (só 8 linhas)
  "adtfm",     # Treatment Failure Maint.    (31 cols,    292 rows)   — wide, AVISITN real
  "adtte",     # Time-to-Event               (34 cols,   3481 rows)   — BDS tall, 6 PARAMCDs, sem AVISITN (AVAL=tempo)
  "aduceis",   # UCEIS Endoscopy Score       (56 cols,  38430 rows)   — BDS tall, 7 PARAMCDs, AVISITN real
  "advs",      # Vital Signs                 (55 cols,  82506 rows)   — BDS tall, 7 PARAMCDs, AVISITN real
  "advscort"   # Corticosteroid Score Long.  (43 cols,  39960 rows)   — BDS tall, 3 PARAMCDs, AVISITN real
)

# ---------------------------------------------------------------
# DATASETS_SEM_AVISITN — não têm AVISITN real no source
# add_pseudo_visit() é chamado para datasets wide desta lista.
# Datasets tall sem AVISITN são pulados automaticamente (ver BUG-5).
# ---------------------------------------------------------------
DATASETS_SEM_AVISITN <- c(
  "adae",      # OCCDS — sem AVISITN (ordenar por ASTDT + AESEQ)
  "adbdc",     # BDS tall — sem AVISITN (pivot só por USUBJID)
  "adcm",      # BDS tall — sem AVISITN
  "adcort",    # wide — pseudo por ADT
  "addeath",   # wide — sem AVISITN (2 linhas)
  "adds",      # wide — pseudo por DSSTDT
  "addv",      # wide — pseudo por DVSTDT
  "adeff",     # BDS tall — sem AVISITN (pivot por USUBJID)
  "adhist",    # BDS tall — sem AVISITN
  "adhist1",   # BDS tall — sem AVISITN
  "adhist2",   # BDS tall — sem AVISITN
  "adho",      # wide — pseudo por ASTDT
  "admh",      # wide — pseudo por MHSTDT / MHSEQ
  "adsg",      # wide — pseudo por ASTDT / SGSEQ
  "adsl",      # wide — BASELINE único por sujeito
  "adtte"      # BDS tall — sem AVISITN (AVAL = tempo até evento)
)

# ---------------------------------------------------------------
# DATASETS_COM_AVISITN — têm AVISITN real no source
# ---------------------------------------------------------------
DATASETS_COM_AVISITN <- c(
  "adchem",
  "adecon",
  "adeq5d",
  "adex",
  "adhema",
  "adibdq",
  "adis",
  "adlbef",
  "admayo",
  "adpc",
  "adsaf",
  "adsf36",
  "adtfi",
  "adtfm",
  "aduceis",
  "advs",
  "advscort"
)

# ---------------------------------------------------------------
# DATASETS_TALL_FORMAT — BDS paramétricos (PARAMCD + AVAL e/ou AVALC)
#
# Regra: entra todo dataset que usa PARAMCD para identificar
# o parâmetro medido e AVAL/AVALC para o valor.
#
# NÃO entram:
#   adae    → OCCDS: AEs com AESEQ, sem PARAMCD BDS
#   adcort  → wide: dose diária, sem PARAMCD
#   addeath → wide: mortes, sem PARAMCD
#   adds    → wide: disposição, sem PARAMCD
#   addv    → wide: desvios, sem PARAMCD
#   adex    → wide: exposição com AVISITN mas sem PARAMCD BDS
#   adho    → wide: hospitalizações, sem PARAMCD
#   admh    → wide: histórico médico, sem PARAMCD
#   adsg    → wide: cirurgia, sem PARAMCD
#   adsl    → wide: subject-level baseline
#   adtfi   → wide: treatment failure induction (sem PARAMCD)
#   adtfm   → wide: treatment failure maintenance (sem PARAMCD)
# ---------------------------------------------------------------
DATASETS_TALL_FORMAT <- c(
  # --- Com AVISITN real ---
  "admayo",    # ÂNCORA — 45 PARAMCDs Mayo Score por visita
  "adchem",    # 19 PARAMCDs chemistry labs por visita
  "adhema",    # 42 PARAMCDs hematology por visita
  "adlbef",    # 18 PARAMCDs efficacy labs por visita
  "adibdq",    # 13 PARAMCDs IBDQ questionnaire por visita
  "adeq5d",    # 14 PARAMCDs EQ-5D QoL por visita
  "adsf36",    # 22 PARAMCDs SF-36 QoL por visita
  "adecon",    # 10 PARAMCDs economic outcomes por visita
  "adis",      # 10 PARAMCDs immunogenicity por visita
  "aduceis",   # 7 PARAMCDs UCEIS endoscopy por visita
  "advs",      # 7 PARAMCDs vital signs por visita
  "adsaf",     # 9 PARAMCDs safety exposure por visita
  "advscort",  # 3 PARAMCDs corticosteroid score por visita
  "adpc",      # 1 PARAMCD (CONC) PK concentrations por visita
  # --- Sem AVISITN (pivot só por USUBJID → colunas STC_/AVALC_) ---
  "adbdc",     # 56 PARAMCDs baseline disease — sem AVISITN
  "adeff",     # 51 PARAMCDs efficacy endpoints — sem AVISITN
  "adcm",      # 25 PARAMCDs concurrent medications — só AVALC, sem AVISITN
  "adhist",    # 6 PARAMCDs hospitalization history — só AVALC, sem AVISITN
  "adhist1",   # 6 PARAMCDs medical history 1 — só AVAL, sem AVISITN
  "adhist2",   # 12 PARAMCDs medical history 2 — AVAL+AVALC, sem AVISITN
  "adtte"      # 6 PARAMCDs time-to-event — sem AVISITN (AVAL=tempo)
)

# ===============================
# LOGGING
# ===============================
log_warn <- function(msg) warning(paste0("[WARNING] ", msg))
log_info <- function(msg) message(paste0("[INFO] ", msg))

# ===============================
# 1. LOAD DATASETS
# ===============================
# SOURCE_SHAPES: registra shape e preenchimento de cada dataset de origem
# para cálculo de fill_efficiency no coverage report.
SOURCE_SHAPES <- list()

load_datasets <- function(path) {

  # Prioriza .csv; fallback para .sas7bdat (lido via haven)
  csv_files <- list.files(path, pattern = "\\.csv$",
                          full.names = TRUE, recursive = TRUE)
  sas_files <- list.files(path, pattern = "\\.sas7bdat$",
                          full.names = TRUE, recursive = TRUE)

  if (length(csv_files) > 0) {
    files   <- csv_files
    read_fn <- function(f) as.data.frame(fread(f, sep = "auto"))
    log_info("Lendo arquivos CSV via data.table::fread")
  } else if (length(sas_files) > 0) {
    if (!requireNamespace("haven", quietly = TRUE)) {
      stop("Pacote 'haven' necessário para SAS7BDAT. Instale com: install.packages('haven')")
    }
    files   <- sas_files
    read_fn <- function(f) as.data.frame(haven::read_sas(f))
    log_info("Lendo arquivos SAS7BDAT via haven")
  } else {
    stop(paste("Nenhum arquivo .csv ou .sas7bdat encontrado em:", path))
  }

  datasets <- lapply(files, function(f) {
    tryCatch(read_fn(f), error = function(e) {
      log_warn(paste("Falha ao ler:", f, "—", conditionMessage(e)))
      return(NULL)
    })
  })

  # tolower do basename sem extensão → "adae.sas7bdat" → "adae"
  names(datasets) <- tolower(tools::file_path_sans_ext(basename(files)))
  datasets <- datasets[!sapply(datasets, is.null)]

  # Registrar shape original para o coverage report
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
    log_warn(paste("Datasets esperados não encontrados:",
                   paste(missing_ds, collapse = ", ")))
  }

  extra_ds <- setdiff(names(datasets), EXPECTED_DATASETS)
  if (length(extra_ds) > 0) {
    log_warn(paste("Datasets fora do escopo UNIFI:",
                   paste(extra_ds, collapse = ", ")))
  }

  log_info(paste("Carregados", length(datasets), "datasets"))
  return(datasets)
}

# ===============================
# 2. ADD PSEUDO VISIT
# ===============================
# Adiciona AVISITN fictício (row_number por sujeito) apenas para
# datasets WIDE sem AVISITN real.
# Datasets TALL sem AVISITN são pulados — o pivot os reduz a
# 1 linha/USUBJID e o join é feito exclusivamente por USUBJID.
add_pseudo_visit <- function(df, dataset_name) {

  if (!("USUBJID" %in% names(df))) return(df)
  if ("AVISITN"  %in% names(df)) return(df)

  # Datasets tall: sem pseudo-visita (join por USUBJID após pivot)
  if (dataset_name %in% DATASETS_TALL_FORMAT) {
    log_info(paste0("[", dataset_name, "] Tall sem AVISITN — sem pseudo-visita"))
    return(df)
  }

  log_warn(paste("Criando pseudo AVISITN para:", dataset_name))

  # Colunas de ordenação preferenciais por dataset
  order_map <- list(
    adae     = c("ASTDT", "AESEQ"),
    adcort   = c("ADT"),
    addeath  = c("ADT"),
    adds     = c("DSSTDT", "DSSEQ"),
    addv     = c("DVSTDT", "DVSEQ"),
    adho     = c("ASTDT", "HOSEQ"),
    admh     = c("MHSTDT", "MHSEQ"),
    adsg     = c("ASTDT", "SGSEQ")
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
# Âncora principal: admayo (45 PARAMCDs Mayo Score, ~790k linhas).
# Complementos: adchem, adhema, adlbef, adsaf, advs, adeq5d, adibdq.
# adsl fornece o universo completo de sujeitos (BASELINE visit = 0).
build_spine <- function(datasets) {

  spine_parts <- list()

  # Âncora principal: admayo
  if ("admayo" %in% names(datasets)) {
    spine_parts[["admayo"]] <- datasets[["admayo"]] %>%
      select(any_of(c("USUBJID", "AVISIT", "AVISITN"))) %>%
      distinct()
    log_info("Usando admayo como âncora principal da spine")
  }

  # Complementos com AVISITN real
  for (nm in c("adchem", "adhema", "adlbef", "adsaf", "advs",
               "adeq5d", "adibdq", "adsf36", "adecon", "aduceis",
               "advscort", "adis", "adpc")) {
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

  # adsl: garante que todos os 1331 sujeitos estejam na spine
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

  if (any(variability$n > 1, na.rm = TRUE)) "LONGITUDINAL" else "STATIC"
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

  # Safety net: PARAMCD + múltiplas linhas por chave
  key_cols <- intersect(c("USUBJID", "AVISITN"), names(df))
  if ("PARAMCD" %in% names(df) && length(key_cols) >= 1) {
    rows_per_key <- df %>% count(across(all_of(key_cols))) %>% pull(n)
    if (median(rows_per_key) > 1) {
      log_warn(paste(dataset_name, "detectado como tall automaticamente — aplicando pivot"))
      return(TRUE)
    }
  }

  return(FALSE)
}

# ===============================
# 6B. PIVOT WIDE BY PARAMCD
# ===============================
# Converte dataset BDS paramétrico (tall) para wide.
# AVAL numérico  → AVAL_{DS}_{PARAMCD}   (média se duplicatas)
# AVALC character→ AVALC_{DS}_{PARAMCD}  (concat " | " se duplicatas)
# Demais colunas → STC_/AVAL_ normal via classify_variable()
pivot_wide_by_paramcd <- function(df, dataset_name) {

  DS       <- toupper(dataset_name)
  key_cols <- intersect(c("USUBJID", "AVISITN"), names(df))
  has_avisitn <- "AVISITN" %in% key_cols
  has_paramcd <- "PARAMCD" %in% names(df)
  has_aval    <- "AVAL"    %in% names(df)
  has_avalc   <- "AVALC"   %in% names(df)

  non_pivot_meta <- c("USUBJID", "AVISIT", "AVISITN",
                      "PARAMCD", "PARAM", "AVAL", "AVALC",
                      "PARAMN", "DTYPE", "PARAMTYP")

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
  # Duplicatas por PARAMCD/sujeito são concatenadas com " | "
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

  # Registrar colunas de metadado sem gerar coluna separada
  for (dcol in intersect(c("PARAM", "PARAMCD", "DTYPE", "PARAMN", "PARAMTYP"), names(df))) {
    used_vars <- c(used_vars, dcol)
  }

  all_vars <- setdiff(names(df), c("USUBJID", "AVISIT", "AVISITN"))

  log_info(paste0("[", dataset_name, "] Pivot tall→wide: ",
                  length(pivot_cols_generated), " colunas geradas por PARAMCD"))

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
    log_info(paste0("[", dataset_name, "] Tall/BDS → pivot_wide_by_paramcd()"))
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
# 8. COVERAGE REPORT
# ===============================
# Métricas:
#   extraction_rate   → % variáveis extraídas vs total no source
#   is_tall_format    → TRUE se processado via pivot
#   n_params_pivoted  → colunas geradas por PARAMCD pivot
#   src_fill_pct      → % células preenchidas no source
#   ard_fill_pct      → % células preenchidas no ARD (completude real)
#   fill_efficiency   → (ard_cells_filled / src_cells_filled) × 100
#                       100% = sem perda | >100% = replicação STC_ (esperado)
#                       << 100% para tall+AVISITN = ESPERADO (denominador inflado)
#                       << 100% para wide = INVESTIGAR
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

  # Alertas: wide datasets com fill_efficiency muito baixo
  if (!is.null(ard)) {
    low_eff <- report %>%
      filter(!is.na(fill_efficiency),
             fill_efficiency < 10,
             src_cells_filled > 100,
             is_tall_format == FALSE)
    if (nrow(low_eff) > 0) {
      log_warn(paste(
        "Wide datasets com fill_efficiency < 10% — possível join nulo:",
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

  log_info("=== UNIFI ARD PIPELINE (v3) INICIADO ===")
  log_info(paste("Study: CNTO1275UCO3001-UNIFI / load-1899"))
  log_info(paste("Data path:", DATA_PATH))

  # 1. Carregar datasets (preenche SOURCE_SHAPES)
  datasets <- load_datasets(DATA_PATH)

  # 2. Pseudo-visita para datasets wide sem AVISITN
  #    Datasets tall sem AVISITN são pulados automaticamente
  datasets <- imap(datasets, add_pseudo_visit)

  # 3. Spine baseada em admayo + complementos + adsl
  spine <- build_spine(datasets)

  # 4. Extração de variáveis (pivot para tall, wide para os demais)
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

  log_info("=== UNIFI ARD PIPELINE CONCLUÍDO ===")
  invisible(ard)
}

# ===============================
# RUN
# ===============================
run_pipeline()
