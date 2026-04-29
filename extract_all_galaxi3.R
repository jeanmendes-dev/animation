# ============================================================
# CLINICAL ADaM → ML-READY ARD PIPELINE
# STUDY: CNTO1959CRD3001-GALAXI-GAL3-WK48
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

# Datasets confirmados no diagnóstico GALAXI-3
# Diferenças vs GALAXI-2:
#   - ADMACE ausente (não existe no GALAXI-3)
#   - ADSAF passa a ter AVISITN real (era pseudo no GALAXI-2)
#   - N sujeitos: 525 (GALAXI-2: 1298)
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
  "adsaf",      # Safety Exposure      (36 cols,  18709 rows) — AVISITN real ✓
  "adsl",       # Subject-Level        (70 cols,  525 rows)
  "advscdai"    # Longitudinal CDAI    (53 cols,  309550 rows)
)

# Datasets sem AVISITN — recebem pseudo-visita
# Nota GALAXI-3: ADSAF agora tem AVISITN real (saiu desta lista vs GALAXI-2)
DATASETS_SEM_AVISITN <- c(
  "adae",       # sem AVISITN — ordenar por ASTDT + AESEQ
  "adbdc",      # sem AVISITN — ordenar por ASTDT / ASTDTC
  "addisp",     # sem AVISITN — ordenar por ASTDT / ASTDTC
  "adice",      # sem AVISITN — ordenar por ADT / ADY
  "adrxfail",   # sem AVISITN — ordenar por PARAMCD
  "adrxhist",   # sem AVISITN — ordenar por ASEQ / CMSEQ
  "adsl"        # sem AVISITN — baseline único por sujeito
)

# Datasets com AVISITN real (âncoras longitudinais)
DATASETS_COM_AVISITN <- c(
  "adcdai",     # AVISITN real ✓ — 159900 linhas
  "adex",       # AVISITN real ✓ — 14871 linhas
  "adlbef",     # AVISITN real ✓ — 60351 linhas
  "adsaf",      # AVISITN real ✓ — 18709 linhas  [novo vs GALAXI-2]
  "advscdai"    # AVISITN real ✓ — 309550 linhas (âncora principal)
)

# ===============================
# LOGGING
# ===============================
log_warn <- function(msg) warning(paste0("[WARNING] ", msg))
log_info <- function(msg) message(paste0("[INFO] ", msg))

# ===============================
# 1. LOAD DATASETS
# ===============================
# Suporta SAS7BDAT (via haven) e CSV (via data.table).
# Os nomes são normalizados para lowercase sem extensão,
# garantindo que "adae.sas7bdat" vire "adae" em todos os
# lookups downstream (switch, %in%, imap, etc.).
load_datasets <- function(path) {

  # Prioridade: SAS7BDAT > CSV (evita duplicatas se ambos existirem)
  sas_files <- list.files(path, pattern = "\\.sas7bdat$",
                          full.names = TRUE, recursive = TRUE)
  csv_files <- list.files(path, pattern = "\\.csv$",
                          full.names = TRUE, recursive = TRUE)

  # Usar SAS se disponível, senão CSV
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

  # Normaliza nomes: lowercase, sem extensão, sem path
  # "adae.sas7bdat" → "adae"  |  "adae.csv" → "adae"
  names(datasets) <- tolower(tools::file_path_sans_ext(basename(files)))
  datasets <- datasets[!sapply(datasets, is.null)]

  # Avisar sobre datasets esperados mas não encontrados
  missing_ds <- setdiff(EXPECTED_DATASETS, names(datasets))
  if (length(missing_ds) > 0) {
    log_warn(paste("Datasets esperados não encontrados:", paste(missing_ds, collapse = ", ")))
  }

  # Avisar sobre datasets encontrados mas não esperados
  extra_ds <- setdiff(names(datasets), EXPECTED_DATASETS)
  if (length(extra_ds) > 0) {
    log_warn(paste("Datasets encontrados fora do escopo do diagnóstico:", paste(extra_ds, collapse = ", ")))
  }

  log_info(paste("Carregados", length(datasets), "datasets"))
  return(datasets)
}

# ===============================
# 2. ADD PSEUDO VISIT (CRÍTICO)
# ===============================
# Apenas datasets sem AVISITN real recebem pseudo-visita.
# No GALAXI-3, ADSAF tem AVISITN real e NÃO entra mais aqui.
# Estratégia de ordenação por dataset:
#   adae     → ASTDT + AESEQ
#   adbdc    → ASTDT / ASTDTC
#   addisp   → ASTDT / ASTDTC
#   adice    → ADT / ADY
#   adrxfail → PARAMCD         (sem coluna de data)
#   adrxhist → ASEQ / CMSEQ
#   adsl     → AVISITN = 0 fixo (uma linha por sujeito)
add_pseudo_visit <- function(df, dataset_name) {

  if (!("USUBJID" %in% names(df))) return(df)
  if ("AVISITN"  %in% names(df)) return(df)

  log_warn(paste("Criando pseudo AVISITN para:", dataset_name))

  # Mapa de colunas de ordenação preferidas por dataset
  order_map <- list(
    adae     = c("ASTDT", "AESEQ"),
    adbdc    = c("ASTDT", "ASTDTC"),
    addisp   = c("ASTDT", "ASTDTC"),
    adice    = c("ADT", "ADY"),
    adrxfail = c("PARAMCD"),
    adrxhist = c("ASEQ", "CMSEQ")
  )

  if (dataset_name == "adsl") {
    # ADSL: baseline único por sujeito
    df <- df %>% mutate(AVISITN = 0L, AVISIT = "BASELINE")
    return(df)
  }

  # Colunas de ordenação: preferidas pelo mapa → fallback genérico → nenhuma
  preferred  <- order_map[[dataset_name]]
  if (is.null(preferred)) {
    preferred <- c("ADT", "ASTDT", "ASTDTC")   # fallback genérico
  }
  order_cols <- intersect(preferred, names(df))  # só colunas que existem no df

  if (length(order_cols) > 0) {
    df <- df %>%
      arrange(USUBJID, across(all_of(order_cols))) %>%
      group_by(USUBJID) %>%
      mutate(AVISITN = row_number(), AVISIT = paste0("PSEUDO_", AVISITN)) %>%
      ungroup()
  } else {
    # Nenhuma coluna de ordenação disponível: sequência por ordem de leitura
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
# Âncora principal: ADVSCDAI (309.550 linhas, PARAMCDs ricos:
#   CDAI, AP, SF, PRO2, CREM*, CRES*, PRO2REMP/S, WTAx)
# Complementada por ADCDAI, ADLBEF, ADEX e ADSAF (que no GALAXI-3
# tem AVISITN real — 18.709 linhas com visitas de safety).
# ADSL garante representação de todos os 525 sujeitos.
build_spine <- function(datasets) {

  spine_parts <- list()

  # Âncora primária: ADVSCDAI
  if ("advscdai" %in% names(datasets)) {
    spine_parts[["advscdai"]] <- datasets[["advscdai"]] %>%
      select(any_of(c("USUBJID", "AVISIT", "AVISITN"))) %>%
      distinct()
    log_info("Usando advscdai como âncora principal da spine")
  }

  # Âncoras secundárias com AVISITN real
  # ADSAF entra aqui no GALAXI-3 (tinha pseudo-visita no GALAXI-2)
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

  # Garantir todos os sujeitos de ADSL (N = 525)
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
# 4. CLASSIFICATION
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
# 5. SMART AGGREGATION
# ===============================
aggregate_value <- function(x) {

  if (is.numeric(x)) {
    return(mean(x, na.rm = TRUE))
  }

  if (is.character(x) || is.factor(x)) {
    return(first(na.omit(x)))
  }

  return(first(x))
}

# ===============================
# 6. EXTRACT VARIABLES
# ===============================
extract_dataset <- function(df, dataset_name) {

  vars <- setdiff(names(df), c("USUBJID", "AVISIT", "AVISITN"))

  extracted  <- list()
  used_vars  <- c()

  for (v in vars) {

    class_type <- classify_variable(df, v)

    if (class_type == "LONGITUDINAL") {

      tmp <- df %>%
        select(USUBJID, AVISITN, value = all_of(v)) %>%
        group_by(USUBJID, AVISITN) %>%
        summarise(value = aggregate_value(value), .groups = "drop")

      colname <- paste0("AVAL_", toupper(dataset_name), "_", v)
      names(tmp)[3] <- colname

      extracted[[colname]] <- tmp
      used_vars <- c(used_vars, v)

    } else {

      tmp <- df %>%
        select(USUBJID, value = all_of(v)) %>%
        group_by(USUBJID) %>%
        summarise(value = aggregate_value(value), .groups = "drop")

      colname <- paste0("STC_", toupper(dataset_name), "_", v)
      names(tmp)[2] <- colname

      extracted[[colname]] <- tmp
      used_vars <- c(used_vars, v)
    }
  }

  return(list(
    extracted  = extracted,
    used_vars  = used_vars,
    all_vars   = vars
  ))
}

# ===============================
# 7. MERGE ARD
# ===============================
merge_ard <- function(spine, extracted_all) {

  ard <- spine

  for (ds in extracted_all) {
    for (obj in ds$extracted) {

      if (all(c("USUBJID", "AVISITN") %in% names(obj))) {
        ard <- left_join(ard, obj, by = c("USUBJID", "AVISITN"))
      } else {
        ard <- left_join(ard, obj, by = "USUBJID")
      }
    }
  }

  dup_check <- ard %>%
    count(USUBJID, AVISITN) %>%
    filter(n > 1)

  if (nrow(dup_check) > 0) {
    log_warn(paste("Duplicatas USUBJID × AVISITN detectadas:", nrow(dup_check), "combinações"))
  }

  log_info(paste("ARD final:", nrow(ard), "linhas ×", ncol(ard), "colunas"))
  return(ard)
}

# ===============================
# 8. COVERAGE REPORT
# ===============================
build_coverage <- function(extracted_all, dataset_names) {

  report <- map2_df(extracted_all, dataset_names, function(ds, name) {

    total     <- length(ds$all_vars)
    extracted <- length(ds$used_vars)
    missing   <- setdiff(ds$all_vars, ds$used_vars)

    tibble(
      dataset           = name,
      total_vars        = total,
      extracted_vars    = extracted,
      missing_vars      = total - extracted,
      extraction_rate   = round(100 * extracted / total, 2),
      missing_var_names = paste(missing, collapse = ", ")
    )
  })

  print(report)
  fwrite(report, OUTPUT_COVERAGE)
  log_info(paste("Coverage report salvo em:", OUTPUT_COVERAGE))

  return(report)
}

# ===============================
# 9. MAIN PIPELINE
# ===============================
run_pipeline <- function() {

  log_info("=== GALAXI-3 ARD PIPELINE INICIADO ===")

  # 1. Carregar datasets
  datasets <- load_datasets(DATA_PATH)

  # 2. Pseudo-visita para datasets sem AVISITN
  #    (ADSAF não entra aqui no GALAXI-3 — já tem AVISITN real)
  datasets <- imap(datasets, add_pseudo_visit)

  # 3. Spine baseada em ADVSCDAI + ADCDAI + ADLBEF + ADEX + ADSAF + ADSL
  spine <- build_spine(datasets)

  # 4. Extrair variáveis de cada dataset
  extracted_all <- list()

  for (name in names(datasets)) {
    log_info(paste("Processando dataset:", name))
    tryCatch({
      res <- extract_dataset(datasets[[name]], name)
      extracted_all[[name]] <- res
    }, error = function(e) {
      log_warn(paste("Erro ao processar", name, ":", conditionMessage(e)))
    })
  }

  # 5. Montar ARD
  ard <- merge_ard(spine, extracted_all)

  # 6. Salvar ARD
  fwrite(ard, OUTPUT_ARD)
  log_info(paste("ARD salvo em:", OUTPUT_ARD))

  # 7. Coverage report
  build_coverage(extracted_all, names(datasets))

  log_info("=== PIPELINE CONCLUÍDO COM SUCESSO ===")

  invisible(ard)
}

# ===============================
# RUN
# ===============================
run_pipeline()
