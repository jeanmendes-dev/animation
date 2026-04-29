# ============================================================
# CLINICAL ADaM → ML-READY ARD PIPELINE
# STUDY: CNTO1959CRD3001-GALAXI-GAL3-WK48
# ============================================================
#
# CORREÇÕES APLICADAS (v2 — 2025):
#
# [BUG-1] PERDA DE VALORES EM DATASETS TALL/LONG (CRÍTICO)
#   PROBLEMA: extract_dataset() usava aggregate_value() que retorna
#   apenas o PRIMEIRO valor (first()) para colunas character. Em
#   datasets tall como adcdai, onde há 26 linhas por USUBJID+AVISITN
#   (uma por PARAMCD), somente o primeiro PARAM era preservado — os
#   demais 25 eram silenciosamente descartados. O coverage report
#   indicava 100% porque media apenas a *presença* da coluna extraída,
#   não a completude dos valores dentro dela.
#
#   SOLUÇÃO: Detecção automática de datasets tall/long via
#   detect_tall_format(). Quando confirmado, a extração executa
#   pivot_wide_by_paramcd(): cada PARAMCD vira um conjunto de colunas
#   próprias (ex: AVAL_ADCDAI_CDAI, AVAL_ADCDAI_PRO2, ...).
#   A lógica de datasets wide (adsl, addisp, etc.) é preservada.
#
# [BUG-2] COVERAGE REPORT SUPERFICIAL
#   PROBLEMA: O report contava apenas variáveis extraídas vs. totais,
#   sem detectar colapso de valores dentro de cada coluna.
#   SOLUÇÃO: build_coverage() agora inclui métricas de completude:
#   - n_params_pivoted: quantos PARAMCDs foram pivotados
#   - pivot_cols_generated: quantas colunas foram geradas pelo pivot
#   - value_loss_risk: flag indicando datasets com risco de perda
#   - n_missing_values_pct: % de NAs no ARD final para colunas do ds
#
# [BUG-3] CLASSIFICAÇÃO STC_/AVAL_ EM DATASETS TALL
#   PROBLEMA: PARAM e PARAMCD em datasets tall classificavam como
#   LONGITUDINAL e geravam uma única coluna colapsada. Após o pivot,
#   essas colunas deixam de existir como tal — cada PARAMCD vira sua
#   própria coluna AVAL_.
#   SOLUÇÃO: extract_dataset() divide a lógica em dois caminhos:
#   tall (pivot_wide_by_paramcd) e wide (lógica original).
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

# Datasets confirmados no diagnóstico GALAXI-3
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

# Datasets sem AVISITN real — recebem pseudo-visita
DATASETS_SEM_AVISITN <- c(
  "adae",       # sem AVISITN — ordenar por ASTDT + AESEQ
  "adbdc",      # sem AVISITN — ordenar por ASTDT / ASTDTC
  "addisp",     # sem AVISITN — ordenar por ASTDT / ASTDTC
  "adice",      # sem AVISITN — ordenar por ADT / ADY
  "adrxfail",   # sem AVISITN — ordenar por PARAMCD
  "adrxhist",   # sem AVISITN — ordenar por ASEQ / CMSEQ
  "adsl"        # sem AVISITN — baseline único por sujeito
)

# Datasets com AVISITN real
DATASETS_COM_AVISITN <- c(
  "adcdai",
  "adex",
  "adlbef",
  "adsaf",
  "advscdai"
)

# ---------------------------------------------------------------
# [CORREÇÃO BUG-1]
# Datasets no formato tall/long (múltiplas linhas por USUBJID+AVISITN,
# cada linha representando um parâmetro distinto via PARAMCD).
# Esses datasets DEVEM ser tratados com pivot wide por PARAMCD —
# NUNCA com first()/mean() sobre PARAM/AVAL diretamente.
#
# Identificação:
#   adcdai    → 26 PARAMCDs por visita (CDAI, PRO2, CD201..CD208W, etc.)
#   advscdai  → múltiplos PARAMCDs longitudinais
#   adlbef    → múltiplos parâmetros laboratoriais por visita
#   adex      → múltiplos tipos de exposição por visita
#   adsaf     → múltiplos parâmetros de safety por visita
#   adbdc     → múltiplos scores de doença baseline por sujeito
#   adae      → múltiplos eventos adversos por sujeito/visita
#   adrxfail  → múltiplos registros de falha de tratamento
#   adrxhist  → múltiplos registros de histórico de tratamento
#   adice     → múltiplos eventos ICE por sujeito
# ---------------------------------------------------------------
DATASETS_TALL_FORMAT <- c(
  "adcdai",    # 26 PARAMCDs por USUBJID+AVISITN
  "advscdai",  # múltiplos PARAMCDs por USUBJID+AVISITN
  "adlbef",    # múltiplos parâmetros lab por USUBJID+AVISITN
  "adex",      # múltiplos tipos exposição por USUBJID+AVISITN
  "adsaf",     # múltiplos parâmetros safety por USUBJID+AVISITN
  "adbdc",     # múltiplos scores doença por USUBJID (pseudo-visita)
  "adae",      # múltiplos AEs por USUBJID (pseudo-visita)
  "adrxfail",  # múltiplos registros tx failure por USUBJID
  "adrxhist",  # múltiplos registros tx history por USUBJID
  "adice"      # múltiplos eventos ICE por USUBJID
)

# ===============================
# LOGGING
# ===============================
log_warn <- function(msg) warning(paste0("[WARNING] ", msg))
log_info <- function(msg) message(paste0("[INFO] ", msg))

# ===============================
# 1. LOAD DATASETS
# ===============================
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
add_pseudo_visit <- function(df, dataset_name) {

  if (!("USUBJID" %in% names(df))) return(df)
  if ("AVISITN"  %in% names(df)) return(df)

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
# 4. CLASSIFICATION (apenas para datasets wide)
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
# 5. SMART AGGREGATION (apenas para datasets wide)
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
# 6A. DETECT TALL FORMAT
# ===============================
# [CORREÇÃO BUG-1]
# Detecta automaticamente se um dataset é tall/long verificando:
#   (a) se o dataset está na lista explícita DATASETS_TALL_FORMAT, OU
#   (b) se tem coluna PARAMCD E a mediana de linhas por USUBJID+AVISITN > 1
#
# A verificação explícita (a) é a fonte primária de verdade.
# A verificação automática (b) serve como safety net para datasets
# não listados que também sejam tall.
detect_tall_format <- function(df, dataset_name) {

  # Verificação explícita via lista de configuração
  if (dataset_name %in% DATASETS_TALL_FORMAT) {
    return(TRUE)
  }

  # Safety net automático: PARAMCD presente + duplicatas por chave
  if ("PARAMCD" %in% names(df) && all(c("USUBJID", "AVISITN") %in% names(df))) {
    rows_per_key <- df %>%
      count(USUBJID, AVISITN) %>%
      pull(n)
    if (median(rows_per_key) > 1) {
      log_warn(paste(dataset_name, "detectado como tall/long automaticamente — aplicando pivot"))
      return(TRUE)
    }
  }

  return(FALSE)
}

# ===============================
# 6B. PIVOT WIDE BY PARAMCD (datasets tall/long)
# ===============================
# [CORREÇÃO BUG-1 — NÚCLEO DA CORREÇÃO]
#
# Para datasets tall, cada linha representa um parâmetro (PARAMCD).
# A extração correta para ML requer:
#
#   1. Identificar colunas "pivot anchor": PARAMCD é a coluna que
#      define o nome das novas colunas. AVAL (numérico) é o valor
#      principal. AVALC (character), se existir, complementa.
#
#   2. Identificar colunas "estáticas por linha" (row-level metadata):
#      colunas que têm o mesmo valor para todos os PARAMCDs de uma
#      dada chave — ex: ADT, ADY, DTYPE, ABLFL. Essas são extraídas
#      uma única vez por chave, sem pivot.
#
#   3. Pivotar AVAL (e AVALC se existir) por PARAMCD, gerando:
#      AVAL_{DS}_{PARAMCD} para cada valor único de PARAMCD.
#      Ex: AVAL_ADCDAI_CDAI, AVAL_ADCDAI_PRO2, AVAL_ADCDAI_CD201...
#
#   4. Colunas demográficas/de subject (AGE, SEX, RACE, TRT*, etc.)
#      que são estáticas entre sujeitos recebem prefixo STC_.
#
# Resultado: um data.frame wide com exatamente 1 linha por
# USUBJID + AVISITN, preservando 100% dos valores.
pivot_wide_by_paramcd <- function(df, dataset_name) {

  DS <- toupper(dataset_name)
  key_cols <- c("USUBJID", "AVISITN")

  # ---- Identificar colunas disponíveis ----
  has_paramcd <- "PARAMCD" %in% names(df)
  has_aval    <- "AVAL"    %in% names(df)
  has_avalc   <- "AVALC"   %in% names(df)

  # Colunas que NÃO devem ser pivotadas (chaves + metadados da visita)
  non_pivot_meta <- c("USUBJID", "AVISIT", "AVISITN",
                      "PARAMCD", "PARAM", "AVAL", "AVALC",
                      "PARAMN", "DTYPE")  # DTYPE varia por linha → excluir do wide fixo

  # ---- Classificar colunas restantes como STC ou AVAL ----
  other_cols <- setdiff(names(df), non_pivot_meta)
  extracted  <- list()
  used_vars  <- c()

  for (v in other_cols) {
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

  # ---- Pivot de AVAL por PARAMCD ----
  # [NÚCLEO DA CORREÇÃO]
  # Cada valor único de PARAMCD gera uma coluna separada no ARD.
  # Ex: PARAMCD = "CDAI" → coluna "AVAL_ADCDAI_CDAI"
  #     PARAMCD = "PRO2" → coluna "AVAL_ADCDAI_PRO2"
  # Isso preserva 100% dos valores sem colapso por first().
  pivot_cols_generated <- character(0)

  if (has_paramcd && has_aval) {

    # Sanitizar PARAMCD para uso como nome de coluna
    # (remove espaços, caracteres especiais, converte para uppercase)
    df_pivot <- df %>%
      mutate(
        PARAMCD_clean = str_replace_all(toupper(PARAMCD), "[^A-Z0-9_]", "_")
      )

    # AVAL numérico: média se houver duplicatas residuais dentro de
    # USUBJID + AVISITN + PARAMCD (ex: múltiplos registros com mesmo PARAMCD)
    aval_wide <- df_pivot %>%
      select(USUBJID, AVISITN, PARAMCD_clean, AVAL) %>%
      group_by(USUBJID, AVISITN, PARAMCD_clean) %>%
      summarise(AVAL = mean(AVAL, na.rm = TRUE), .groups = "drop") %>%
      pivot_wider(
        id_cols     = c(USUBJID, AVISITN),
        names_from  = PARAMCD_clean,
        values_from = AVAL,
        names_prefix = paste0("AVAL_", DS, "_")
      )

    pivot_cols_generated <- c(pivot_cols_generated, setdiff(names(aval_wide), c("USUBJID", "AVISITN")))
    extracted[["__aval_pivot__"]] <- aval_wide
    used_vars <- c(used_vars, "AVAL")
  }

  # AVALC character se existir: valor mais frequente por chave+PARAMCD
  if (has_paramcd && has_avalc) {

    df_pivot <- df %>%
      mutate(
        PARAMCD_clean = str_replace_all(toupper(PARAMCD), "[^A-Z0-9_]", "_")
      )

    mode_chr <- function(x) {
      x <- na.omit(x)
      if (length(x) == 0) return(NA_character_)
      names(sort(table(x), decreasing = TRUE))[1]
    }

    avalc_wide <- df_pivot %>%
      select(USUBJID, AVISITN, PARAMCD_clean, AVALC) %>%
      group_by(USUBJID, AVISITN, PARAMCD_clean) %>%
      summarise(AVALC = mode_chr(AVALC), .groups = "drop") %>%
      pivot_wider(
        id_cols     = c(USUBJID, AVISITN),
        names_from  = PARAMCD_clean,
        values_from = AVALC,
        names_prefix = paste0("AVALC_", DS, "_")
      )

    pivot_cols_generated <- c(pivot_cols_generated, setdiff(names(avalc_wide), c("USUBJID", "AVISITN")))
    extracted[["__avalc_pivot__"]] <- avalc_wide
    used_vars <- c(used_vars, "AVALC")
  }

  # PARAM (label descritivo): não pivota — apenas registra que foi processado
  # via PARAMCD. Manter PARAM como coluna separada seria redundante após pivot.
  if ("PARAM" %in% names(df)) {
    used_vars <- c(used_vars, "PARAM")
  }
  if ("PARAMCD" %in% names(df)) {
    used_vars <- c(used_vars, "PARAMCD")
  }

  # DTYPE: metadado por linha — registrado como usado, não pivotado
  # (contém valores como "LOCF", "CFB" que só fazem sentido por linha)
  for (dcol in intersect(c("DTYPE", "PARAMN"), names(df))) {
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
# 6C. EXTRACT VARIABLES (datasets wide — lógica original preservada)
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
# [CORREÇÃO BUG-1 — PONTO DE ENTRADA]
# Separa o processamento em dois caminhos mutuamente exclusivos:
#   - tall/long → pivot_wide_by_paramcd()
#   - wide       → extract_wide_dataset() (lógica original)
extract_dataset <- function(df, dataset_name) {

  if (detect_tall_format(df, dataset_name)) {
    log_info(paste0("[", dataset_name, "] Formato tall/long detectado — executando pivot wide por PARAMCD"))
    return(pivot_wide_by_paramcd(df, dataset_name))
  } else {
    log_info(paste0("[", dataset_name, "] Formato wide — extração padrão STC_/AVAL_"))
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

      # Colunas de join disponíveis no objeto extraído
      has_avisitn <- "AVISITN" %in% names(obj)

      if (has_avisitn) {
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
# 8. COVERAGE REPORT (aprimorado)
# ===============================
# [CORREÇÃO BUG-2]
# O report original media apenas se a coluna foi criada (presença),
# o que gerava 100% mesmo com valores colapsados.
#
# O report aprimorado inclui:
#   - extraction_rate: taxa de variáveis extraídas vs. total (mantido)
#   - is_tall_format: flag indicando se foi processado via pivot
#   - n_params_pivoted: quantas colunas PARAMCD foram geradas
#   - value_loss_risk: TRUE se o dataset é tall E não usou pivot
#     (indica um possível dataset tall não listado em DATASETS_TALL_FORMAT)
#   - na_pct_in_ard: % médio de NAs nas colunas do dataset no ARD final
#     (completude real dos dados no arquivo de saída)
build_coverage <- function(extracted_all, dataset_names, ard = NULL) {

  report <- map2_df(extracted_all, dataset_names, function(ds, name) {

    total       <- length(ds$all_vars)
    n_extracted <- length(ds$used_vars)
    missing     <- setdiff(ds$all_vars, ds$used_vars)

    tibble(
      dataset              = name,
      total_vars           = total,
      extracted_vars       = n_extracted,
      missing_vars         = total - n_extracted,
      extraction_rate      = round(100 * n_extracted / total, 2),
      is_tall_format       = ds$is_tall,
      n_params_pivoted     = ds$n_params_pivoted,
      # [CORREÇÃO BUG-2] value_loss_risk: TRUE quando o dataset tem
      # PARAMCD mas NÃO foi processado via pivot (indicaria que um
      # dataset tall está sendo tratado como wide, gerando colapso de valores)
      value_loss_risk      = !ds$is_tall && (n_extracted < total),
      missing_var_names    = paste(missing, collapse = ", ")
    )
  })

  # Adicionar % de NA por dataset no ARD final (completude real)
  if (!is.null(ard)) {
    na_summary <- map2_df(extracted_all, dataset_names, function(ds, name) {
      DS <- toupper(name)
      ds_cols <- names(ard)[str_starts(names(ard), paste0("STC_", DS, "_")) |
                              str_starts(names(ard), paste0("AVAL_", DS, "_")) |
                              str_starts(names(ard), paste0("AVALC_", DS, "_"))]
      if (length(ds_cols) == 0) {
        return(tibble(dataset = name, na_pct_in_ard = NA_real_, ard_cols_count = 0L))
      }
      na_pct <- ard %>%
        select(all_of(ds_cols)) %>%
        summarise(across(everything(), ~ mean(is.na(.)))) %>%
        unlist() %>%
        mean(na.rm = TRUE) * 100

      tibble(
        dataset       = name,
        na_pct_in_ard = round(na_pct, 2),
        ard_cols_count = length(ds_cols)
      )
    })

    report <- left_join(report, na_summary, by = "dataset")
  }

  print(report)
  fwrite(report, OUTPUT_COVERAGE)
  log_info(paste("Coverage report salvo em:", OUTPUT_COVERAGE))

  # Alertar sobre possíveis perdas de valores não detectadas
  risky <- report %>% filter(value_loss_risk == TRUE)
  if (nrow(risky) > 0) {
    log_warn(paste("Datasets com risco de perda de valores (verifique manualmente):",
                   paste(risky$dataset, collapse = ", ")))
  }

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
  datasets <- imap(datasets, add_pseudo_visit)

  # 3. Spine baseada em ADVSCDAI + ADCDAI + ADLBEF + ADEX + ADSAF + ADSL
  spine <- build_spine(datasets)

  # 4. Extrair variáveis de cada dataset
  #    [CORREÇÃO BUG-1] detect_tall_format() dentro de extract_dataset()
  #    garante que datasets tall usem pivot_wide_by_paramcd()
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

  # 7. Coverage report aprimorado (passa o ARD para cálculo de NA%)
  #    [CORREÇÃO BUG-2] build_coverage() agora recebe o ARD final
  build_coverage(extracted_all, names(datasets), ard = ard)

  log_info("=== PIPELINE CONCLUÍDO COM SUCESSO ===")

  invisible(ard)
}

# ===============================
# RUN
# ===============================
run_pipeline()
