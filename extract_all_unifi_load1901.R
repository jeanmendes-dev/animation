# ============================================================
# CLINICAL ADaM → ML-READY ARD PIPELINE
# STUDY: CNTO1275UCO3001-UNIFI / load-1901
# ============================================================
#
# Baseado em: extract_all_unifi.R (load-1899)
# Adaptado para load-1901
#
# DIFERENÇAS ESTRUTURAIS vs LOAD-1899:
#
# [1] DATASETS AUSENTES NO LOAD-1901 (presentes no 1899):
#     adeff, adhist, adhist1, adhist2, admayo, admh, advs
#     Removidos de todas as listas de configuração.
#
# [2] DATASETS NOVOS NO LOAD-1901 (ausentes no 1899):
#     admayo2 — Mayo Score categorical/binary, só AVALC, 18 PARAMCDs,
#               com AVISITN. É a nova ÂNCORA da spine (substitui admayo).
#               1.079.244 linhas — o maior dataset desta carga.
#     admalig — Malignancies (OCCDS), 67 linhas, sem PARAMCD, sem AVISITN.
#               Wide, tratado como adae.
#
# [3] DATASETS COM MUDANÇAS RELEVANTES VS 1899:
#     adbdc   — 66 PARAMCDs (+10 vs 1899: IBDQBLI, IBDQBLM, CRPMBL,
#               CALPRMBL, LTFMBL, MAYOMBL, ENDOMBL, BCORT44, IBDQBL44,
#               MAYO44BL, ENDO44BL, CRP44BL, CALP44BL, LTF44BL)
#     adcort  — 1.276.653 linhas (+5x vs 1899), +8 cols. Pré-agregação
#               por sujeito obrigatória (ver seção 2B).
#     adecon  — 15 PARAMCDs (+5: ACTIMP, ACTIMTF1, ACTIMTF2, PRODVAS,
#               PRVASTF1, PRVASTF2)
#     adeq5d  — 21 PARAMCDs (+7: EQ5DTCF1/2, EQ0*TF1/2 por dimensão)
#     adho    — 259 linhas (+105 vs 1899), +7 cols. Pré-agregação
#               por sujeito obrigatória (ver seção 2B).
#     adibdq  — 19 PARAMCDs (+6 vs 1899)
#     adlbef  — 30 PARAMCDs (+12 vs 1899: CALLNORM, CALNORM, CALPL,
#               CALPRO etc)
#     adsg    — 7.112 linhas (+658 vs 1899), +8 cols. Múltiplos
#               procedimentos por sujeito — pré-agregação recomendada.
#     adsf36  — 30 PARAMCDs (+8 vs 1899: SMCS*, SPCS* com variantes TF)
#     advscort— 6 PARAMCDs (+3: CORTLTF1..4), +11 cols
#     adsl    — 122 cols (+15 vs 1899)
#
# [4] ÂNCORA DA SPINE: admayo2 (substitui admayo)
#     admayo2: 1.079.244 linhas, 18 PARAMCDs de Mayo Score
#     (RMLGL2/4, RMGL2/4, RESPE2/4 etc — resposta e remissão)
#     Complementos: adchem, adhema, adlbef, adsaf, adeq5d, adibdq
#
# [5] DATASETS DE EVENTOS COM MÚLTIPLAS LINHAS POR SUJEITO (seção 2B):
#     adcort — dose diária (1.27M linhas, ~958/sujeito)
#     adho   — hospitalizações (259 linhas, múltiplas/sujeito)
#     adsg   — cirurgias (7112 linhas, múltiplas/sujeito)
#
#     Problema: sem AVISITN real, pseudo-AVISITN (1, 2, 3...) nunca
#     alinha com a spine → colunas AVAL_ ficam 100% vazias no ARD.
#     Solução sem derivar features: pivot_wide_by_seq() — cada evento
#     vira um sufixo numérico: AVAL_ADHO_HODUR_1, AVAL_ADHO_HODUR_2.
#     Join por USUBJID apenas → sem null-join. Colunas estáticas (TRT,
#     AGE etc.) continuam como STC_ normalmente.
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
DATA_PATH <- "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1901/data"

OUTPUT_ARD      <- "/mnt/unifi_load1901_ml_dataset.csv"
OUTPUT_COVERAGE <- "/mnt/unifi_load1901_ml_coverage_report.csv"

# ---------------------------------------------------------------
# EXPECTED_DATASETS — 27 datasets do UNIFI / load-1901
# (adeff, adhist, adhist1, adhist2, admayo, admh, advs ausentes vs 1899)
# (admalig, admayo2 novos vs 1899)
# ---------------------------------------------------------------
EXPECTED_DATASETS <- c(
  "adae",      # Adverse Events              (123 cols,  6572 rows)   — OCCDS, sem PARAMCD
  "adbdc",     # Baseline Disease Char.      (43 cols,  58812 rows)   — BDS tall, 66 PARAMCDs, sem AVISITN
  "adchem",    # Chemistry Labs              (93 cols, 530931 rows)   — BDS tall, 19 PARAMCDs, AVISITN real
  "adcm",      # Concurrent Medications      (36 cols,  24025 rows)   — BDS tall, 25 PARAMCDs, só AVALC, sem AVISITN
  "adcort",    # Corticosteroid Dose (daily) (42 cols,1276653 rows)   — wide, sem PARAMCD, pré-agregado por sujeito
  "addeath",   # Deaths                      (56 cols,      3 rows)   — wide, sem PARAMCD
  "adds",      # Disposition/Study Status    (72 cols,   7470 rows)   — wide, sem PARAMCD
  "addv",      # Protocol Deviations         (56 cols,    567 rows)   — wide, sem PARAMCD
  "adecon",    # Economic Outcomes           (57 cols, 437025 rows)   — BDS tall, 15 PARAMCDs, AVISITN real
  "adeq5d",    # EQ-5D Quality of Life       (53 cols, 226170 rows)   — BDS tall, 21 PARAMCDs, AVISITN real
  "adex",      # Exposure/Dosing             (91 cols,  18613 rows)   — wide, AVISITN real, sem PARAMCD BDS
  "adhema",    # Hematology                  (93 cols, 566056 rows)   — BDS tall, 41 PARAMCDs, AVISITN real
  "adho",      # Hospitalizations            (52 cols,    259 rows)   — wide evento, pré-agregado por sujeito
  "adibdq",    # IBDQ Questionnaire          (55 cols, 375928 rows)   — BDS tall, 19 PARAMCDs, AVISITN real
  "adis",      # Immunogenicity Serology     (72 cols,  35898 rows)   — BDS tall, 10 PARAMCDs, AVISITN real
  "adlbef",    # Efficacy Labs               (70 cols, 860930 rows)   — BDS tall, 30 PARAMCDs, AVISITN real
  "admalig",   # Malignancies [NOVO]         (90 cols,     67 rows)   — OCCDS, sem PARAMCD, sem AVISITN
  "admayo2",   # Mayo Score categorical      (53 cols,1079244 rows)   — BDS tall, 18 PARAMCDs, só AVALC, ÂNCORA
  "adpc",      # PK Concentrations           (79 cols,  16217 rows)   — BDS tall, 1 PARAMCD (CONC), AVISITN real
  "adsaf",     # Safety Exposure             (48 cols,  24555 rows)   — BDS tall, 10 PARAMCDs, AVISITN real
  "adsf36",    # SF-36 QoL                   (58 cols, 644724 rows)   — BDS tall, 30 PARAMCDs, AVISITN real
  "adsg",      # Surgery                     (52 cols,   7112 rows)   — wide evento, pré-agregado por sujeito
  "adsl",      # Subject-Level Baseline      (122 cols,  1331 rows)   — wide, sem PARAMCD
  "adtfi",     # Treatment Failure Induction (45 cols,      8 rows)   — wide, AVISITN real
  "adtfm",     # Treatment Failure Maint.    (33 cols,    551 rows)   — wide, AVISITN real
  "aduceis",   # UCEIS Endoscopy Score       (55 cols,  38430 rows)   — BDS tall, 7 PARAMCDs, AVISITN real
  "advscort"   # Corticosteroid Score Long.  (54 cols, 806204 rows)   — BDS tall, 6 PARAMCDs, AVISITN real
)

# ---------------------------------------------------------------
# DATASETS_SEM_AVISITN
# ---------------------------------------------------------------
DATASETS_SEM_AVISITN <- c(
  "adae",      # OCCDS — sem AVISITN (ordenar por ASTDT + AESEQ)
  "adbdc",     # BDS tall — sem AVISITN (pivot só por USUBJID)
  "adcm",      # BDS tall — sem AVISITN
  "addeath",   # wide — sem AVISITN (3 linhas)
  "adds",      # wide — pseudo por DSSTDT
  "addv",      # wide — pseudo por DVSTDT
  "admalig",   # OCCDS — sem AVISITN [NOVO]
  "adsl"       # wide — BASELINE único por sujeito
  # adcort, adho, adsg: NÃO recebem pseudo-AVISITN
  # → tratados via pivot_wide_by_seq() (seção 2B)
)

# ---------------------------------------------------------------
# DATASETS_COM_AVISITN
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
  "admayo2",   # [NOVO — ÂNCORA]
  "adpc",
  "adsaf",
  "adsf36",
  "adtfi",
  "adtfm",
  "aduceis",
  "advscort"
)

# ---------------------------------------------------------------
# DATASETS_TALL_FORMAT — BDS paramétricos (PARAMCD + AVAL e/ou AVALC)
#
# [NOVO] admayo2: só AVALC (sem AVAL numérico), 18 PARAMCDs, AVISITN real
# [REMOVIDOS vs 1899] adeff, adhist, adhist1, adhist2 (ausentes no 1901)
#
# NÃO entram: adae, adcort, addeath, adds, addv, adex, adho,
#             admalig, adsg, adsl, adtfi, adtfm
# ---------------------------------------------------------------
DATASETS_TALL_FORMAT <- c(
  # --- Com AVISITN real ---
  "admayo2",   # ÂNCORA [NOVO] — 18 PARAMCDs Mayo categorical, só AVALC
  "adchem",    # 19 PARAMCDs chemistry labs
  "adhema",    # 41 PARAMCDs hematology
  "adlbef",    # 30 PARAMCDs efficacy labs
  "adibdq",    # 19 PARAMCDs IBDQ questionnaire
  "adeq5d",    # 21 PARAMCDs EQ-5D QoL
  "adsf36",    # 30 PARAMCDs SF-36 QoL
  "adecon",    # 15 PARAMCDs economic outcomes
  "adis",      # 10 PARAMCDs immunogenicity
  "aduceis",   # 7 PARAMCDs UCEIS endoscopy
  "adsaf",     # 10 PARAMCDs safety exposure
  "advscort",  # 6 PARAMCDs corticosteroid score
  "adpc",      # 1 PARAMCD (CONC) PK concentrations
  # --- Sem AVISITN (pivot só por USUBJID → colunas STC_/AVALC_) ---
  "adbdc",     # 66 PARAMCDs baseline disease
  "adcm"       # 25 PARAMCDs concurrent medications — só AVALC
)

# ===============================
# LOGGING
# ===============================
log_warn <- function(msg) warning(paste0("[WARNING] ", msg))
log_info <- function(msg) message(paste0("[INFO] ", msg))

# ===============================
# 1. LOAD DATASETS
# ===============================
SOURCE_SHAPES <- list()

load_datasets <- function(path) {

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

  # Remove dupla extensão: "adae.sas7bdat.csv" → "adae"
  raw_names   <- tolower(tools::file_path_sans_ext(basename(files)))
  clean_names <- gsub("\\.sas7bdat$", "", raw_names)
  names(datasets) <- clean_names
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
    log_warn(paste("Datasets fora do escopo load-1901:",
                   paste(extra_ds, collapse = ", ")))
  }

  log_info(paste("Carregados", length(datasets), "datasets"))
  return(datasets)
}

# ===============================
# 2A. ADD PSEUDO VISIT
# ===============================
add_pseudo_visit <- function(df, dataset_name) {

  if (!("USUBJID" %in% names(df))) return(df)
  if ("AVISITN"  %in% names(df)) return(df)

  # Datasets tall: join por USUBJID após pivot — sem pseudo-visita
  if (dataset_name %in% DATASETS_TALL_FORMAT) {
    log_info(paste0("[", dataset_name, "] Tall sem AVISITN — sem pseudo-visita"))
    return(df)
  }

  log_warn(paste("Criando pseudo AVISITN para:", dataset_name))

  order_map <- list(
    adae    = c("ASTDT", "AESEQ"),
    addeath = c("ADT"),
    adds    = c("DSSTDT", "DSSEQ"),
    addv    = c("DVSTDT", "DVSEQ"),
    admalig = c("ASTDT", "AESEQ")   # [NOVO] mesmo padrão de adae
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

# ---------------------------------------------------------------
# DATASETS_SEQ_PIVOT — datasets de eventos com múltiplas linhas
# por sujeito e sem AVISITN real. Em vez de pseudo-AVISITN (que
# causa null-join) ou pré-agregação (que deriva features), cada
# evento recebe um sufixo numérico nas colunas:
#   AVAL_ADHO_HODUR_1, AVAL_ADHO_HODUR_2, AVAL_ADHO_HODUR_3 ...
#   AVAL_ADCORT_DYPEDOSE_1, AVAL_ADCORT_DYPEDOSE_2 ...
# Join final por USUBJID apenas → sem null-join, sem derivação.
#
# Colunas que não variam por sujeito (TRT, AGE, flags) continuam
# extraídas como STC_ normalmente pelo classify_variable().
#
# seq_col: coluna de ordenação dos eventos dentro de cada sujeito.
# max_events: limite de eventos por sujeito. Eventos além desse
#   limite são descartados com warning. Escolher com base na
#   distribuição real (ex: adcort é muito volumoso — use com cuidado).
# ---------------------------------------------------------------
DATASETS_SEQ_PIVOT <- list(
  adho   = list(seq_col = "HOSEQ",  max_events = 10),
  adsg   = list(seq_col = "SGSEQ",  max_events = 15),
  adcort = list(seq_col = "ADT",    max_events = 30)
  # adcort: 1.27M linhas (até ~958 dias/sujeito). max_events=30
  # captura os primeiros 30 dias de registro — suficiente para
  # estabelecer o perfil inicial de corticosteroide sem explodir
  # o ARD com 958 colunas por variável. Ajuste conforme necessário.
)

# ===============================
# 2B. PIVOT WIDE BY SEQUENCE (datasets de eventos)
# ===============================
# Transforma dataset de eventos (múltiplas linhas/sujeito, sem AVISITN)
# em formato wide, preservando TODOS os valores originais sem derivação.
#
# Estratégia:
#   1. Ordenar eventos por seq_col dentro de cada sujeito
#   2. Numerar eventos: event_num = 1, 2, 3, ...
#   3. Colunas que variam entre eventos → pivotar com sufixo _N:
#        AVAL_ADHO_HODUR_1, AVAL_ADHO_HODUR_2 ...
#        AVAL_ADHO_ASTDT_1, AVAL_ADHO_ASTDT_2 ...
#   4. Colunas estáticas por sujeito (TRT, AGE etc.) → STC_ por USUBJID
#   5. Resultado: 1 linha por USUBJID, join por USUBJID apenas
pivot_wide_by_seq <- function(df, dataset_name) {

  cfg    <- DATASETS_SEQ_PIVOT[[dataset_name]]
  DS     <- toupper(dataset_name)
  seqcol <- cfg$seq_col
  maxev  <- cfg$max_events

  # Ordenar e numerar eventos por sujeito
  order_col <- intersect(seqcol, names(df))
  if (length(order_col) > 0) {
    df <- df %>% arrange(USUBJID, across(all_of(order_col)))
  }

  df <- df %>%
    group_by(USUBJID) %>%
    mutate(event_num = row_number()) %>%
    ungroup()

  # Avisar se há eventos além do limite
  n_excess <- df %>% filter(event_num > maxev) %>% nrow()
  if (n_excess > 0) {
    log_warn(paste0("[", dataset_name, "] ", n_excess,
                    " linhas além do limite max_events=", maxev,
                    " foram descartadas. Ajuste max_events se necessário."))
  }

  df <- df %>% filter(event_num <= maxev)

  # Separar colunas variáveis (variam entre eventos) das estáticas
  non_key <- setdiff(names(df), c("USUBJID", "event_num"))
  vary_cols   <- character(0)
  static_cols <- character(0)

  for (v in non_key) {
    var_check <- df %>%
      group_by(USUBJID) %>%
      summarise(n_distinct_vals = n_distinct(.data[[v]], na.rm = TRUE),
                .groups = "drop")
    if (any(var_check$n_distinct_vals > 1)) {
      vary_cols <- c(vary_cols, v)
    } else {
      static_cols <- c(static_cols, v)
    }
  }

  extracted <- list()
  used_vars <- non_key  # todas as colunas são processadas

  # ---- Colunas variáveis: pivot com sufixo _N ----
  # pivot_longer não suporta tipos mistos (numeric + character) numa
  # única coluna value — separar em dois grupos antes de pivotar.
  if (length(vary_cols) > 0) {

    vary_num <- vary_cols[sapply(vary_cols, function(v) is.numeric(df[[v]]))]
    vary_chr <- vary_cols[sapply(vary_cols, function(v) !is.numeric(df[[v]]))]

    pivot_group <- function(vcols, prefix) {
      if (length(vcols) == 0) return(NULL)
      df %>%
        select(USUBJID, event_num, all_of(vcols)) %>%
        pivot_longer(cols      = all_of(vcols),
                     names_to  = "var_name",
                     values_to = "value") %>%
        mutate(col_name = paste0(prefix, DS, "_", var_name, "_", event_num)) %>%
        select(USUBJID, col_name, value) %>%
        pivot_wider(names_from  = col_name,
                    values_from = value,
                    values_fn   = first)
    }

    wide_num <- pivot_group(vary_num, "AVAL_")
    wide_chr <- pivot_group(vary_chr, "AVAL_")

    # Juntar os dois grupos wide por USUBJID
    if (!is.null(wide_num) && !is.null(wide_chr)) {
      df_wide_vary <- left_join(wide_num, wide_chr, by = "USUBJID")
    } else if (!is.null(wide_num)) {
      df_wide_vary <- wide_num
    } else {
      df_wide_vary <- wide_chr
    }

    extracted[["__seq_vary_pivot__"]] <- df_wide_vary
  }

  # ---- Colunas estáticas: STC_ por USUBJID ----
  for (v in static_cols) {
    tmp <- df %>%
      select(USUBJID, value = all_of(v)) %>%
      group_by(USUBJID) %>%
      summarise(value = first(na.omit(value)), .groups = "drop")
    colname <- paste0("STC_", DS, "_", v)
    names(tmp)[2] <- colname
    extracted[[colname]] <- tmp
  }

  all_vars <- non_key

  log_info(paste0("[", dataset_name, "] Seq pivot: ",
                  length(vary_cols), " cols variáveis × até ", maxev,
                  " eventos + ", length(static_cols), " cols STC_"))

  return(list(
    extracted            = extracted,
    used_vars            = all_vars,
    all_vars             = all_vars,
    n_params_pivoted     = length(vary_cols) * maxev,
    is_tall              = TRUE,
    pivot_cols_generated = character(0)
  ))
}

# ===============================
# 3. BUILD SPINE
# ===============================
# Âncora principal: admayo2 [NOVO — substitui admayo do load-1899]
# admayo2: 1.079.244 linhas, 18 PARAMCDs de Mayo Score com AVISITN real.
# Complementos: adchem, adhema, adlbef, adsaf, adeq5d, adibdq, adsf36,
#               adecon, aduceis, advscort, adis, adpc.
# adsl: garante todos os 1.331 sujeitos na spine.
build_spine <- function(datasets) {

  spine_parts <- list()

  # Âncora principal: admayo2
  if ("admayo2" %in% names(datasets)) {
    spine_parts[["admayo2"]] <- datasets[["admayo2"]] %>%
      select(any_of(c("USUBJID", "AVISIT", "AVISITN"))) %>%
      distinct()
    log_info("Usando admayo2 como âncora principal da spine [load-1901]")
  }

  # Complementos com AVISITN real
  for (nm in c("adchem", "adhema", "adlbef", "adsaf", "adeq5d",
               "adibdq", "adsf36", "adecon", "aduceis", "advscort",
               "adis", "adpc")) {
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

  # adsl: garante todos os 1331 sujeitos
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
      log_warn(paste(dataset_name, "detectado como tall automaticamente"))
      return(TRUE)
    }
  }

  return(FALSE)
}

# ===============================
# 6B. PIVOT WIDE BY PARAMCD
# ===============================
# AVAL numérico  → AVAL_{DS}_{PARAMCD}   (média se duplicatas)
# AVALC character→ AVALC_{DS}_{PARAMCD}  (concat " | " se duplicatas)
# admayo2 tem só AVALC — o pivot funciona mesmo sem AVAL presente.
pivot_wide_by_paramcd <- function(df, dataset_name) {

  DS       <- toupper(dataset_name)
  key_cols <- intersect(c("USUBJID", "AVISITN"), names(df))
  has_avisitn <- "AVISITN" %in% key_cols
  has_paramcd <- "PARAMCD" %in% names(df)
  has_aval    <- "AVAL"    %in% names(df)
  has_avalc   <- "AVALC"   %in% names(df)

  non_pivot_meta <- c("USUBJID", "AVISIT", "AVISITN",
                      "PARAMCD", "PARAM", "AVAL", "AVALC",
                      "PARAMN", "DTYPE", "PARAMTYP", "BASETYPE",
                      "BASE", "CHG", "PCHG")

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
  # Funciona independentemente de AVAL (caso admayo2 e adcm)
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

  for (dcol in intersect(c("PARAM", "PARAMCD", "DTYPE", "PARAMN",
                            "PARAMTYP", "BASETYPE", "BASE", "CHG", "PCHG"),
                          names(df))) {
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

  # Datasets de eventos: pivot por sequência (sem derivação de features)
  if (dataset_name %in% names(DATASETS_SEQ_PIVOT)) {
    log_info(paste0("[", dataset_name, "] Evento multi-linha → pivot_wide_by_seq()"))
    return(pivot_wide_by_seq(df, dataset_name))
  }

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
        return(tibble(dataset=name, ard_cols_count=0L,
                      ard_cells_total=NA_integer_, ard_cells_filled=NA_integer_,
                      ard_fill_pct=NA_real_, fill_efficiency=NA_real_))
      }

      ard_sub          <- ard %>% select(all_of(ds_cols))
      ard_cells_total  <- as.integer(nrow(ard) * length(ds_cols))
      ard_cells_filled <- as.integer(sum(!is.na(ard_sub)))
      ard_fill_pct     <- round(100 * ard_cells_filled / ard_cells_total, 2)

      src      <- SOURCE_SHAPES[[name]]
      fill_eff <- if (!is.null(src) && src$cells_filled > 0) {
        round(100 * ard_cells_filled / src$cells_filled, 2)
      } else { NA_real_ }

      tibble(dataset=name, ard_cols_count=length(ds_cols),
             ard_cells_total=ard_cells_total, ard_cells_filled=ard_cells_filled,
             ard_fill_pct=ard_fill_pct, fill_efficiency=fill_eff)
    })

    report <- left_join(report, ard_metrics, by = "dataset")
  }

  print(report, width = 250)
  fwrite(report, OUTPUT_COVERAGE)
  log_info(paste("Coverage report salvo em:", OUTPUT_COVERAGE))

  # Alertas: wide com fill_efficiency < 10% e dados substanciais no source
  if (!is.null(ard)) {
    low_eff <- report %>%
      filter(!is.na(fill_efficiency), fill_efficiency < 10,
             src_cells_filled > 1000, is_tall_format == FALSE)
    if (nrow(low_eff) > 0) {
      log_warn(paste(
        "Wide datasets com fill_efficiency < 10% — verificar join ou pré-agregação:",
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

  log_info("=== UNIFI ARD PIPELINE (load-1901) INICIADO ===")
  log_info("Study: CNTO1275UCO3001-UNIFI / load-1901")
  log_info(paste("Data path:", DATA_PATH))

  # 1. Carregar datasets (preenche SOURCE_SHAPES)
  datasets <- load_datasets(DATA_PATH)

  # 2. Pseudo-visita para datasets wide sem AVISITN
  #    Datasets tall sem AVISITN são pulados automaticamente
  #    adcort, adho, adsg: pulados aqui — vão para pivot_wide_by_seq()
  datasets <- imap(datasets, add_pseudo_visit)

  # 3. Spine baseada em admayo2 + complementos + adsl
  spine <- build_spine(datasets)

  # 4. Extração de variáveis
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

  # 7. Coverage report
  # Usa names(extracted_all) — reflete só os datasets processados
  # com sucesso, evitando mismatch de tamanho com datasets que
  # falharam silenciosamente no tryCatch acima.
  build_coverage(extracted_all, names(extracted_all), ard = ard)

  log_info("=== UNIFI ARD PIPELINE (load-1901) CONCLUÍDO ===")
  invisible(ard)
}

# ===============================
# RUN
# ===============================
run_pipeline()
