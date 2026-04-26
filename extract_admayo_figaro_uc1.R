# ============================================================
# EXTRACÇÃO FOCADA — ADMAYO
# STUDY: SHP647UC301-FIGARO-UC1
#
# INPUT:  admayo_figaro_uc1_clean.csv
# OUTPUT: figaro_uc1_mes_labels.csv
#
# Grain esperado:
#   1 linha por sujeito × visita × parâmetro
# ============================================================

library(data.table)
library(dplyr)
library(stringr)

# ===============================
# CONFIG
# ===============================
DATA_PATH <- "/domino/datasets/local/clinical-trial-data/SHP647UC301-FIGARO-UC1/data_updated"
INPUT     <- file.path(DATA_PATH, "admayo_figaro_uc1_clean.csv")
OUTPUT    <- "/mnt/figaro_uc1_mes_labels.csv"

# ===============================
# LOGGING
# ===============================
log_info <- function(msg) message(paste0("[INFO] ", msg))
log_warn <- function(msg) warning(paste0("[WARNING] ", msg))

# ===============================
# 1. LOAD
# ===============================
log_info(paste("Lendo:", INPUT))
dt <- fread(INPUT, sep = "auto")
names(dt) <- toupper(names(dt))

log_info(paste("Carregado:", nrow(dt), "linhas ×", ncol(dt), "colunas"))

# ===============================
# 2. RENOMEAR PARA MODELO UNIFI JR
# ===============================

dt <- dt %>%
  mutate(
    SUBJECT_ID = USUBJID,
    VISIT_WEEK = AVISIT,
    MES_SCORES = PARAMCD
  )

# ===============================
# 3. SELECÇÃO DE COLUNAS
# ===============================

COLS_KEEP <- c(
  # ── Grain / labels ──────────────────────────────────────
  "SUBJECT_ID", "AVISITN", "VISIT_WEEK",
  "MES_SCORES", "PARAM", "PARAMN", "PARAMTYP", "PARCAT1",
  
  # ── Valores principais ──────────────────────────────────
  "AVAL", "AVALC", "AVALCAT1",
  "BASE", "BASETYPE", "CHG", "PCHG",
  
  # ── Flags de análise ────────────────────────────────────
  "ABLFL", "APOBLFL", "ANL01FL", "DTYPE", "ICE",
  "CRIT1FL", "CRIT2FL", "CRIT3FL", "CRIT4FL", "CRIT5FL",
  "WK8MISFL", "WK8DISFL", "WK44MIFL", "WK44DIFL",
  
  # ── Datas / dia relativo ────────────────────────────────
  "ADT", "ADTF", "ADY",
  
  # ── Sub-study / visit source ────────────────────────────
  "SSVISIT", "SSVISITN",
  
  # ── Tratamento / população ──────────────────────────────
  "TRT01P", "TRT01A", "TRT02P", "TRT02A", "TRT03P", "TRT03A",
  "TR02PG1", "TR02AG1",
  "RANDFL", "SAFFL", "FASFL",
  
  # ── Rastreabilidade ─────────────────────────────────────
  "SRCDOM", "SRCVAR", "SRCSEQ", "QSDTC", "MODTC"
)

cols_present <- intersect(COLS_KEEP, names(dt))
cols_missing <- setdiff(COLS_KEEP, names(dt))

if (length(cols_missing) > 0) {
  log_warn(paste(
    "Colunas não encontradas no ficheiro:",
    paste(cols_missing, collapse = ", ")
  ))
}

dt <- dt[, ..cols_present]

# ===============================
# 4. LIMPEZA MÍNIMA PARA ARD
# ===============================

dt <- dt %>%
  filter(
    !is.na(SUBJECT_ID),
    !is.na(AVISITN),
    !is.na(VISIT_WEEK),
    !is.na(MES_SCORES)
  )

# ===============================
# 5. ORDENAÇÃO
# ===============================

dt <- as.data.table(dt)
dt <- dt[order(SUBJECT_ID, AVISITN, PARAMN, MES_SCORES)]

# ===============================
# 6. VERIFICAÇÃO DE COMPLETUDE
# ===============================

log_info(paste("Linhas totais:   ", nrow(dt)))
log_info(paste("Sujeitos únicos: ", uniqueN(dt$SUBJECT_ID)))
log_info(paste("Visitas únicas:  ", uniqueN(dt$AVISITN)))
log_info(paste("Scores únicos:   ", uniqueN(dt$MES_SCORES)))

paramcd_summary <- dt[, .(
  label = first(PARAM),
  n_rows = .N,
  n_subjects = uniqueN(SUBJECT_ID),
  n_visits = uniqueN(AVISITN),
  pct_aval_missing = round(mean(is.na(AVAL) | AVAL == "") * 100, 1)
), by = MES_SCORES][order(MES_SCORES)]

cat("\n=== COMPLETUDE POR MES_SCORES ===\n")
print(paramcd_summary)

dup_check <- dt[, .N, by = .(SUBJECT_ID, AVISITN, MES_SCORES)][N > 1]

if (nrow(dup_check) > 0) {
  log_warn(paste(
    "Duplicatas SUBJECT_ID × AVISITN × MES_SCORES:",
    nrow(dup_check),
    "combinações"
  ))
} else {
  log_info("Grain OK — sem duplicatas SUBJECT_ID × AVISITN × MES_SCORES")
}

# ===============================
# 7. SALVAR OUTPUT ÚNICO
# ===============================

write.csv(dt, OUTPUT, row.names = FALSE)

log_info(paste("Output salvo em:", OUTPUT))
log_info(paste("Colunas no output:", ncol(dt)))