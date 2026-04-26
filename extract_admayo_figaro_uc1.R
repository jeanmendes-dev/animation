# ============================================================
# EXTRACÇÃO FOCADA — ADMAYO
#
# OBJECTIVO: extrair TODO o conteúdo de admayo.csv
# organizado pelo grain USUBJID × AVISITN × AVISIT × PARAMCD,
# com o label PARAM embutido — sem necessidade de dicionário
# separado.
#
# OUTPUT: unifijr_load2793_mes_labels.csv
#   Grain: 1 linha por sujeito × visita × parâmetro
#   Garantia: todos os 82 PARAMCDs × 16 visitas preservados
#   Verificação: contagem por PARAMCD impressa no console
# ============================================================

library(data.table)
library(dplyr)
library(stringr)

# ===============================
# CONFIG
# ===============================
DATA_PATH  <- "/domino/datasets/local/clinical-trial-data/SHP647UC301-FIGARO-UC1/data_updated/admayo_figaro_uc1_clean.csv"
OUTPUT     <- "/mnt/figaro_uc1_mes_labels.csv"

# ===============================
# LOGGING
# ===============================
log_info <- function(msg) message(paste0("[INFO] ", msg))
log_warn <- function(msg) warning(paste0("[WARNING] ", msg))

# ===============================
# 1. LOAD
# ===============================
admayo_path <- file.path(DATA_PATH, "admayo.csv")

log_info(paste("Lendo:", admayo_path))
dt <- fread(admayo_path, sep = "auto")
log_info(paste("Carregado:", nrow(dt), "linhas ×", ncol(dt), "colunas"))

# ===============================
# 2. SELECÇÃO DE COLUNAS
# ===============================
# Grain:    USUBJID, AVISITN, AVISIT, PARAMCD, PARAM
# Valores:  AVAL, AVALC, AVALCAT1, BASE, CHG, PCHG, BASETYPE
# Flags:    ABLFL, APOBLFL, ANL01FL, DTYPE,
#           CRIT1FL, CRIT2FL, CRIT3FL, CRIT4FL, CRIT5FL,
#           WK8MISFL, WK8DISFL, WK44MIFL, WK44DIFL, ICE
# Datas:    ADT, ADTF, ADY
# Contexto: PARCAT1, PARAMTYP, SSVISIT, SSVISITN
# Sujeito:  TRT01P, TRT01A, TRT02P, TRT02A, TRT03P, TRT03A,
#           TR02PG1, TR02AG1, SAFFL, FASFL, RANDFL
# Diagnóstico: SRCDOM, SRCVAR, SRCSEQ, QSDTC, MODTC

COLS_KEEP <- c(
  # ── Grain ──────────────────────────────────────────────
  "USUBJID", "AVISITN", "AVISIT",
  "PARAMCD", "PARAM", "PARAMN", "PARAMTYP", "PARCAT1",

  # ── Valores principais ──────────────────────────────────
  "AVAL", "AVALC", "AVALCAT1",
  "BASE", "BASETYPE", "CHG", "PCHG",

  # ── Flags de análise ────────────────────────────────────
  "ABLFL", "APOBLFL", "ANL01FL", "DTYPE", "ICE",
  "CRIT1FL", "CRIT2FL", "CRIT3FL", "CRIT4FL", "CRIT5FL",
  "WK8MISFL", "WK8DISFL", "WK44MIFL", "WK44DIFL",

  # ── Datas / dia relativo ────────────────────────────────
  "ADT", "ADTF", "ADY",

  # ── Sub-study ───────────────────────────────────────────
  "SSVISIT", "SSVISITN",

  # ── Tratamento / população ──────────────────────────────
  "TRT01P", "TRT01A", "TRT02P", "TRT02A", "TRT03P", "TRT03A",
  "TR02PG1", "TR02AG1",
  "RANDFL", "SAFFL", "FASFL",

  # ── Rastreabilidade ─────────────────────────────────────
  "SRCDOM", "SRCVAR", "SRCSEQ", "QSDTC", "MODTC"
)

# Manter apenas colunas que existem no ficheiro
cols_present <- intersect(COLS_KEEP, names(dt))
cols_missing  <- setdiff(COLS_KEEP, names(dt))
if (length(cols_missing) > 0)
  log_warn(paste("Colunas não encontradas no ficheiro:",
                 paste(cols_missing, collapse = ", ")))

dt <- dt[, ..cols_present]

# ===============================
# 3. ORDENAÇÃO
# ===============================
# Ordenar por sujeito → visita → parâmetro para facilitar leitura
dt <- dt[order(USUBJID, AVISITN, PARAMN)]

# ===============================
# 4. VERIFICAÇÃO DE COMPLETUDE
# ===============================
n_rows      <- nrow(dt)
n_subjects  <- uniqueN(dt$USUBJID)
n_visits    <- uniqueN(dt$AVISITN)
n_paramcds  <- uniqueN(dt$PARAMCD)

log_info(paste("Linhas totais:         ", n_rows))
log_info(paste("Sujeitos únicos:       ", n_subjects))
log_info(paste("Visitas únicas:        ", n_visits))
log_info(paste("PARAMCDs únicos:       ", n_paramcds))
log_info(paste("Grain USUBJID×AVISITN×PARAMCD:",
               uniqueN(paste(dt$USUBJID, dt$AVISITN, dt$PARAMCD))))

# Contagem por PARAMCD com label — confirmar extracção completa
paramcd_summary <- dt[, .(
  label     = first(PARAM),
  n_rows    = .N,
  n_subjects = uniqueN(USUBJID),
  n_visits  = uniqueN(AVISITN),
  pct_aval_missing = round(mean(is.na(AVAL) | AVAL == "") * 100, 1)
), by = PARAMCD][order(PARAMCD)]

cat("\n=== COMPLETUDE POR PARAMCD ===\n")
cat(sprintf("%-15s %-45s %6s %8s %8s %12s\n",
            "PARAMCD", "PARAM (label)", "n_rows",
            "n_subj", "n_vis", "aval_miss%"))
cat(strrep("-", 100), "\n")
for (i in seq_len(nrow(paramcd_summary))) {
  r <- paramcd_summary[i]
  cat(sprintf("%-15s %-45s %6d %8d %8d %11.1f%%\n",
              r$PARAMCD, substr(r$label, 1, 45),
              r$n_rows, r$n_subjects, r$n_visits,
              r$pct_aval_missing))
}

# Verificar duplicatas no grain
dup_check <- dt[, .N, by = .(USUBJID, AVISITN, PARAMCD)][N > 1]
if (nrow(dup_check) > 0) {
  log_warn(paste("Duplicatas USUBJID×AVISITN×PARAMCD:",
                 nrow(dup_check), "combinações"))
} else {
  log_info("Grain OK — sem duplicatas USUBJID×AVISITN×PARAMCD")
}

# ===============================
# 5. SALVAR
# ===============================
write.csv(dt, OUTPUT, row.names = FALSE)
log_info(paste("Output salvo em:", OUTPUT))
log_info(paste("Colunas no output:", ncol(dt)))
