# ============================================================
# ARD MULTI-LOAD INTEGRATION
# Estratégia: Best row + fill missing from other loads
# ============================================================

library(data.table)

# ============================================================
# CONFIG
# ============================================================

CONFIG <- list(
  input_files = c(
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1899/data/ard/unifi_load1899_ard.csv",
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1901/data/ard/unifi_load1901_ard.csv",
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1903/data/ard/unifi_load1903_ard.csv"
  ),
  
  output_dir = "/mnt/ard_merged",
  
  key_vars = c("USUBJID", "AVISIT", "AVISITN"),
  
  # Prioridade em caso de empate de completude
  load_priority = c("load-1903", "load-1901", "load-1899")
)

dir.create(CONFIG$output_dir, showWarnings = FALSE, recursive = TRUE)

# ============================================================
# FUNÇÕES
# ============================================================

extract_load_name <- function(path) {
  parts <- unlist(strsplit(path, "/"))
  load_part <- parts[grepl("^load-[0-9]+$", parts)]
  if (length(load_part) > 0) return(tail(load_part, 1))
  tools::file_path_sans_ext(basename(path))
}

first_non_empty <- function(x) {
  x <- as.character(x)
  idx <- which(!is.na(x) & trimws(x) != "")
  if (length(idx) == 0) return(NA_character_)
  x[idx[1]]
}

normalize_empty_to_na <- function(dt, cols) {
  for (col in cols) {
    set(dt, i = which(trimws(as.character(dt[[col]])) == ""), j = col, value = NA_character_)
  }
  dt
}

# ============================================================
# 1. LER ARQUIVOS
# ============================================================

dt_list <- list()

for (f in CONFIG$input_files) {
  
  cat("\n[INFO] Lendo:", f, "\n")
  
  load_name <- extract_load_name(f)
  
  dt <- fread(
    f,
    colClasses = "character",
    na.strings = c("", "NA", "N/A", "NULL"),
    showProgress = TRUE
  )
  
  missing_keys <- setdiff(CONFIG$key_vars, names(dt))
  if (length(missing_keys) > 0) {
    stop("Arquivo sem chaves obrigatórias: ",
         paste(missing_keys, collapse = ", "),
         "\nArquivo: ", f)
  }
  
  for (k in CONFIG$key_vars) {
    dt[[k]] <- trimws(as.character(dt[[k]]))
  }
  
  dt[, .ROW_KEY := paste(USUBJID, AVISIT, AVISITN, sep = "||")]
  dt[, .SOURCE_LOAD := load_name]
  dt[, .SOURCE_FILE := basename(f)]
  
  dt_list[[load_name]] <- dt
}

# ============================================================
# 2. RELATÓRIO DE ENTRADA
# ============================================================

load_report <- rbindlist(lapply(names(dt_list), function(nm) {
  dt <- dt_list[[nm]]
  data.table(
    source_load = nm,
    source_file = unique(dt$.SOURCE_FILE),
    n_rows = nrow(dt),
    n_unique_keys = uniqueN(dt$.ROW_KEY),
    n_columns = ncol(dt)
  )
}), fill = TRUE)

# ============================================================
# 3. EMPILHAR TODOS OS LOADS
# ============================================================

cat("\n[INFO] Empilhando ARDs...\n")

all_ards <- rbindlist(dt_list, fill = TRUE, use.names = TRUE)

technical_cols <- c(".ROW_KEY", ".SOURCE_LOAD", ".SOURCE_FILE")
value_cols <- setdiff(names(all_ards), technical_cols)

# Normalizar vazios para NA somente nas colunas de valor
all_ards <- normalize_empty_to_na(all_ards, value_cols)

# ============================================================
# 4. CALCULAR COMPLETUDE E PRIORIDADE
# ============================================================

cat("\n[INFO] Calculando completude das linhas...\n")

all_ards[, .NON_EMPTY_COUNT := rowSums(!is.na(.SD)), .SDcols = value_cols]

all_ards[, .LOAD_PRIORITY := match(.SOURCE_LOAD, CONFIG$load_priority)]
all_ards[is.na(.LOAD_PRIORITY), .LOAD_PRIORITY := 999]

# Ordem:
# 1. mesma chave
# 2. linha mais completa primeiro
# 3. load prioritário em caso de empate
setorder(all_ards, .ROW_KEY, -.NON_EMPTY_COUNT, .LOAD_PRIORITY)

# ============================================================
# 5. LINHA BASE POR CHAVE
# ============================================================

cat("\n[INFO] Selecionando linha base por chave...\n")

base_rows <- all_ards[
  ,
  .SD[1],
  by = .ROW_KEY
]

base_info <- base_rows[
  ,
  .(
    .ROW_KEY,
    BASE_LOAD = .SOURCE_LOAD,
    BASE_FILE = .SOURCE_FILE,
    BASE_NON_EMPTY_COUNT = .NON_EMPTY_COUNT
  )
]

# ============================================================
# 6. MERGE COMPLEMENTAR ENTRE LOADS
# ============================================================

cat("\n[INFO] Fazendo merge complementar entre loads...\n")
cat("[INFO] Esta etapa preenche campos vazios usando outros loads da mesma chave.\n")

consolidated_values <- all_ards[
  ,
  lapply(.SD, first_non_empty),
  by = .ROW_KEY,
  .SDcols = value_cols
]

# ============================================================
# 7. INFORMAÇÕES DE LINHAGEM POR CHAVE
# ============================================================

cat("\n[INFO] Gerando row lineage...\n")

row_lineage <- all_ards[
  ,
  .(
    USUBJID = first(USUBJID),
    AVISIT = first(AVISIT),
    AVISITN = first(AVISITN),
    source_loads = paste(unique(.SOURCE_LOAD), collapse = " | "),
    source_files = paste(unique(.SOURCE_FILE), collapse = " | "),
    n_source_rows = .N,
    n_source_loads = uniqueN(.SOURCE_LOAD),
    duplicate_or_overlap = .N > 1,
    max_non_empty_count = max(.NON_EMPTY_COUNT, na.rm = TRUE)
  ),
  by = .ROW_KEY
]

row_lineage <- merge(
  row_lineage,
  base_info,
  by = ".ROW_KEY",
  all.x = TRUE
)

# ============================================================
# 8. ADICIONAR METADADOS AO CONSOLIDADO
# ============================================================

source_info <- row_lineage[
  ,
  .(
    .ROW_KEY,
    .SOURCE_LOADS = source_loads,
    .SOURCE_FILES = source_files,
    .N_SOURCE_ROWS = n_source_rows,
    .N_SOURCE_LOADS = n_source_loads,
    .BASE_LOAD = BASE_LOAD,
    .BASE_FILE = BASE_FILE,
    .BASE_NON_EMPTY_COUNT = BASE_NON_EMPTY_COUNT
  )
]

consolidated <- merge(
  consolidated_values,
  source_info,
  by = ".ROW_KEY",
  all.x = TRUE
)

# ============================================================
# 9. ORGANIZAR OUTPUT FINAL
# ============================================================

final_cols <- c(
  CONFIG$key_vars,
  setdiff(names(consolidated), c(".ROW_KEY", CONFIG$key_vars))
)

final_ard <- consolidated[, ..final_cols]

# ============================================================
# 10. RELATÓRIO DE COLUNAS POR LOAD
# ============================================================

cat("\n[INFO] Gerando column origin report...\n")

column_origin_report <- rbindlist(lapply(names(dt_list), function(nm) {
  
  dt <- copy(dt_list[[nm]])
  
  cols <- setdiff(names(dt), c(".ROW_KEY", ".SOURCE_LOAD", ".SOURCE_FILE"))
  
  data.table(
    source_load = nm,
    column = cols,
    non_empty_cells = sapply(cols, function(col) {
      sum(!is.na(dt[[col]]) & trimws(as.character(dt[[col]])) != "")
    }),
    total_rows = nrow(dt)
  )
  
}), fill = TRUE)

column_origin_report[
  ,
  fill_rate := round(non_empty_cells / total_rows, 4)
]

# ============================================================
# 11. RELATÓRIO DE RECONCILIAÇÃO
# ============================================================

total_input <- sum(load_report$n_rows)
total_final <- nrow(final_ard)

reconciliation_report <- copy(load_report)

reconciliation_report[, total_input_rows_all_loads := total_input]
reconciliation_report[, total_final_rows := total_final]
reconciliation_report[, duplicates_or_overlaps_resolved := total_input - total_final]
reconciliation_report[, integration_strategy := "best_row_plus_fill_missing_from_other_loads"]

# ============================================================
# 12. EXPORTAR
# ============================================================

cat("\n[INFO] Exportando outputs...\n")

fwrite(
  final_ard,
  file.path(CONFIG$output_dir, "ard_consolidated.csv"),
  na = ""
)

fwrite(
  reconciliation_report,
  file.path(CONFIG$output_dir, "reconciliation_report.csv"),
  na = ""
)

fwrite(
  row_lineage,
  file.path(CONFIG$output_dir, "row_lineage_mapping.csv"),
  na = ""
)

fwrite(
  column_origin_report,
  file.path(CONFIG$output_dir, "column_origin_report.csv"),
  na = ""
)

# ============================================================
# FINAL
# ============================================================

cat("\n=============================================\n")
cat("CONSOLIDAÇÃO FINALIZADA\n")
cat("Estratégia: best row + fill missing\n")
cat("Linhas de entrada:", total_input, "\n")
cat("Linhas finais:", total_final, "\n")
cat("Overlaps resolvidos:", total_input - total_final, "\n")
cat("Output:", file.path(CONFIG$output_dir, "ard_consolidated.csv"), "\n")
cat("=============================================\n")