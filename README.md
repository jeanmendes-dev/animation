# animation

# ============================================================
# CONSOLIDAÇÃO INTELIGENTE DE ARDs - VERSÃO OTIMIZADA
# ============================================================

library(data.table)
library(stringr)

CONFIG <- list(
  input_files = c(
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1899/data/ard/unifi_load1899_ard.csv",
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1901/data/ard/unifi_load1901_ard.csv",
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1903/data/ard/unifi_load1903_ard.csv"
  ),
  output_dir = "/mnt/ard_merged",
  key_vars = c("USUBJID", "AVISIT", "AVISITN")
)

dir.create(CONFIG$output_dir, showWarnings = FALSE, recursive = TRUE)

extract_load_name <- function(path) {
  parts <- unlist(strsplit(path, "/"))
  load_part <- parts[grepl("^load-[0-9]+$", parts)]
  if (length(load_part) > 0) return(tail(load_part, 1))
  tools::file_path_sans_ext(basename(path))
}

collapse_unique <- function(x) {
  x <- as.character(x)
  x <- trimws(x)
  x <- x[!is.na(x) & x != ""]
  ux <- unique(x)
  if (length(ux) == 0) return(NA_character_)
  if (length(ux) == 1) return(ux)
  paste(ux, collapse = " || ")
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
# 2. RELATÓRIOS DE ENTRADA
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
# 3. EMPILHAR
# ============================================================

cat("\n[INFO] Empilhando ARDs...\n")

all_ards <- rbindlist(dt_list, fill = TRUE, use.names = TRUE)

# ============================================================
# 4. CONSOLIDAR POR CHAVE
# ============================================================

cat("\n[INFO] Consolidando por chave. Esta etapa pode demorar alguns minutos...\n")

technical_cols <- c(".ROW_KEY", ".SOURCE_LOAD", ".SOURCE_FILE")
value_cols <- setdiff(names(all_ards), technical_cols)

consolidated <- all_ards[
  ,
  c(
    lapply(.SD, collapse_unique),
    list(
      .SOURCE_LOADS = paste(unique(.SOURCE_LOAD), collapse = " | "),
      .SOURCE_FILES = paste(unique(.SOURCE_FILE), collapse = " | "),
      .N_SOURCE_ROWS = .N
    )
  ),
  by = .ROW_KEY,
  .SDcols = value_cols
]

# ============================================================
# 5. ORGANIZAR OUTPUT FINAL
# ============================================================

final_cols <- c(
  CONFIG$key_vars,
  setdiff(names(consolidated), c(".ROW_KEY", CONFIG$key_vars))
)

final_ard <- consolidated[, ..final_cols]

# ============================================================
# 6. LINHAGEM
# ============================================================

row_lineage <- all_ards[
  ,
  .(
    USUBJID = first(USUBJID),
    AVISIT = first(AVISIT),
    AVISITN = first(AVISITN),
    source_loads = paste(unique(.SOURCE_LOAD), collapse = " | "),
    source_files = paste(unique(.SOURCE_FILE), collapse = " | "),
    n_source_rows = .N,
    duplicate_or_overlap = .N > 1
  ),
  by = .ROW_KEY
]

# ============================================================
# 7. RELATÓRIO DE COLUNAS
# ============================================================

column_origin_report <- rbindlist(lapply(names(dt_list), function(nm) {
  data.table(
    source_load = nm,
    column = names(dt_list[[nm]])
  )
}), fill = TRUE)[
  !column %in% technical_cols,
  .(
    present_in_loads = paste(unique(source_load), collapse = " | "),
    n_loads = uniqueN(source_load)
  ),
  by = column
]

# ============================================================
# 8. RELATÓRIO FINAL
# ============================================================

total_input <- sum(load_report$n_rows)
total_final <- nrow(final_ard)

reconciliation_report <- copy(load_report)
reconciliation_report[, total_input_rows_all_loads := total_input]
reconciliation_report[, total_final_rows := total_final]
reconciliation_report[, duplicates_or_overlaps_removed := total_input - total_final]

# ============================================================
# 9. EXPORTAR
# ============================================================

cat("\n[INFO] Exportando arquivos...\n")

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

cat("\n=============================================\n")
cat("CONSOLIDAÇÃO FINALIZADA\n")
cat("Linhas de entrada:", total_input, "\n")
cat("Linhas finais:", total_final, "\n")
cat("Duplicatas/overlaps removidos:", total_input - total_final, "\n")
cat("Output:", file.path(CONFIG$output_dir, "ard_consolidated.csv"), "\n")
cat("=============================================\n")
