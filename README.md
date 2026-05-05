# animation

# ============================================================
# CONSOLIDAÇÃO INTELIGENTE DE ARDs (MULTI-LOAD)
# ============================================================

library(tidyverse)
library(readr)
library(stringr)
library(purrr)

# ============================================================
# CONFIGURAÇÃO
# ============================================================

CONFIG <- list(
  input_paths = c(
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI-LTE/load-1899/Data/csv",
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI-LTE/load-1901/Data/csv",
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI-LTE/load-1903/Data/csv"
  ),
  
  output_dir = "/mnt/ard_merged",
  
  key_vars = c("USUBJID", "AVISIT", "AVISITN"),
  
  output_csv = "ard_consolidated.csv",
  reconciliation_report_csv = "reconciliation_report.csv",
  row_lineage_csv = "row_lineage_mapping.csv",
  column_report_csv = "column_origin_report.csv"
)

dir.create(CONFIG$output_dir, showWarnings = FALSE, recursive = TRUE)

# ============================================================
# FUNÇÕES AUXILIARES
# ============================================================

normalize_value <- function(x) {
  x <- as.character(x)
  x <- ifelse(is.na(x), "", str_trim(x))
  x
}

is_empty_value <- function(x) {
  is.na(x) | str_trim(as.character(x)) == ""
}

make_key <- function(df, key_vars) {
  df %>%
    mutate(
      across(all_of(key_vars), normalize_value),
      .ROW_KEY = paste(!!!syms(key_vars), sep = "||")
    )
}

count_non_empty <- function(df, exclude_cols = character()) {
  cols <- setdiff(names(df), exclude_cols)
  rowSums(!sapply(df[cols], is_empty_value))
}

collapse_unique_values <- function(x) {
  x <- normalize_value(x)
  x <- x[x != ""]
  ux <- unique(x)
  
  if (length(ux) == 0) return(NA_character_)
  if (length(ux) == 1) return(ux)
  
  paste(ux, collapse = " || ")
}

# ============================================================
# 1. DETECTAR TODOS OS CSVs DOS LOADS
# ============================================================

get_all_csvs <- function(paths) {
  map_dfr(paths, function(p) {
    
    files <- list.files(
      path = p,
      pattern = "\\.csv$",
      full.names = TRUE
    )
    
    if (length(files) == 0) {
      message("[WARNING] Nenhum CSV encontrado em: ", p)
      return(tibble())
    }
    
    tibble(
      file = files,
      load_path = p,
      load_name = basename(dirname(dirname(p))) # ex: load-1903
    )
  })
}

file_map <- get_all_csvs(CONFIG$input_paths)

if (nrow(file_map) == 0) {
  stop("Nenhum CSV encontrado nos paths informados.")
}

cat("\n[INFO] Arquivos detectados:\n")
print(file_map)

# ============================================================
# 2. LEITURA DOS DATASETS
# ============================================================

read_ard <- function(file, load_name) {
  
  df <- read_csv(
    file,
    col_types = cols(.default = col_character()),
    show_col_types = FALSE,
    na = c("", "NA", "N/A", "NULL")
  )
  
  missing_keys <- setdiff(CONFIG$key_vars, names(df))
  
  if (length(missing_keys) > 0) {
    stop(paste0("Arquivo ", file, " não possui as chaves: ",
                paste(missing_keys, collapse = ", ")))
  }
  
  df %>%
    make_key(CONFIG$key_vars) %>%
    mutate(
      .SOURCE_LOAD = load_name,
      .SOURCE_FILE = basename(file),
      .NON_EMPTY_COUNT = count_non_empty(
        .,
        exclude_cols = c(".ROW_KEY", ".SOURCE_LOAD", ".SOURCE_FILE")
      )
    )
}

ard_list <- pmap(
  list(file_map$file, file_map$load_name),
  read_ard
)

names(ard_list) <- paste0(file_map$load_name, "_", basename(file_map$file))

# ============================================================
# 3. RELATÓRIO POR LOAD
# ============================================================

load_report <- map2_dfr(
  ard_list,
  names(ard_list),
  ~ tibble(
    dataset = .y,
    n_rows = nrow(.x),
    n_unique_keys = n_distinct(.x$.ROW_KEY),
    n_columns = ncol(.x) - 4
  )
)

# ============================================================
# 4. RELATÓRIO DE COLUNAS
# ============================================================

technical_cols <- c(".ROW_KEY", ".SOURCE_LOAD", ".SOURCE_FILE", ".NON_EMPTY_COUNT")

column_report <- map2_dfr(
  ard_list,
  names(ard_list),
  ~ tibble(
    dataset = .y,
    column = setdiff(names(.x), technical_cols)
  )
)

column_origin_report <- column_report %>%
  group_by(column) %>%
  summarise(
    present_in = paste(unique(dataset), collapse = " | "),
    n_datasets = n_distinct(dataset),
    .groups = "drop"
  )

# ============================================================
# 5. EMPILHAR TODOS OS ARDs
# ============================================================

all_ards <- bind_rows(ard_list) %>%
  mutate(across(everything(), as.character))

# ============================================================
# 6. CONSOLIDAÇÃO INTELIGENTE
# ============================================================

value_cols <- setdiff(
  names(all_ards),
  c(".ROW_KEY", ".SOURCE_LOAD", ".SOURCE_FILE", ".NON_EMPTY_COUNT")
)

consolidated <- all_ards %>%
  group_by(.ROW_KEY) %>%
  summarise(
    across(all_of(value_cols), collapse_unique_values),
    .SOURCE_LOADS = paste(unique(.SOURCE_LOAD), collapse = " | "),
    .SOURCE_FILES = paste(unique(.SOURCE_FILE), collapse = " | "),
    .N_SOURCE_ROWS = n(),
    .MAX_COMPLETENESS = max(as.numeric(.NON_EMPTY_COUNT), na.rm = TRUE),
    .groups = "drop"
  )

# ============================================================
# 7. ORGANIZAR DATASET FINAL
# ============================================================

final_cols <- c(
  CONFIG$key_vars,
  setdiff(names(consolidated), c(".ROW_KEY", CONFIG$key_vars))
)

final_ard <- consolidated %>%
  select(all_of(final_cols))

# ============================================================
# 8. LINHAGEM POR LINHA
# ============================================================

row_lineage <- all_ards %>%
  group_by(.ROW_KEY) %>%
  summarise(
    USUBJID = first(USUBJID),
    AVISIT = first(AVISIT),
    AVISITN = first(AVISITN),
    source_loads = paste(unique(.SOURCE_LOAD), collapse = " | "),
    source_files = paste(unique(.SOURCE_FILE), collapse = " | "),
    n_rows = n(),
    max_completeness = max(as.numeric(.NON_EMPTY_COUNT), na.rm = TRUE),
    duplicate_flag = n() > 1,
    .groups = "drop"
  )

# ============================================================
# 9. RELATÓRIO FINAL
# ============================================================

total_input <- sum(load_report$n_rows)
total_final <- nrow(final_ard)

reconciliation_report <- load_report %>%
  mutate(
    total_input_rows = total_input,
    final_rows = total_final,
    duplicates_removed = total_input - total_final
  )

# ============================================================
# 10. EXPORTAR
# ============================================================

write_csv(final_ard,
          file.path(CONFIG$output_dir, CONFIG$output_csv),
          na = "")

write_csv(reconciliation_report,
          file.path(CONFIG$output_dir, CONFIG$reconciliation_report_csv),
          na = "")

write_csv(row_lineage,
          file.path(CONFIG$output_dir, CONFIG$row_lineage_csv),
          na = "")

write_csv(column_origin_report,
          file.path(CONFIG$output_dir, CONFIG$column_report_csv),
          na = "")

# ============================================================
# FINAL
# ============================================================

cat("\n=============================================\n")
cat("CONSOLIDAÇÃO FINALIZADA\n")
cat("Linhas entrada:", total_input, "\n")
cat("Linhas finais:", total_final, "\n")
cat("Removidos:", total_input - total_final, "\n")
cat("=============================================\n")
