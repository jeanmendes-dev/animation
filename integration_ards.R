# ============================================================
# CONSOLIDAÇÃO INTELIGENTE DE ARDs ADaM / ML READY DATASETS
# Chave: USUBJID + AVISIT + AVISITN
# ============================================================

library(tidyverse)
library(readr)
library(stringr)

# ============================================================
# CONFIGURAÇÃO
# ============================================================

CONFIG <- list(
  input_dir  = "/mnt/ards",        # pasta com os CSVs dos ARDs
  output_dir = "/mnt/ard_merged",  # pasta de saída
  
  # Se quiser informar arquivos manualmente, use:
  # input_files = c("/path/load1899.csv", "/path/load1901.csv")
  input_files = NULL,
  
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

is_empty_value <- function(x) {
  is.na(x) | str_trim(as.character(x)) == ""
}

normalize_value <- function(x) {
  x <- as.character(x)
  x <- ifelse(is.na(x), "", str_trim(x))
  x
}

make_key <- function(df, key_vars) {
  df %>%
    mutate(
      across(all_of(key_vars), ~ normalize_value(.x)),
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
  
  if (length(ux) == 0) {
    return(NA_character_)
  }
  
  if (length(ux) == 1) {
    return(ux)
  }
  
  paste(ux, collapse = " || ")
}

extract_load_name <- function(path) {
  tools::file_path_sans_ext(basename(path))
}

# ============================================================
# 1. DETECTAR E LER ARQUIVOS
# ============================================================

if (is.null(CONFIG$input_files)) {
  input_files <- list.files(
    CONFIG$input_dir,
    pattern = "\\.csv$",
    full.names = TRUE
  )
} else {
  input_files <- CONFIG$input_files
}

if (length(input_files) == 0) {
  stop("Nenhum arquivo CSV encontrado.")
}

message("[INFO] Arquivos encontrados:")
print(input_files)

read_ard <- function(file) {
  load_name <- extract_load_name(file)
  
  df <- read_csv(
    file,
    col_types = cols(.default = col_character()),
    show_col_types = FALSE,
    na = c("", "NA", "N/A", "NULL")
  )
  
  missing_keys <- setdiff(CONFIG$key_vars, names(df))
  
  if (length(missing_keys) > 0) {
    stop(
      paste0(
        "Arquivo ", file,
        " não possui as chaves obrigatórias: ",
        paste(missing_keys, collapse = ", ")
      )
    )
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

ard_list <- map(input_files, read_ard)

names(ard_list) <- map_chr(input_files, extract_load_name)

# ============================================================
# 2. RELATÓRIO POR LOAD
# ============================================================

load_report <- map2_dfr(
  ard_list,
  names(ard_list),
  ~ tibble(
    load = .y,
    source_file = unique(.x$.SOURCE_FILE),
    n_rows = nrow(.x),
    n_unique_keys = n_distinct(.x$.ROW_KEY),
    n_columns = ncol(.x) - 4
  )
)

# ============================================================
# 3. RELATÓRIO DE COLUNAS POR LOAD
# ============================================================

all_columns <- reduce(
  map(ard_list, names),
  union
)

technical_cols <- c(".ROW_KEY", ".SOURCE_LOAD", ".SOURCE_FILE", ".NON_EMPTY_COUNT")

column_report <- map2_dfr(
  ard_list,
  names(ard_list),
  ~ tibble(
    load = .y,
    column = setdiff(names(.x), technical_cols),
    present_in_load = TRUE
  )
)

column_origin_report <- column_report %>%
  group_by(column) %>%
  summarise(
    present_in_loads = paste(unique(load), collapse = " | "),
    n_loads = n_distinct(load),
    .groups = "drop"
  )

# ============================================================
# 4. EMPILHAR TODOS OS ARDs COM TODAS AS COLUNAS
# ============================================================

all_ards <- bind_rows(ard_list)

# Garantir que todas as colunas estejam como character
all_ards <- all_ards %>%
  mutate(across(everything(), as.character))

# ============================================================
# 5. CONSOLIDAÇÃO INTELIGENTE POR CHAVE
# ============================================================

value_cols <- setdiff(
  names(all_ards),
  c(".ROW_KEY", ".SOURCE_LOAD", ".SOURCE_FILE", ".NON_EMPTY_COUNT")
)

consolidated <- all_ards %>%
  group_by(.ROW_KEY) %>%
  summarise(
    across(
      all_of(value_cols),
      collapse_unique_values
    ),
    .SOURCE_LOADS = paste(unique(.SOURCE_LOAD), collapse = " | "),
    .SOURCE_FILES = paste(unique(.SOURCE_FILE), collapse = " | "),
    .N_SOURCE_ROWS = n(),
    .MAX_NON_EMPTY_COUNT = max(as.numeric(.NON_EMPTY_COUNT), na.rm = TRUE),
    .groups = "drop"
  )

# ============================================================
# 6. REMOVER COLUNA TÉCNICA E ORGANIZAR SAÍDA
# ============================================================

final_cols <- c(
  CONFIG$key_vars,
  setdiff(names(consolidated), c(".ROW_KEY", CONFIG$key_vars))
)

final_ard <- consolidated %>%
  select(all_of(final_cols))

# ============================================================
# 7. RELATÓRIO DE LINHAGEM POR LINHA
# ============================================================

row_lineage <- all_ards %>%
  group_by(.ROW_KEY) %>%
  summarise(
    USUBJID = first(USUBJID),
    AVISIT = first(AVISIT),
    AVISITN = first(AVISITN),
    source_loads = paste(unique(.SOURCE_LOAD), collapse = " | "),
    source_files = paste(unique(.SOURCE_FILE), collapse = " | "),
    n_source_rows = n(),
    max_non_empty_count = max(as.numeric(.NON_EMPTY_COUNT), na.rm = TRUE),
    min_non_empty_count = min(as.numeric(.NON_EMPTY_COUNT), na.rm = TRUE),
    duplicate_or_overlap = n() > 1,
    .groups = "drop"
  )

# ============================================================
# 8. RELATÓRIO FINAL DE RECONCILIAÇÃO
# ============================================================

total_input_rows <- sum(load_report$n_rows)
total_final_rows <- nrow(final_ard)
duplicates_removed <- total_input_rows - total_final_rows

reconciliation_report <- load_report %>%
  mutate(
    total_input_rows_all_loads = total_input_rows,
    total_final_rows = total_final_rows,
    total_duplicates_or_overlaps_removed = duplicates_removed
  )

# ============================================================
# 9. EXPORTAR RESULTADOS
# ============================================================

write_csv(
  final_ard,
  file.path(CONFIG$output_dir, CONFIG$output_csv),
  na = ""
)

write_csv(
  reconciliation_report,
  file.path(CONFIG$output_dir, CONFIG$reconciliation_report_csv),
  na = ""
)

write_csv(
  row_lineage,
  file.path(CONFIG$output_dir, CONFIG$row_lineage_csv),
  na = ""
)

write_csv(
  column_origin_report,
  file.path(CONFIG$output_dir, CONFIG$column_report_csv),
  na = ""
)

# ============================================================
# 10. MENSAGEM FINAL
# ============================================================

message("====================================================")
message("[DONE] Consolidação concluída.")
message("Arquivo consolidado: ",
        file.path(CONFIG$output_dir, CONFIG$output_csv))
message("Relatório de reconciliação: ",
        file.path(CONFIG$output_dir, CONFIG$reconciliation_report_csv))
message("Mapeamento de linhagem: ",
        file.path(CONFIG$output_dir, CONFIG$row_lineage_csv))
message("Relatório de colunas: ",
        file.path(CONFIG$output_dir, CONFIG$column_report_csv))
message("Linhas de entrada: ", total_input_rows)
message("Linhas finais: ", total_final_rows)
message("Duplicatas/overlaps removidos: ", duplicates_removed)
message("====================================================")