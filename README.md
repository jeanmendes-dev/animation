# animation

# ============================================================
# CONSOLIDAÇÃO INTELIGENTE DE ARDs (MULTI-LOAD)
# Aceita paths de pastas OU arquivos CSV diretamente
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
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1899/data/ard/unifi_load1899_ard.csv",
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1901/data/ard/unifi_load1901_ard.csv",
    "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1903/data/ard/unifi_load1903_ard.csv"
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

extract_load_name <- function(path) {
  parts <- str_split(path, "/", simplify = TRUE)
  load_part <- parts[str_detect(parts, "^load-[0-9]+$")]
  
  if (length(load_part) > 0) {
    return(load_part[length(load_part)])
  }
  
  tools::file_path_sans_ext(basename(path))
}

# ============================================================
# 1. DETECTAR ARQUIVOS CSV
# ============================================================

get_all_csvs <- function(paths) {
  
  map_dfr(paths, function(p) {
    
    if (file.exists(p) && !dir.exists(p) && str_detect(tolower(p), "\\.csv$")) {
      
      return(tibble(
        file = p,
        load_path = dirname(p),
        load_name = extract_load_name(p)
      ))
      
    } else if (dir.exists(p)) {
      
      files <- list.files(
        path = p,
        pattern = "\\.csv$",
        full.names = TRUE
      )
      
      if (length(files) == 0) {
        message("[WARNING] Nenhum CSV encontrado na pasta: ", p)
        return(tibble())
      }
      
      return(tibble(
        file = files,
        load_path = p,
        load_name = map_chr(files, extract_load_name)
      ))
      
    } else {
      
      message("[WARNING] Path não encontrado ou inválido: ", p)
      return(tibble())
    }
  })
}

file_map <- get_all_csvs(CONFIG$input_paths)

if (nrow(file_map) == 0) {
  stop("Nenhum CSV válido encontrado nos paths informados.")
}

cat("\n[INFO] Arquivos detectados:\n")
print(file_map)

# ============================================================
# 2. LEITURA DOS ARDs
# ============================================================

read_ard <- function(file, load_name) {
  
  cat("\n[INFO] Lendo arquivo:", file, "\n")
  
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

ard_list <- pmap(
  list(file_map$file, file_map$load_name),
  read_ard
)

names(ard_list) <- paste0(file_map$load_name, "_", basename(file_map$file))

# ============================================================
# 3. RELATÓRIO POR LOAD / ARQUIVO
# ============================================================

load_report <- map2_dfr(
  ard_list,
  names(ard_list),
  ~ tibble(
    dataset = .y,
    source_load = unique(.x$.SOURCE_LOAD),
    source_file = unique(.x$.SOURCE_FILE),
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
    source_load = unique(.x$.SOURCE_LOAD),
    column = setdiff(names(.x), technical_cols)
  )
)

column_origin_report <- column_report %>%
  group_by(column) %>%
  summarise(
    present_in_loads = paste(unique(source_load), collapse = " | "),
    present_in_datasets = paste(unique(dataset), collapse = " | "),
    n_loads = n_distinct(source_load),
    .groups = "drop"
  )

# ============================================================
# 5. EMPILHAR TODOS OS ARDs
# ============================================================

all_ards <- bind_rows(ard_list) %>%
  mutate(across(everything(), as.character))

# ============================================================
# 6. CONSOLIDAÇÃO INTELIGENTE POR CHAVE
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
    n_source_rows = n(),
    max_completeness = max(as.numeric(.NON_EMPTY_COUNT), na.rm = TRUE),
    duplicate_or_overlap = n() > 1,
    .groups = "drop"
  )

# ============================================================
# 9. RELATÓRIO FINAL
# ============================================================

total_input <- sum(load_report$n_rows)
total_final <- nrow(final_ard)

reconciliation_report <- load_report %>%
  mutate(
    total_input_rows_all_loads = total_input,
    total_final_rows = total_final,
    duplicates_or_overlaps_removed = total_input - total_final
  )

# ============================================================
# 10. EXPORTAR RESULTADOS
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
# FINAL
# ============================================================

cat("\n=============================================\n")
cat("CONSOLIDAÇÃO FINALIZADA\n")
cat("Linhas de entrada:", total_input, "\n")
cat("Linhas finais:", total_final, "\n")
cat("Duplicatas/overlaps removidos:", total_input - total_final, "\n")
cat("Output:", file.path(CONFIG$output_dir, CONFIG$output_csv), "\n")
cat("=============================================\n")
