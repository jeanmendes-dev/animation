# animation

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

xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

# ============================================================
# 1. DETECTAR ARQUIVOS EM MÚLTIPLOS PATHS
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
      load_name = basename(dirname(dirname(p))) # pega "load-1903"
    )
  })
}

file_map <- get_all_csvs(CONFIG$input_paths)

if (nrow(file_map) == 0) {
  stop("Nenhum CSV encontrado nos paths informados.")
}

print(file_map)

xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

read_ard <- function(file, load_name) {
  
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

xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

ard_list <- pmap(
  list(file_map$file, file_map$load_name),
  read_ard
)

names(ard_list) <- paste0(file_map$load_name, "_", basename(file_map$file))
