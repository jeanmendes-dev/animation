# animation

# ============================================================
# 6. MERGE COMPLEMENTAR ENTRE LOADS - VERSÃO RÁPIDA
# ============================================================

cat("\n[INFO] Fazendo merge complementar otimizado...\n")

# Começa com a melhor linha por chave
consolidated_values <- copy(base_rows)

# Remover colunas técnicas que não devem ser preenchidas como dados
fill_cols <- setdiff(
  value_cols,
  c(".NON_EMPTY_COUNT", ".LOAD_PRIORITY")
)

# Relatório de contribuição por load/coluna
fill_contribution <- list()

# Registrar contribuição da linha base
base_contrib <- rbindlist(lapply(fill_cols, function(col) {
  data.table(
    column = col,
    source_load = "BASE_ROW",
    filled_cells = sum(!is.na(consolidated_values[[col]]) & trimws(consolidated_values[[col]]) != "")
  )
}))

fill_contribution[["BASE_ROW"]] <- base_contrib

# Usar os outros loads para preencher campos vazios
for (ld in CONFIG$load_priority) {
  
  cat("[INFO] Complementando com:", ld, "\n")
  
  candidate <- all_ards[.SOURCE_LOAD == ld]
  
  if (nrow(candidate) == 0) next
  
  # uma linha por chave dentro do load
  candidate <- candidate[
    ,
    .SD[1],
    by = .ROW_KEY
  ]
  
  # alinhar candidate à ordem do consolidated_values
  candidate <- candidate[match(consolidated_values$.ROW_KEY, candidate$.ROW_KEY)]
  
  contrib_ld <- list()
  
  for (col in fill_cols) {
    
    base_val <- consolidated_values[[col]]
    cand_val <- candidate[[col]]
    
    idx_fill <- (
      (is.na(base_val) | trimws(base_val) == "") &
        (!is.na(cand_val) & trimws(cand_val) != "")
    )
    
    n_fill <- sum(idx_fill, na.rm = TRUE)
    
    if (n_fill > 0) {
      set(consolidated_values, which(idx_fill), col, cand_val[idx_fill])
    }
    
    contrib_ld[[col]] <- data.table(
      column = col,
      source_load = ld,
      filled_cells = n_fill
    )
  }
  
  fill_contribution[[ld]] <- rbindlist(contrib_ld)
}

fill_contribution_report <- rbindlist(fill_contribution, fill = TRUE)

# Remover colunas técnicas auxiliares do consolidado
cols_to_remove <- intersect(
  c(".SOURCE_LOAD", ".SOURCE_FILE", ".NON_EMPTY_COUNT", ".LOAD_PRIORITY"),
  names(consolidated_values)
)

consolidated_values[, (cols_to_remove) := NULL]

cat("[INFO] Merge complementar finalizado.\n")

xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

fwrite(
  fill_contribution_report,
  file.path(CONFIG$output_dir, "fill_contribution_report.csv"),
  na = ""
)
