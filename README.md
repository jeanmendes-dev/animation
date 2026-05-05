# animation

cat("\n[INFO] Otimizando consolidação (modo rápido)...\n")

# calcular completude
all_ards[, NON_EMPTY := rowSums(!is.na(.SD) & .SD != ""), .SDcols = value_cols]

# ordenar por melhor linha
setorder(all_ards, .ROW_KEY, -NON_EMPTY)

# pegar melhor linha por chave
consolidated <- all_ards[
  ,
  .SD[1],
  by = .ROW_KEY
]

# remover coluna auxiliar
consolidated[, NON_EMPTY := NULL]
