# animation

consolidated <- all_ards[
  ,
  lapply(.SD, collapse_unique),
  by = .ROW_KEY,
  .SDcols = value_cols
]

source_info <- all_ards[
  ,
  .(
    .SOURCE_LOADS = paste(unique(.SOURCE_LOAD), collapse = " | "),
    .SOURCE_FILES = paste(unique(.SOURCE_FILE), collapse = " | "),
    .N_SOURCE_ROWS = .N
  ),
  by = .ROW_KEY
]

consolidated <- merge(
  consolidated,
  source_info,
  by = ".ROW_KEY",
  all.x = TRUE
)
