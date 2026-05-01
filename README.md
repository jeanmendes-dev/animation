# animation

coverage <- tibble(
  dataset = dataset_name,
  original_total_columns = original_n_cols,
  extracted_total_columns = extracted_n_cols,
  coverage_percent = round((extracted_n_cols / original_n_cols) * 100, 2),

  has_paramcd = has_paramcd,

  # NOVO (mais completo)
  has_aval  = has_aval,
  has_avalc = has_avalc,
  has_any_value = has_any_value,

  n_paramcd_extracted = length(paramcd_values),
  paramcd_extracted = paste(paramcd_values, collapse = ", "),

  is_100_percent_coverage = coverage_percent >= 100
)
