# animation

# Garantir que AVAL e AVALC existam SEMPRE
df_safe <- df %>%
  mutate(
    AVAL  = if ("AVAL" %in% names(.)) as.character(AVAL) else NA_character_,
    AVALC = if ("AVALC" %in% names(.)) as.character(AVALC) else NA_character_
  )

paramcd_values <- df_safe %>%
  filter(!is.na(PARAMCD)) %>%
  distinct(PARAMCD) %>%
  pull(PARAMCD) %>%
  as.character() %>%
  sort()

pivot_df <- df_safe %>%
  select(all_of(KEYS), PARAMCD, AVAL, AVALC) %>%
  filter(!is.na(PARAMCD)) %>%
  mutate(
    PARAMCD = as.character(PARAMCD),

    PARAM_VALUE = coalesce(AVAL, AVALC),

    VALUE_SOURCE = case_when(
      !is.na(AVAL)  ~ "AVAL",
      !is.na(AVALC) ~ "AVALC",
      TRUE ~ "VALUE"
    ),

    pivot_name = paste0(dataset_name, "_PARAMCD_", PARAMCD, "_", VALUE_SOURCE)
  ) %>%
  select(all_of(KEYS), pivot_name, PARAM_VALUE) %>%
  group_by(across(all_of(KEYS)), pivot_name) %>%
  summarise(
    PARAM_VALUE = {
      values <- unique(na.omit(PARAM_VALUE))
      if (length(values) == 0) NA_character_
      else paste(values, collapse = " | ")
    },
    .groups = "drop"
  ) %>%
  pivot_wider(
    names_from = pivot_name,
    values_from = PARAM_VALUE
  )
