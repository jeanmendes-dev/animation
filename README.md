# animation

paramcd_values <- df_safe %>%
  filter(!is.na(PARAMCD) & PARAMCD != "") %>%
  distinct(PARAMCD) %>%
  pull(PARAMCD) %>%
  as.character() %>%
  sort()
