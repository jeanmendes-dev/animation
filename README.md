# animation

library(data.table)
library(fst)
library(dplyr)

input_fst <- "/domino/datasets/local/clinical-trial-data/CNTO1275PUC3001-UNIFI-JR/load-2793/Data/_csv/ard/unifijr_load2793_ard.fst"

output_csv <- "/domino/datasets/local/clinical-trial-data/CNTO1275PUC3001-UNIFI-JR/load-2793/Data/_csv/ard/unifijr_load2793_extract_admayo_paramcd.csv"

dt <- read_fst(
  input_fst,
  as.data.table = TRUE
)

dt_selected <- dt %>%
  select(
    any_of(c("USUBJID", "AVISIT", "AVISITN")),
    starts_with("ADMAYO_PARAMCD_")
  )

fwrite(dt_selected, output_csv)

cat("CSV exportado com sucesso em:\n", output_csv, "\n")

xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx

dt_selected <- dt %>%
  select(
    any_of(c("USUBJID", "AVISIT", "AVISITN")),
    matches("^ADMAYO_PARAMCD_.*_AVAL$")
  )
