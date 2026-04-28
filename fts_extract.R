library(data.table)
library(fst)

######################################## se quiser exportar as primeiras 1000 linhas
dt <- read_fst("/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_ml_dataset.fst")

fwrite(
  dt[1:1000, ],
  "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_sample_1000.csv"
)

######################################### se quiser selecionar as colunas do dataset que quero exportar

input_fst <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_ml_dataset.fst"

output_csv <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_extract_selected.csv"

cols_to_select <- c(
  "USUBJID",
  "AVISIT",
  "AVISITN",
  "STUDYID",
  "SITEID"
)

dt <- read_fst(
  input_fst,
  columns = cols_to_select,
  as.data.table = TRUE
)

fwrite(dt, output_csv)

cat("CSV exportado com sucesso em:\n", output_csv, "\n")

######################################### se quiser filtrar dados por visita

input_fst <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_ml_dataset.fst"

output_csv <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_extract_selected.csv"

cols_to_select <- c(
  "USUBJID",
  "AVISIT",
  "AVISITN",
  "STUDYID",
  "SITEID"
)

dt <- read_fst(
  input_fst,
  columns = cols_to_select,
  as.data.table = TRUE
)

dt_filtered <- dt[AVISIT == "Week 8"]

fwrite(dt_filtered, output_csv)

cat("CSV exportado com sucesso em:\n", output_csv, "\n")

######################################### se quiser filtrar dados por visita

input_fst <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_ml_dataset.fst"

output_csv <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_extract_selected.csv"

cols_to_select <- c(
  "USUBJID",
  "AVISIT",
  "AVISITN",
  "STUDYID",
  "SITEID"
)

dt <- read_fst(
  input_fst,
  columns = cols_to_select,
  as.data.table = TRUE
)

dt_filtered <- dt[AVISIT == "Week 8"]

fwrite(dt_filtered, output_csv)

cat("CSV exportado com sucesso em:\n", output_csv, "\n")

######################################### se quiser filtrar dados por parametro especifico

input_fst <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_ml_dataset.fst"

output_csv <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_extract_selected.csv"

cols_to_select <- c(
  "USUBJID",
  "AVISIT",
  "AVISITN",
  "STUDYID",
  "PARAMCD",
  "PARAM",
  "AVAL"
)

dt <- read_fst(
  input_fst,
  columns = cols_to_select,
  as.data.table = TRUE
)

dt_filtered <- dt[PARAMCD == "ENSCORE"]

fwrite(dt_filtered, output_csv)

cat("CSV exportado com sucesso em:\n", output_csv, "\n")




