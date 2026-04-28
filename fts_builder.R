install.packages("fst")

library(data.table)
library(fst)

input_csv <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_ml_dataset.csv"

output_fst <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_ml_dataset.fst"

dt <- fread(input_csv)

write_fst(dt, output_fst)

cat("Arquivo FST criado em:", output_fst, "\n")

