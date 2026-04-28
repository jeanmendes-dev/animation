# animation

################ criar db a partir de dataset grande

library(DBI)
library(duckdb)

con <- dbConnect(duckdb(), "/domino/project/data/db/clinical_data.duckdb")

dbExecute(con, "
CREATE TABLE galaxi_ml AS
SELECT * FROM read_csv_auto('/domino/.../galaxi_ml_dataset.csv')
")


############## criar exports para o time

dbExecute(con, "
COPY (
  SELECT *
  FROM galaxi_ml
  WHERE PARAMCD = 'MES'
)
TO '/domino/project/data/exports/ard_mes.csv'
WITH (HEADER, DELIMITER ',')
")
