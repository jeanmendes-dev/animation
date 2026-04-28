# animation

/domino/project/

├── data/
│   ├── raw/                     # Dados brutos (SDTM, SAS, etc)
│   ├── processed/               # Dados intermediários
│   ├── db/                      # 🔥 DuckDB fica aqui
│   │   └── clinical_data.duckdb
│   ├── exports/                 # CSVs leves para o time
│   │   ├── ard_mes.csv
│   │   ├── ard_summary.csv
│   │   └── sample_1000_rows.csv
│   └── parquet/                 # (opcional) dados otimizados
│
├── scripts/
│   ├── 01_ingestion.R
│   ├── 02_build_duckdb.R
│   ├── 03_feature_engineering.R
│   ├── 04_export_csv.R
│
├── outputs/
│   ├── reports/
│   └── logs/
│
└── README.md

To avoid performance issues with very large CSV files, I’m keeping the full compiled dataset in DuckDB as the master analytical database. From this master file, I can export smaller CSV extracts for review, sharing, and validation, depending on the specific study, domain, parameters, or variables needed.
