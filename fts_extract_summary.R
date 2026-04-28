library(data.table)
library(fst)

# ============================================================
# CONFIG
# ============================================================

input_fst <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_ml_dataset.fst"

output_summary <- "/domino/datasets/local/clinical-trial-data/CNT01959CRD3001-GALAXI-GAL1-WK48/load-1358/data/db/galaxi1_summary.csv"

# ============================================================
# CDISC / ADaM dictionary básico
# ============================================================

dataset_dict <- c(
  ADAE = "Adverse Events",
  ADSL = "Subject-Level Analysis Dataset",
  ADEX = "Exposure",
  ADLB = "Laboratory Test Results",
  ADLBEF = "Laboratory Efficacy",
  ADVS = "Vital Signs",
  ADTTE = "Time-to-Event",
  ADDV = "Protocol Deviations",
  ADIS = "Immunogenicity Specimen",
  ADPC = "Pharmacokinetics Concentrations",
  ADIE = "Inclusion/Exclusion Criteria",
  ADMHI = "Medical History",
  BIOMARKER = "Biomarker-derived features"
)

variable_dict <- c(
  SAFFL = "Safety population flag",
  FASFL = "Full analysis set flag",
  RANDFL = "Randomized population flag",
  PASFL = "Per-protocol analysis set flag",
  USMFL = "Use in analysis flag",
  STUDYID = "Study identifier",
  SUBJID = "Subject identifier",
  SITEID = "Site identifier",
  AGE = "Age",
  AGEU = "Age unit",
  SEX = "Sex",
  RACE = "Race",
  COUNTRY = "Country",
  ETHNIC = "Ethnicity",
  PARAMCD = "Parameter code",
  PARAM = "Parameter description",
  AVAL = "Analysis value",
  AVALC = "Analysis character value",
  BASE = "Baseline value",
  CHG = "Change from baseline",
  PCHG = "Percent change from baseline",
  ABLFL = "Baseline record flag",
  APOBLFL = "Post-baseline record flag",
  ANL01FL = "Analysis flag 01",
  ANL02FL = "Analysis flag 02",
  VISIT = "Visit name",
  VISITNUM = "Visit number",
  AVISIT = "Analysis visit",
  AVISITN = "Analysis visit number",
  TRT01P = "Planned treatment for period 01",
  TRT01A = "Actual treatment for period 01",
  TRT02P = "Planned treatment for period 02",
  TRT02A = "Actual treatment for period 02",
  TRTSDT = "Treatment start date",
  TRTEDT = "Treatment end date"
)

# ============================================================
# READ DATA
# ============================================================

dt <- read_fst(input_fst, as.data.table = TRUE)

# ============================================================
# STUDY-LEVEL SUMMARY
# ============================================================

n_subjects <- uniqueN(dt$USUBJID)

visit_values <- sort(unique(na.omit(dt$AVISIT)))
n_visits <- length(visit_values)
visit_list <- paste(visit_values, collapse = " | ")

# ============================================================
# COLUMN-LEVEL SUMMARY
# ============================================================

cols <- names(dt)

summary_dt <- rbindlist(lapply(cols, function(col) {
  
  parts <- strsplit(col, "_")[[1]]
  
  prefix <- ifelse(length(parts) >= 1, parts[1], NA)
  dataset <- ifelse(length(parts) >= 2, parts[2], NA)
  variable <- ifelse(length(parts) >= 3, paste(parts[-c(1, 2)], collapse = "_"), col)
  
  if (col %in% c("USUBJID", "AVISIT", "AVISITN")) {
    prefix <- "KEY"
    dataset <- "ARD"
    variable <- col
  }
  
  values <- unique(na.omit(as.character(dt[[col]])))
  example_values <- paste(head(values, 5), collapse = " | ")
  
  data.table(
    study_dataset = "GALAXI1_ML_DATASET",
    n_unique_usubjid = n_subjects,
    n_visit_windows = n_visits,
    visit_windows = visit_list,
    column_name = col,
    prefix_type = prefix,
    source_dataset = dataset,
    source_dataset_meaning = ifelse(dataset %in% names(dataset_dict), dataset_dict[[dataset]], NA),
    derived_variable = variable,
    derived_variable_meaning = ifelse(variable %in% names(variable_dict), variable_dict[[variable]], NA),
    example_output = example_values
  )
}))

# ============================================================
# EXPORT
# ============================================================

fwrite(summary_dt, output_summary)

cat("Summary CSV criado em:\n", output_summary, "\n")