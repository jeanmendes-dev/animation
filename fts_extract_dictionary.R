library(data.table)
library(fst)
library(stringr)

# ============================================================
# INPUT / OUTPUT
# ============================================================

input_fst <- "/domino/.../galaxi1_ml_dataset.fst"
output_summary <- "/domino/.../galaxi1_summary_v2.csv"

dt <- read_fst(input_fst, as.data.table = TRUE)

# ============================================================
# DATASET DICTIONARY (expandido)
# ============================================================

dataset_dict <- c(
  ADAE = "Adverse Events",
  ADSL = "Subject-Level Analysis Dataset",
  ADEX = "Exposure",
  ADLB = "Laboratory",
  ADLBEF = "Laboratory Efficacy",
  ADVS = "Vital Signs",
  ADTTE = "Time-to-Event",
  ADDV = "Protocol Deviations",
  ADIS = "Immunogenicity",
  ADPC = "Pharmacokinetics",
  ADIE = "Inclusion/Exclusion",
  ADMHI = "Medical History",
  ADMEDRVW = "Medication Review",
  ADPROMIS = "PROMIS Scores",
  ADWPAI = "Work Productivity",
  BIOMARKER = "Biomarker Features",
  ADCDAI = "Crohn’s Disease Activity Index",
  ADCORT = "Corticosteroid Use",
  ADIBDQ = "IBDQ Score",
  ADPGI = "Patient Global Impression",
  ADSESCD = "SES-CD Score"
)

# ============================================================
# VARIABLE DICTIONARY (expandido)
# ============================================================

variable_dict <- c(
  SAFFL = "Safety population flag (Y/N)",
  FASFL = "Full analysis set flag (Y/N)",
  RANDFL = "Randomized flag (Y/N)",
  PASFL = "Per protocol flag (Y/N)",
  USMFL = "Used in model flag (Y/N)",
  TRTEMFL = "Treatment emergent flag (Y/N)",
  AGE = "Age",
  AGEU = "Age unit",
  SEX = "Sex",
  RACE = "Race",
  COUNTRY = "Country",
  PARAMCD = "Parameter code",
  PARAM = "Parameter description",
  AVAL = "Analysis value",
  AVALC = "Analysis character value",
  BASE = "Baseline",
  CHG = "Change from baseline",
  PCHG = "Percent change",
  ABLFL = "Baseline flag",
  APOBLFL = "Post-baseline flag",
  ANL01FL = "Analysis flag",
  ANL02FL = "Analysis flag",
  VISIT = "Visit",
  VISITNUM = "Visit number",
  AVISIT = "Analysis visit",
  AVISITN = "Analysis visit number",
  TRT01P = "Planned treatment",
  TRT01A = "Actual treatment",
  TRTSDT = "Treatment start date",
  TRTEDT = "Treatment end date",
  ASTDT = "Analysis start date",
  AENDT = "Analysis end date",
  ASTDY = "Analysis start day",
  AENDY = "Analysis end day",
  AESEQ = "Adverse event sequence",
  AETERM = "Adverse event term",
  AEDECOD = "MedDRA preferred term",
  AEBODSYS = "Body system",
  AESEV = "Severity",
  AESER = "Serious event flag",
  AEOUT = "Outcome",
  AEREL = "Relationship to treatment"
)

# ============================================================
# HELPER FUNCTIONS
# ============================================================

extract_dataset <- function(col) {
  parts <- strsplit(col, "_")[[1]]
  
  if (length(parts) >= 2) {
    return(parts[2])
  }
  
  return("UNKNOWN_DATASET")
}

extract_variable <- function(col) {
  parts <- strsplit(col, "_")[[1]]
  
  if (length(parts) >= 3) {
    return(paste(parts[-c(1,2)], collapse = "_"))
  }
  
  return(col)
}

get_dataset_meaning <- function(ds) {
  if (ds %in% names(dataset_dict)) {
    return(dataset_dict[[ds]])
  } else {
    return(paste("Unknown dataset:", ds))
  }
}

get_variable_meaning <- function(var) {
  
  # match exato
  if (var %in% names(variable_dict)) {
    return(variable_dict[[var]])
  }
  
  # match parcial (ex: TRT01AN → TRT01A)
  base_var <- str_extract(var, "^[A-Z]+")
  
  if (!is.na(base_var) && base_var %in% names(variable_dict)) {
    return(variable_dict[[base_var]])
  }
  
  return(paste("Derived variable:", var))
}

# ============================================================
# BUILD SUMMARY
# ============================================================

summary_dt <- rbindlist(lapply(names(dt), function(col) {
  
  dataset <- extract_dataset(col)
  variable <- extract_variable(col)
  
  values <- unique(na.omit(as.character(dt[[col]])))
  example <- paste(head(values, 5), collapse = " | ")
  
  data.table(
    column_name = col,
    source_dataset = dataset,
    source_dataset_meaning = get_dataset_meaning(dataset),
    derived_variable = variable,
    derived_variable_meaning = get_variable_meaning(variable),
    example_output = example
  )
}))

# ============================================================
# EXPORT
# ============================================================

fwrite(summary_dt, output_summary)

cat("Summary atualizado criado em:\n", output_summary)