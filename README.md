# animation

#===============================================================================
# FIGARO UC1 - Create ADMAYO-like dataset from SDTM
# Sources: QS* domains + MO + DM + SV
#===============================================================================

library(data.table)
library(dplyr)
library(stringr)
library(lubridate)
library(readr)

#===============================================================================
# CONFIG
#===============================================================================

CONFIG <- list(
  input_dir  = "/domino/datasets/local/clinical-trial-data/SHP647UC301-FIGARO-UC1/data_updated/csv",
  output_dir = "/domino/datasets/local/clinical-trial-data/SHP647UC301-FIGARO-UC1/data_updated/adam_like",
  output_file = "admayo_figaro_uc1.csv"
)

dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)

log_msg <- function(...) {
  cat("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", ..., "\n", sep = "")
}

#===============================================================================
# HELPERS
#===============================================================================

read_sdtm <- function(domain) {
  file <- file.path(CONFIG$input_dir, paste0(domain, ".csv"))
  
  if (!file.exists(file)) {
    log_msg("WARNING: File not found: ", file)
    return(NULL)
  }
  
  log_msg("Reading: ", domain)
  df <- fread(file, sep = "auto", na.strings = c("", "NA", "N/A", "."))
  names(df) <- toupper(names(df))
  df
}

safe_date <- function(x) {
  suppressWarnings(as.Date(substr(as.character(x), 1, 10)))
}

derive_ady <- function(adt, rfstdtc) {
  rfstdt <- safe_date(rfstdtc)
  adt <- as.Date(adt)
  
  ifelse(
    is.na(adt) | is.na(rfstdt),
    NA_integer_,
    ifelse(adt >= rfstdt, as.integer(adt - rfstdt + 1), as.integer(adt - rfstdt))
  )
}

#===============================================================================
# 1) LOAD SDTM DATASETS
#===============================================================================

dm <- read_sdtm("DM")
sv <- read_sdtm("SV")
mo <- read_sdtm("MO")

qs_domains <- c(
  "QSUC", "QSMS", "QSPG", "QSEA", "QSEQ", "QSIB",
  "QSS2", "QSTS", "QSCR", "QSNS", "QSPI", "QSWP"
)

qs_list <- lapply(qs_domains, read_sdtm)
names(qs_list) <- qs_domains
qs_list <- qs_list[!sapply(qs_list, is.null)]

#===============================================================================
# 2) DISCOVERY - FIND MAYO-RELATED PARAMETERS
#===============================================================================

log_msg("Starting Mayo parameter discovery...")

discover_qs <- rbindlist(
  lapply(names(qs_list), function(dom) {
    df <- qs_list[[dom]]
    
    if (!all(c("QSTESTCD", "QSTEST") %in% names(df))) return(NULL)
    
    df %>%
      distinct(QSTESTCD, QSTEST, QSCAT) %>%
      mutate(
        SOURCE_DOMAIN = dom,
        SEARCH_TEXT = str_to_upper(paste(QSTESTCD, QSTEST, QSCAT))
      ) %>%
      filter(str_detect(
        SEARCH_TEXT,
        "MAYO|STOOL|RECTAL|BLEED|BLEEDING|ENDOSCOP|PHYSICIAN|GLOBAL|ULCERATIVE|COLITIS|UC|SCORE"
      ))
  }),
  fill = TRUE
)

discover_mo <- NULL

if (!is.null(mo) && all(c("MOTESTCD", "MOTEST") %in% names(mo))) {
  discover_mo <- mo %>%
    distinct(MOTESTCD, MOTEST, MOCAT, MOLOC) %>%
    mutate(
      SOURCE_DOMAIN = "MO",
      SEARCH_TEXT = str_to_upper(paste(MOTESTCD, MOTEST, MOCAT, MOLOC))
    ) %>%
    filter(str_detect(
      SEARCH_TEXT,
      "MAYO|ENDOSCOP|ULCER|ULCERATION|MUCOSA|MUCOSAL|ERYTHEMA|FRIABILITY|BLEED|COLON|RECTUM|SCORE"
    ))
}

discovery_all <- bind_rows(discover_qs, discover_mo)

discovery_path <- file.path(CONFIG$output_dir, "admayo_figaro_parameter_discovery.csv")
fwrite(discovery_all, discovery_path)

log_msg("Discovery exported to: ", discovery_path)

#===============================================================================
# 3) MAP RAW SDTM PARAMETERS TO ADMAYO PARAMCD
#===============================================================================
# NOTE:
# This is a rule-based mapping using text patterns.
# After running discovery, you should review admayo_figaro_parameter_discovery.csv
# and adjust this mapping if needed.

map_mayo_param <- function(testcd, test, cat = "", source_domain = "") {
  
  txt <- str_to_upper(paste(testcd, test, cat, source_domain))
  
  case_when(
    str_detect(txt, "STOOL|FREQUENCY") ~ "SFSCORE",
    str_detect(txt, "RECTAL|BLEED") ~ "RBSCORE",
    str_detect(txt, "PHYSICIAN|GLOBAL") ~ "PGSCORE",
    str_detect(txt, "ENDOSCOP|ENDOSCOPY|MUCOSAL|ULCER|FRIABILITY|ERYTHEMA") ~ "ENSCORE",
    str_detect(txt, "MAYO") & str_detect(txt, "PARTIAL") ~ "PMAYO",
    str_detect(txt, "MAYO") & str_detect(txt, "MODIFIED") ~ "MMAYO",
    str_detect(txt, "MAYO") ~ "MAYO",
    TRUE ~ NA_character_
  )
}

param_label <- function(paramcd) {
  case_when(
    paramcd == "SFSCORE" ~ "Stool Frequency Score",
    paramcd == "RBSCORE" ~ "Rectal Bleeding Score",
    paramcd == "ENSCORE" ~ "Endoscopic Score",
    paramcd == "PGSCORE" ~ "Physician Global Assessment Score",
    paramcd == "PMAYO"   ~ "Partial Mayo Score",
    paramcd == "MMAYO"   ~ "Modified Mayo Score",
    paramcd == "MAYO"    ~ "Total Mayo Score",
    TRUE ~ paramcd
  )
}

param_num <- function(paramcd) {
  case_when(
    paramcd == "SFSCORE" ~ 1,
    paramcd == "RBSCORE" ~ 2,
    paramcd == "ENSCORE" ~ 3,
    paramcd == "PGSCORE" ~ 4,
    paramcd == "PMAYO"   ~ 5,
    paramcd == "MMAYO"   ~ 6,
    paramcd == "MAYO"    ~ 7,
    TRUE ~ 99
  )
}

#===============================================================================
# 4) STANDARDIZE QS DOMAINS TO ADaM-LIKE BDS STRUCTURE
#===============================================================================

standardize_qs <- function(df, dom) {
  
  required <- c("STUDYID", "USUBJID", "QSSEQ", "QSTESTCD", "QSTEST",
                "QSSTRESC", "QSSTRESN", "QSDTC", "QSDY", "VISITNUM")
  
  missing_req <- setdiff(required, names(df))
  if (length(missing_req) > 0) {
    log_msg("Skipping ", dom, " due to missing columns: ", paste(missing_req, collapse = ", "))
    return(NULL)
  }
  
  if (!"VISIT" %in% names(df)) df$VISIT <- NA_character_
  if (!"QSBLFL" %in% names(df)) df$QSBLFL <- NA_character_
  if (!"QSCAT" %in% names(df)) df$QSCAT <- NA_character_
  
  out <- df %>%
    mutate(
      SOURCE_DOMAIN = dom,
      PARAMCD = map_mayo_param(QSTESTCD, QSTEST, QSCAT, dom),
      PARAM = param_label(PARAMCD),
      PARAMN = param_num(PARAMCD),
      AVAL = suppressWarnings(as.numeric(QSSTRESN)),
      AVALC = as.character(QSSTRESC),
      ADT = safe_date(QSDTC),
      ADY = suppressWarnings(as.integer(QSDY)),
      AVISITN = suppressWarnings(as.numeric(VISITNUM)),
      AVISIT = as.character(VISIT),
      ABLFL = ifelse(QSBLFL == "Y", "Y", NA_character_),
      SRCDOM = dom,
      SRCSEQ = suppressWarnings(as.integer(QSSEQ)),
      SRCVAR = ifelse(!is.na(AVAL), "QSSTRESN", "QSSTRESC")
    ) %>%
    filter(!is.na(PARAMCD)) %>%
    select(
      STUDYID, USUBJID, AVISIT, AVISITN, ADT, ADY,
      PARAMCD, PARAM, PARAMN, AVAL, AVALC,
      ABLFL, SRCDOM, SRCSEQ, SRCVAR,
      VISIT, VISITNUM
    )
  
  out
}

qs_adam <- rbindlist(
  lapply(names(qs_list), function(dom) standardize_qs(qs_list[[dom]], dom)),
  fill = TRUE
)

log_msg("QS-derived Mayo records: ", nrow(qs_adam))

#===============================================================================
# 5) STANDARDIZE MO DOMAIN TO ADaM-LIKE STRUCTURE
#===============================================================================

mo_adam <- NULL

if (!is.null(mo) && all(c("MOTESTCD", "MOTEST", "MOSTRESC", "MOSTRESN", "MODTC", "MODY", "MOSEQ") %in% names(mo))) {
  
  if (!"MOCAT" %in% names(mo)) mo$MOCAT <- NA_character_
  if (!"MOLOC" %in% names(mo)) mo$MOLOC <- NA_character_
  if (!"MOBLFL" %in% names(mo)) mo$MOBLFL <- NA_character_
  if (!"VISIT" %in% names(mo)) mo$VISIT <- NA_character_
  
  mo_adam <- mo %>%
    mutate(
      PARAMCD = map_mayo_param(MOTESTCD, MOTEST, paste(MOCAT, MOLOC), "MO"),
      PARAM = param_label(PARAMCD),
      PARAMN = param_num(PARAMCD),
      AVAL = suppressWarnings(as.numeric(MOSTRESN)),
      AVALC = as.character(MOSTRESC),
      ADT = safe_date(MODTC),
      ADY = suppressWarnings(as.integer(MODY)),
      AVISITN = suppressWarnings(as.numeric(VISITNUM)),
      AVISIT = as.character(VISIT),
      ABLFL = ifelse(MOBLFL == "Y", "Y", NA_character_),
      SRCDOM = "MO",
      SRCSEQ = suppressWarnings(as.integer(MOSEQ)),
      SRCVAR = ifelse(!is.na(AVAL), "MOSTRESN", "MOSTRESC")
    ) %>%
    filter(!is.na(PARAMCD)) %>%
    select(
      STUDYID, USUBJID, AVISIT, AVISITN, ADT, ADY,
      PARAMCD, PARAM, PARAMN, AVAL, AVALC,
      ABLFL, SRCDOM, SRCSEQ, SRCVAR,
      VISIT, VISITNUM
    )
  
  log_msg("MO-derived Mayo records: ", nrow(mo_adam))
}

#===============================================================================
# 6) COMBINE DIRECT COMPONENTS
#===============================================================================

admayo_base <- rbindlist(list(qs_adam, mo_adam), fill = TRUE) %>%
  distinct()

# Fill AVISIT using SV if missing
if (!is.null(sv) && all(c("USUBJID", "VISITNUM", "VISIT") %in% names(sv))) {
  
  sv_lookup <- sv %>%
    distinct(USUBJID, VISITNUM, VISIT) %>%
    mutate(VISITNUM = suppressWarnings(as.numeric(VISITNUM)))
  
  admayo_base <- admayo_base %>%
    left_join(
      sv_lookup,
      by = c("USUBJID", "VISITNUM"),
      suffix = c("", "_SV")
    ) %>%
    mutate(
      VISIT = coalesce(VISIT, VISIT_SV),
      AVISIT = coalesce(AVISIT, VISIT)
    ) %>%
    select(-VISIT_SV)
}

#===============================================================================
# 7) DERIVE COMPOSITE MAYO SCORES
#===============================================================================
# PMAYO = SFSCORE + RBSCORE + PGSCORE
# MMAYO = SFSCORE + RBSCORE + ENSCORE
# MAYO  = SFSCORE + RBSCORE + ENSCORE + PGSCORE
#
# Review these formulas against SAP/specification if available.

component_wide <- admayo_base %>%
  filter(PARAMCD %in% c("SFSCORE", "RBSCORE", "ENSCORE", "PGSCORE")) %>%
  group_by(STUDYID, USUBJID, AVISIT, AVISITN, ADT, ADY, VISIT, VISITNUM, PARAMCD) %>%
  summarise(AVAL = first(na.omit(AVAL)), .groups = "drop") %>%
  tidyr::pivot_wider(names_from = PARAMCD, values_from = AVAL)

derive_composite <- function(df, paramcd, components) {
  
  missing_components <- setdiff(components, names(df))
  if (length(missing_components) > 0) {
    for (x in missing_components) df[[x]] <- NA_real_
  }
  
  df %>%
    mutate(
      PARAMCD = paramcd,
      PARAM = param_label(paramcd),
      PARAMN = param_num(paramcd),
      AVAL = rowSums(across(all_of(components)), na.rm = FALSE),
      AVALC = as.character(AVAL),
      ABLFL = NA_character_,
      SRCDOM = "DERIVED",
      SRCSEQ = NA_integer_,
      SRCVAR = paste(components, collapse = "+")
    ) %>%
    filter(!is.na(AVAL)) %>%
    select(
      STUDYID, USUBJID, AVISIT, AVISITN, ADT, ADY,
      PARAMCD, PARAM, PARAMN, AVAL, AVALC,
      ABLFL, SRCDOM, SRCSEQ, SRCVAR,
      VISIT, VISITNUM
    )
}

derived_pmayo <- derive_composite(component_wide, "PMAYO", c("SFSCORE", "RBSCORE", "PGSCORE"))
derived_mmayo <- derive_composite(component_wide, "MMAYO", c("SFSCORE", "RBSCORE", "ENSCORE"))
derived_mayo  <- derive_composite(component_wide, "MAYO",  c("SFSCORE", "RBSCORE", "ENSCORE", "PGSCORE"))

admayo_all <- rbindlist(
  list(admayo_base, derived_pmayo, derived_mmayo, derived_mayo),
  fill = TRUE
) %>%
  distinct()

#===============================================================================
# 8) ADD BASELINE, CHG, PCHG, POST-BASELINE FLAG
#===============================================================================

baseline <- admayo_all %>%
  filter(ABLFL == "Y") %>%
  group_by(STUDYID, USUBJID, PARAMCD) %>%
  summarise(BASE = first(na.omit(AVAL)), .groups = "drop")

admayo_all <- admayo_all %>%
  left_join(baseline, by = c("STUDYID", "USUBJID", "PARAMCD")) %>%
  mutate(
    CHG = ifelse(!is.na(AVAL) & !is.na(BASE), AVAL - BASE, NA_real_),
    PCHG = ifelse(!is.na(CHG) & !is.na(BASE) & BASE != 0, 100 * CHG / BASE, NA_real_),
    APOBLFL = ifelse(is.na(ABLFL) & !is.na(BASE) & !is.na(ADT), "Y", NA_character_),
    DTYPE = ifelse(SRCDOM == "DERIVED", "DERIVED", NA_character_)
  )

#===============================================================================
# 9) ADD DM SUBJECT-LEVEL VARIABLES
#===============================================================================

if (!is.null(dm)) {
  
  dm_keep <- dm %>%
    select(any_of(c(
      "STUDYID", "USUBJID", "SUBJID", "SITEID",
      "AGE", "AGEU", "SEX", "RACE", "ETHNIC", "COUNTRY",
      "ARM", "ARMCD", "ACTARM", "ACTARMCD", "RFSTDTC"
    ))) %>%
    distinct(STUDYID, USUBJID, .keep_all = TRUE)
  
  admayo_all <- admayo_all %>%
    left_join(dm_keep, by = c("STUDYID", "USUBJID"))
  
  if ("RFSTDTC" %in% names(admayo_all)) {
    admayo_all <- admayo_all %>%
      mutate(
        ADY = ifelse(is.na(ADY), derive_ady(ADT, RFSTDTC), ADY)
      )
  }
}

#===============================================================================
# 10) CREATE ANALYSIS FLAGS
#===============================================================================

admayo_final <- admayo_all %>%
  mutate(
    ANL01FL = ifelse(!is.na(AVAL), "Y", NA_character_),
    ANL02FL = ifelse(!is.na(BASE) & !is.na(AVAL), "Y", NA_character_),
    ANL03FL = ifelse(PARAMCD %in% c("PMAYO", "MMAYO", "MAYO"), "Y", NA_character_),
    ANL04FL = ifelse(SRCDOM != "DERIVED", "Y", NA_character_),
    ANL05FL = ifelse(SRCDOM == "DERIVED", "Y", NA_character_),
    ASEQ = row_number()
  ) %>%
  arrange(USUBJID, PARAMN, AVISITN, ADT, PARAMCD) %>%
  select(
    STUDYID, USUBJID,
    AVISIT, AVISITN, ADT, ADY,
    PARAMCD, PARAM, PARAMN,
    AVAL, AVALC,
    BASE, CHG, PCHG,
    DTYPE, ABLFL, APOBLFL,
    ANL01FL, ANL02FL, ANL03FL, ANL04FL, ANL05FL,
    SRCSEQ, SRCVAR, SRCDOM,
    VISITNUM, VISIT,
    ASEQ,
    any_of(c(
      "AGE", "AGEU", "SEX", "RACE", "ETHNIC", "COUNTRY",
      "ARM", "ARMCD", "ACTARM", "ACTARMCD",
      "SITEID", "SUBJID"
    ))
  )

#===============================================================================
# 11) EXPORT
#===============================================================================

output_path <- file.path(CONFIG$output_dir, CONFIG$output_file)

fwrite(admayo_final, output_path)

log_msg("ADMAYO-like dataset created successfully.")
log_msg("Output: ", output_path)
log_msg("Rows: ", nrow(admayo_final))
log_msg("Subjects: ", length(unique(admayo_final$USUBJID)))
log_msg("PARAMCDs: ", paste(unique(admayo_final$PARAMCD), collapse = ", "))

#===============================================================================
# 12) QC SUMMARY
#===============================================================================

qc_param <- admayo_final %>%
  group_by(PARAMCD, PARAM, SRCDOM) %>%
  summarise(
    n_records = n(),
    n_subjects = n_distinct(USUBJID),
    n_nonmissing_aval = sum(!is.na(AVAL)),
    .groups = "drop"
  ) %>%
  arrange(PARAMCD, SRCDOM)

qc_path <- file.path(CONFIG$output_dir, "admayo_figaro_qc_summary.csv")
fwrite(qc_param, qc_path)

log_msg("QC summary exported to: ", qc_path)
