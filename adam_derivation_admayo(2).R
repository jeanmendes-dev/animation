#===============================================================================
# FIGARO UC1 - Convert ADMAYO-like to UNIFI JR-like structure
#===============================================================================

library(data.table)
library(dplyr)
library(stringr)

#===============================================================================
# CONFIG
#===============================================================================

input_admayo <- "/domino/datasets/local/clinical-trial-data/SHP647UC301-FIGARO-UC1/data_updated/admayo_figaro_uc1.csv"

sdtm_dir <- "/domino/datasets/local/clinical-trial-data/SHP647UC301-FIGARO-UC1/data_updated/csv"

output_file <- "/domino/datasets/local/clinical-trial-data/SHP647UC301-FIGARO-UC1/data_updated/admayo_figaro_uc1_unifi_like.csv"

#===============================================================================
# HELPERS
#===============================================================================

log_msg <- function(...) {
  cat("[", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "] ", ..., "\n", sep = "")
}

safe_date <- function(x) {
  x <- as.character(x)
  suppressWarnings(as.Date(x, tryFormats = c(
    "%Y-%m-%d",
    "%m/%d/%Y",
    "%d/%m/%Y",
    "%Y-%m-%dT%H:%M:%S"
  )))
}

read_sdtm <- function(domain) {
  
  files <- list.files(sdtm_dir, full.names = TRUE)
  
  file <- files[
    grepl(
      paste0("^", domain, "(\\.|_|$)"),
      basename(files),
      ignore.case = TRUE
    )
  ]
  
  if (length(file) == 0) {
    log_msg("WARNING: File not found for domain: ", domain)
    return(NULL)
  }
  
  file <- file[1]
  log_msg("Reading SDTM: ", basename(file))
  
  df <- fread(file, sep = "auto", na.strings = c("", "NA", "N/A", "."))
  names(df) <- toupper(names(df))
  df
}

#===============================================================================
# 1) READ ADMAYO FIGARO + SV
#===============================================================================

admayo <- fread(input_admayo, sep = "auto", na.strings = c("", "NA", "N/A", "."))
names(admayo) <- toupper(names(admayo))

sv <- read_sdtm("SV")

admayo[, ADT2 := safe_date(ADT)]

#===============================================================================
# 2) FILL AVISIT / AVISITN FROM SV USING DATE WINDOWS
#===============================================================================

if (!is.null(sv) && all(c("USUBJID", "VISITNUM", "VISIT", "SVSTDTC", "SVENDTC") %in% names(sv))) {
  
  log_msg("Filling AVISIT/AVISITN using SV date windows...")
  
  a <- as.data.table(admayo)
  a[, ROWID := .I]
  
  s <- as.data.table(sv)
  s <- s[, .(
    USUBJID,
    SV_VISITNUM = suppressWarnings(as.numeric(VISITNUM)),
    SV_VISIT = as.character(VISIT),
    SV_START = safe_date(SVSTDTC),
    SV_END = safe_date(SVENDTC)
  )]
  
  s <- s[!is.na(SV_START) & !is.na(SV_END)]
  
  a[, ADT_START := ADT2]
  a[, ADT_END := ADT2]
  
  setkey(s, USUBJID, SV_START, SV_END)
  setkey(a, USUBJID, ADT_START, ADT_END)
  
  overlap <- foverlaps(
    a,
    s,
    by.x = c("USUBJID", "ADT_START", "ADT_END"),
    by.y = c("USUBJID", "SV_START", "SV_END"),
    type = "within",
    nomatch = 0L
  )
  
  if (nrow(overlap) > 0) {
    
    fill_visit <- overlap[
      order(ROWID, SV_VISITNUM),
      .SD[1],
      by = ROWID
    ][, .(ROWID, SV_VISITNUM, SV_VISIT)]
    
    a[fill_visit, on = "ROWID", `:=`(
      AVISITN = ifelse(is.na(AVISITN), i.SV_VISITNUM, AVISITN),
      AVISIT  = ifelse(is.na(AVISIT) | AVISIT == "", i.SV_VISIT, AVISIT),
      VISITNUM = ifelse(is.na(VISITNUM), i.SV_VISITNUM, VISITNUM),
      VISIT = ifelse(is.na(VISIT) | VISIT == "", i.SV_VISIT, VISIT)
    )]
  }
  
  # If still missing, use nearest SV visit by date
  still_missing <- a[is.na(AVISITN) & !is.na(ADT2), .(ROWID, USUBJID, ADT2)]
  
  if (nrow(still_missing) > 0) {
    
    nearest <- s[
      still_missing,
      on = "USUBJID",
      allow.cartesian = TRUE
    ][
      ,
      DIFF := abs(as.integer(ADT2 - SV_START))
    ][
      order(ROWID, DIFF),
      .SD[1],
      by = ROWID
    ][
      ,
      .(ROWID, SV_VISITNUM, SV_VISIT)
    ]
    
    a[nearest, on = "ROWID", `:=`(
      AVISITN = ifelse(is.na(AVISITN), i.SV_VISITNUM, AVISITN),
      AVISIT  = ifelse(is.na(AVISIT) | AVISIT == "", i.SV_VISIT, AVISIT),
      VISITNUM = ifelse(is.na(VISITNUM), i.SV_VISITNUM, VISITNUM),
      VISIT = ifelse(is.na(VISIT) | VISIT == "", i.SV_VISIT, VISIT)
    )]
  }
  
  a[, c("ROWID", "ADT_START", "ADT_END") := NULL]
  admayo <- a
}

#===============================================================================
# 3) STANDARDIZE TO UNIFI-LIKE STRUCTURE
#===============================================================================

admayo_unifi_like <- admayo %>%
  mutate(
    # Keep original source domain but standardize SRCDOM like UNIFI
    ORIGDOM = SRCDOM,
    SRCDOM = case_when(
      str_detect(ORIGDOM, "^QS") ~ "QS",
      ORIGDOM == "MO" ~ "MO",
      ORIGDOM == "DERIVED" ~ NA_character_,
      TRUE ~ ORIGDOM
    ),
    
    PARCAT1 = case_when(
      PARAMCD %in% c("SFSCORE", "RBSCORE", "PGSCORE", "PMAYO") ~ "Mayo Daily Diary",
      PARAMCD %in% c("ENSCORE", "MMAYO", "MAYO") ~ "Mayo Endoscopy",
      TRUE ~ "Mayo Score"
    ),
    
    PARAMTYP = ifelse(ORIGDOM == "DERIVED", "DERIVED", NA_character_),
    
    ADTF = NA_character_,
    ADTM = NA_character_,
    AVALCAT1 = NA_character_,
    
    # UNIFI-like source visit variables
    SSVISIT = AVISIT,
    SSVISITN = AVISITN,
    VISITD = VISIT,
    VISNUMD = VISITNUM,
    
    # Approximate source date variables
    QSDTC = ifelse(str_detect(ORIGDOM, "^QS"), as.character(ADT), NA_character_),
    MODTC = ifelse(ORIGDOM == "MO", as.character(ADT), NA_character_),
    
    # Population flags not available in FIGARO SDTM derivation
    RANDFL = NA_character_,
    SAFFL = NA_character_,
    SAFRFL = NA_character_,
    SAFCRFL = NA_character_,
    FASFL = NA_character_,
    FASRFL = NA_character_,
    FASCRFL = NA_character_,
    FASRESFL = NA_character_,
    SUBFL = NA_character_,
    CRSPM0 = NA_character_,
    
    # Treatment fields not fully derivable from current ADMAYO
    TRT01P = ARM,
    TRT01PN = NA_real_,
    TRT01A = ACTARM,
    TRT01AN = NA_real_,
    TRT02P = NA_character_,
    TRT02PN = NA_real_,
    TR02PG1 = NA_character_,
    TR02PG1N = NA_real_,
    TRT02A = NA_character_,
    TRT02AN = NA_real_,
    TR02AG1 = NA_character_,
    TR02AG1N = NA_real_,
    TRT03P = NA_character_,
    TRT03PN = NA_real_,
    TRT03A = NA_character_,
    TRT03AN = NA_real_,
    
    # Criteria columns like UNIFI JR
    CRIT1 = NA_character_,
    CRIT1FL = NA_character_,
    CRIT2 = NA_character_,
    CRIT2FL = NA_character_,
    CRIT3 = NA_character_,
    CRIT3FL = NA_character_,
    CRIT4 = NA_character_,
    CRIT4FL = NA_character_,
    CRIT5 = NA_character_,
    CRIT5FL = NA_character_,
    MCRIT1 = NA_character_,
    MCRIT1ML = NA_character_,
    WK8MISFL = NA_character_,
    WK8DISFL = NA_character_,
    WK44MIFL = NA_character_,
    WK44DIFL = NA_character_,
    ICE = NA_character_,
    BASETYPE = NA_character_,
    SSRFL = NA_character_
  )

#===============================================================================
# 4) FILTER ANALYSIS-READY RECORDS
#===============================================================================
# This makes FIGARO closer to UNIFI:
# - remove records without AVISIT/AVISITN
# - remove records without AVAL
# - keep grain usable for ML: USUBJID + AVISIT + AVISITN + PARAMCD

admayo_unifi_like <- admayo_unifi_like %>%
  filter(
    !is.na(USUBJID),
    !is.na(AVISIT),
    !is.na(AVISITN),
    !is.na(PARAMCD),
    !is.na(AVAL)
  )

#===============================================================================
# 5) ORDER COLUMNS LIKE UNIFI JR ADMAYO
#===============================================================================

unifi_cols <- c(
  "STUDYID", "USUBJID", "SUBJID", "AGE", "AGEU", "SEX", "COUNTRY",
  "SITEID", "ETHNIC", "RACE",
  "TRT01P", "TRT01PN", "TRT01A", "TRT01AN",
  "TRT02P", "TRT02PN", "TR02PG1", "TR02PG1N",
  "TRT02A", "TRT02AN", "TR02AG1", "TR02AG1N",
  "TRT03P", "TRT03PN", "TRT03A", "TRT03AN",
  "RANDFL", "SAFFL", "SAFRFL", "SAFCRFL",
  "FASFL", "FASRFL", "FASCRFL", "FASRESFL",
  "SUBFL", "CRSPM0",
  "PARCAT1", "PARAMCD", "PARAM", "PARAMN", "PARAMTYP",
  "ADT", "ADTF", "ADTM", "ADY",
  "AVISIT", "AVISITN",
  "AVAL", "AVALCAT1", "AVALC", "DTYPE",
  "CRIT1", "CRIT1FL", "CRIT2", "CRIT2FL",
  "CRIT3", "CRIT3FL", "CRIT4", "CRIT4FL",
  "CRIT5", "CRIT5FL", "MCRIT1", "MCRIT1ML",
  "WK8MISFL", "WK8DISFL", "WK44MIFL", "WK44DIFL",
  "ABLFL", "APOBLFL", "ANL01FL", "ICE",
  "BASE", "BASETYPE", "CHG", "PCHG",
  "SSRFL", "SSVISIT", "SSVISITN", "VISITD", "VISNUMD",
  "SRCDOM", "SRCVAR", "SRCSEQ", "QSDTC", "MODTC",
  "ORIGDOM"
)

missing_cols <- setdiff(unifi_cols, names(admayo_unifi_like))

for (col in missing_cols) {
  admayo_unifi_like[[col]] <- NA
}

admayo_unifi_like <- admayo_unifi_like %>%
  select(all_of(unifi_cols)) %>%
  arrange(USUBJID, AVISITN, PARAMN, PARAMCD)

#===============================================================================
# 6) EXPORT
#===============================================================================

fwrite(admayo_unifi_like, output_file, sep = ",")

log_msg("UNIFI-like ADMAYO for FIGARO created.")
log_msg("Output: ", output_file)
log_msg("Rows: ", nrow(admayo_unifi_like))
log_msg("Subjects: ", length(unique(admayo_unifi_like$USUBJID)))
log_msg("Missing AVISIT: ", sum(is.na(admayo_unifi_like$AVISIT)))
log_msg("Missing AVISITN: ", sum(is.na(admayo_unifi_like$AVISITN)))
log_msg("Missing AVAL: ", sum(is.na(admayo_unifi_like$AVAL)))
log_msg("PARAMCDs: ", paste(unique(admayo_unifi_like$PARAMCD), collapse = ", "))