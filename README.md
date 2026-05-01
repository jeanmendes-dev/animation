# animation

> # ==============================================================================
> # ADaM Multi-Dataset Integration Pipeline
> # Granularity: USUBJID + AVISIT + AVISITN
> # ==============================================================================
> 
> library(tidyverse)
> library(haven)
> 
> # ==============================================================================
> # CONFIG
> # ==============================================================================
> 
> CONFIG <- list(
+   input_dir  = "/domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1901/data",
+   output_dir = "/mnt",
+   output_dataset = "analysis-ready-dataset.csv",
+   output_rds = "analysis-ready-dataset.rds",
+   coverage_report = "coverage_report.csv"
+ )
> 
> KEYS <- c("USUBJID", "AVISIT", "AVISITN")
> 
> # ==============================================================================
> # HELPERS
> # ==============================================================================
> 
> log_msg <- function(...) {
+   cat(sprintf("[%s] %s\n", Sys.time(), paste0(..., collapse = "")))
+ }
> 
> clean_dataset_name <- function(path) {
+   tools::file_path_sans_ext(basename(path)) %>%
+     toupper() %>%
+     str_replace_all("[^A-Z0-9_]", "_")
+ }
> 
> read_any_dataset <- function(path) {
+   
+   ext <- tolower(tools::file_ext(path))
+   
+   if (ext == "sas7bdat") {
+     
+     df <- haven::read_sas(path)
+     
+   } else if (ext == "csv") {
+     
+     # USAR fread (mais robusto para Domino + arquivos grandes)
+     df <- data.table::fread(path, sep = "auto", data.table = FALSE)
+     
+   } else {
+     stop("Unsupported file type: ", path)
+   }
+   
+   # Padronização CDISC
+   df %>%
+     rename_with(~ toupper(.x))
+ }
> 
> ensure_keys <- function(df) {
+   if (!"USUBJID" %in% names(df)) {
+     stop("Dataset does not contain USUBJID.")
+   }
+   
+   if (!"AVISIT" %in% names(df)) {
+     df <- df %>% mutate(AVISIT = "PSEUDO")
+   }
+   
+   if (!"AVISITN" %in% names(df)) {
+     df <- df %>% mutate(AVISITN = "PSEUDO")
+   }
+   
+   df %>%
+     mutate(
+       USUBJID = as.character(USUBJID),
+       AVISIT  = as.character(AVISIT),
+       AVISITN = as.character(AVISITN)
+     )
+ }
> 
> prefix_non_key_columns <- function(df, dataset_name) {
+   non_key_cols <- setdiff(names(df), KEYS)
+   
+   df %>%
+     rename_with(
+       .fn = ~ paste0(dataset_name, "_", .x),
+       .cols = all_of(non_key_cols)
+     )
+ }
> 
> collapse_duplicate_keys <- function(df) {
+   df %>%
+     group_by(across(all_of(KEYS))) %>%
+     summarise(
+       across(
+         everything(),
+         ~ {
+           values <- unique(na.omit(as.character(.x)))
+           if (length(values) == 0) NA_character_
+           else paste(values, collapse = " | ")
+         }
+       ),
+       .groups = "drop"
+     )
+ }
> 
> process_dataset <- function(path) {
+   
+   dataset_name <- clean_dataset_name(path)
+   
+   log_msg("Processing dataset: ", dataset_name)
+   
+   raw_df <- read_any_dataset(path)
+   original_cols <- names(raw_df)
+   original_n_cols <- length(original_cols)
+   
+   df <- raw_df %>%
+     ensure_keys()
+   
+   has_paramcd <- "PARAMCD" %in% names(df)
+   has_aval  <- "AVAL"  %in% names(df)
+   has_avalc <- "AVALC" %in% names(df)
+   
+   paramcd_values <- character(0)
+   
+   has_any_value <- has_aval | has_avalc
+   
+   if (has_paramcd && has_any_value) {
+     
+     paramcd_values <- df_safe %>%
+       filter(!is.na(PARAMCD) & PARAMCD != "") %>%
+       distinct(PARAMCD) %>%
+       pull(PARAMCD) %>%
+       as.character() %>%
+       sort()
+     
+     # --------------------------------------------------------------------------
+     # PARAMCD tall -> wide using AVAL
+     # Example: ADCDAI_PARAMCD_ACDAI_AVAL
+     # --------------------------------------------------------------------------
+     
+     df_safe <- df %>%
+       mutate(
+         AVAL  = if ("AVAL" %in% names(.)) as.character(AVAL) else NA_character_,
+         AVALC = if ("AVALC" %in% names(.)) as.character(AVALC) else NA_character_
+       )
+     
+     paramcd_values <- df_safe %>%
+       filter(!is.na(PARAMCD)) %>%
+       distinct(PARAMCD) %>%
+       pull(PARAMCD) %>%
+       as.character() %>%
+       sort()
+     
+     pivot_df <- df_safe %>%
+       select(all_of(KEYS), PARAMCD, AVAL, AVALC) %>%
+       filter(!is.na(PARAMCD)) %>%
+       mutate(
+         PARAMCD = as.character(PARAMCD),
+         
+         PARAM_VALUE = coalesce(AVAL, AVALC),
+         
+         VALUE_SOURCE = case_when(
+           !is.na(AVAL)  ~ "AVAL",
+           !is.na(AVALC) ~ "AVALC",
+           TRUE ~ "VALUE"
+         ),
+         
+         pivot_name = paste0(dataset_name, "_PARAMCD_", PARAMCD, "_", VALUE_SOURCE)
+       ) %>%
+       select(all_of(KEYS), pivot_name, PARAM_VALUE) %>%
+       group_by(across(all_of(KEYS)), pivot_name) %>%
+       summarise(
+         PARAM_VALUE = {
+           values <- unique(na.omit(PARAM_VALUE))
+           if (length(values) == 0) NA_character_
+           else paste(values, collapse = " | ")
+         },
+         .groups = "drop"
+       ) %>%
+       pivot_wider(
+         names_from = pivot_name,
+         values_from = PARAM_VALUE
+       )
+     
+     # --------------------------------------------------------------------------
+     # Keep all other columns except PARAMCD and AVAL after pivot
+     # PARAMCD is transformed into wide columns.
+     # AVAL is represented by PARAMCD-specific columns.
+     # --------------------------------------------------------------------------
+     
+     other_df <- df %>%
+       select(-PARAMCD, -any_of(c("AVAL", "AVALC"))) %>%
+       prefix_non_key_columns(dataset_name) %>%
+       collapse_duplicate_keys()
+     
+     final_df <- full_join(other_df, pivot_df, by = KEYS)
+     
+   } else {
+     
+     # --------------------------------------------------------------------------
+     # Datasets without PARAMCD: keep all variables with dataset prefix
+     # --------------------------------------------------------------------------
+     
+     final_df <- df %>%
+       prefix_non_key_columns(dataset_name) %>%
+       collapse_duplicate_keys()
+   }
+   
+   extracted_cols <- setdiff(names(final_df), KEYS)
+   extracted_n_cols <- length(extracted_cols)
+   
+   has_aval  <- "AVAL"  %in% names(df)
+   has_avalc <- "AVALC" %in% names(df)
+   has_any_value <- has_aval | has_avalc
+   
+   coverage <- tibble(
+     dataset = dataset_name,
+     original_total_columns = original_n_cols,
+     extracted_total_columns = extracted_n_cols,
+     coverage_percent = round((extracted_n_cols / original_n_cols) * 100, 2),
+     
+     has_paramcd = has_paramcd,
+     
+     # NOVO (mais completo)
+     has_aval  = has_aval,
+     has_avalc = has_avalc,
+     has_any_value = has_any_value,
+     
+     n_paramcd_extracted = length(paramcd_values),
+     paramcd_extracted = paste(paramcd_values, collapse = ", "),
+     
+     is_100_percent_coverage = coverage_percent >= 100
+   )
+   
+   list(
+     data = final_df,
+     coverage = coverage
+   )
+ }
> 
> # ==============================================================================
> # EXECUTION
> # ==============================================================================
> 
> log_msg("Starting ADaM integration pipeline")
[2026-05-01 18:29:00] Starting ADaM integration pipeline
> 
> files <- list.files(
+   CONFIG$input_dir,
+   pattern = "\\.(sas7bdat|csv)$",
+   full.names = TRUE,
+   ignore.case = TRUE
+ )
> 
> if (length(files) == 0) {
+   stop("No SAS7BDAT or CSV files found in: ", CONFIG$input_dir)
+ }
> 
> log_msg("Files found: ", length(files))
[2026-05-01 18:29:00] Files found: 27
> 
> processed <- map(files, safely(process_dataset))
[2026-05-01 18:29:00] Processing dataset: ADAE_SAS7BDAT
[2026-05-01 18:29:02] Processing dataset: ADBDC_SAS7BDAT
[2026-05-01 18:29:02] Processing dataset: ADCHEM_SAS7BDAT
[2026-05-01 18:29:03] Processing dataset: ADCM_SAS7BDAT
[2026-05-01 18:29:03] Processing dataset: ADCORT_SAS7BDAT
|--------------------------------------------------|
|==================================================|
[2026-05-01 18:29:09] Processing dataset: ADDEATH_SAS7BDAT
[2026-05-01 18:29:09] Processing dataset: ADDS_SAS7BDAT
[2026-05-01 18:29:10] Processing dataset: ADDV_SAS7BDAT
[2026-05-01 18:29:10] Processing dataset: ADECON_SAS7BDAT
[2026-05-01 18:29:11] Processing dataset: ADEQ5D_SAS7BDAT
[2026-05-01 18:29:11] Processing dataset: ADEX_SAS7BDAT
[2026-05-01 18:29:30] Processing dataset: ADHEMA_SAS7BDAT
[2026-05-01 18:29:32] Processing dataset: ADHO_SAS7BDAT
[2026-05-01 18:29:32] Processing dataset: ADIBDQ_SAS7BDAT
[2026-05-01 18:29:32] Processing dataset: ADIS_SAS7BDAT
[2026-05-01 18:29:32] Processing dataset: ADLBEF_SAS7BDAT
[2026-05-01 18:29:34] Processing dataset: ADMALIG_SAS7BDAT
[2026-05-01 18:29:34] Processing dataset: ADMAYO2_SAS7BDAT
|--------------------------------------------------|
|==================================================|
[2026-05-01 18:29:35] Processing dataset: ADPC_SAS7BDAT
[2026-05-01 18:29:35] Processing dataset: ADSAF_SAS7BDAT
[2026-05-01 18:29:36] Processing dataset: ADSF36_SAS7BDAT
[2026-05-01 18:29:36] Processing dataset: ADSG_SAS7BDAT
[2026-05-01 18:29:37] Processing dataset: ADSL_SAS7BDAT
[2026-05-01 18:29:40] Processing dataset: ADTFI_SAS7BDAT
[2026-05-01 18:29:40] Processing dataset: ADTFM_SAS7BDAT
[2026-05-01 18:29:40] Processing dataset: ADUCEIS_SAS7BDAT
[2026-05-01 18:29:40] Processing dataset: ADVSCORT_SAS7BDAT
|--------------------------------------------------|
|==================================================|
> 
> successful <- processed %>%
+   keep(~ is.null(.x$error)) %>%
+   map("result")
> 
> failed <- processed %>%
+   keep(~ !is.null(.x$error))
> 
> if (length(failed) > 0) {
+   log_msg("WARNING: Some datasets failed to process:")
+   walk(failed, ~ log_msg(.x$error$message))
+ }
[2026-05-01 18:29:42] WARNING: Some datasets failed to process:
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
[2026-05-01 18:29:42] object 'df_safe' not found
> 
> if (length(successful) == 0) {
+   stop("No datasets were successfully processed.")
+ }
> 
> final_dataset <- successful %>%
+   map("data") %>%
+   reduce(full_join, by = KEYS) %>%
+   arrange(USUBJID, AVISITN, AVISIT)
> 
> coverage_report <- successful %>%
+   map("coverage") %>%
+   bind_rows()
> 
> # ==============================================================================
> # OUTPUT
> # ==============================================================================
> 
> dir.create(CONFIG$output_dir, recursive = TRUE, showWarnings = FALSE)
> 
> write_csv(
+   final_dataset,
+   file.path(CONFIG$output_dir, CONFIG$output_dataset),
+   na = ""
+ )
> 
> saveRDS(
+   final_dataset,
+   file.path(CONFIG$output_dir, CONFIG$output_rds)
+ )
> 
> write_csv(
+   coverage_report,
+   file.path(CONFIG$output_dir, CONFIG$coverage_report),
+   na = ""
+ )
> 
> log_msg("Final dataset saved to: ", file.path(CONFIG$output_dir, CONFIG$output_dataset))
[2026-05-01 18:29:47] Final dataset saved to: /mnt/analysis-ready-dataset.csv
> log_msg("Final RDS saved to: ", file.path(CONFIG$output_dir, CONFIG$output_rds))
[2026-05-01 18:29:47] Final RDS saved to: /mnt/analysis-ready-dataset.rds
> log_msg("Coverage report saved to: ", file.path(CONFIG$output_dir, CONFIG$coverage_report))
[2026-05-01 18:29:47] Coverage report saved to: /mnt/coverage_report.csv
> 
> log_msg("Final dataset dimensions: ", nrow(final_dataset), " rows x ", ncol(final_dataset), " columns")
[2026-05-01 18:29:47] Final dataset dimensions: 19902 rows x 819 columns
> 
> print(coverage_report)
# A tibble: 12 x 11
   dataset  original_total_… extracted_total… coverage_percent has_paramcd has_aval has_avalc has_any_value
   <chr>               <int>            <int>            <dbl> <lgl>       <lgl>    <lgl>     <lgl>        
 1 ADAE_SA…              123              122             99.2 FALSE       FALSE    FALSE     FALSE        
 2 ADCORT_…               42               41             97.6 FALSE       FALSE    FALSE     FALSE        
 3 ADDEATH…               56               55             98.2 FALSE       FALSE    FALSE     FALSE        
 4 ADDS_SA…               72               71             98.6 FALSE       FALSE    FALSE     FALSE        
 5 ADDV_SA…               56               55             98.2 FALSE       FALSE    FALSE     FALSE        
 6 ADEX_SA…               91               88             96.7 FALSE       FALSE    FALSE     FALSE        
 7 ADHO_SA…               52               51             98.1 FALSE       FALSE    FALSE     FALSE        
 8 ADMALIG…               90               89             98.9 FALSE       FALSE    FALSE     FALSE        
 9 ADSG_SA…               52               51             98.1 FALSE       FALSE    FALSE     FALSE        
10 ADSL_SA…              122              121             99.2 FALSE       FALSE    FALSE     FALSE        
11 ADTFI_S…               45               42             93.3 FALSE       FALSE    FALSE     FALSE  
