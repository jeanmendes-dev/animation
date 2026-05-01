# animation

> # ==============================================================================
> # EXECUTION
> # ==============================================================================
> 
> log_msg("Starting ADaM integration pipeline")
[2026-05-01 17:13:59] Starting ADaM integration pipeline
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
[2026-05-01 17:13:59] Files found: 27
> 
> processed <- map(files, safely(process_dataset))
[2026-05-01 17:13:59] Processing dataset: ADAE_SAS7BDAT
[2026-05-01 17:14:01] Processing dataset: ADBDC_SAS7BDAT
[2026-05-01 17:14:02] Processing dataset: ADCHEM_SAS7BDAT
[2026-05-01 17:14:40] Processing dataset: ADCM_SAS7BDAT
[2026-05-01 17:14:41] Processing dataset: ADCORT_SAS7BDAT
|--------------------------------------------------|
|==================================================|
[2026-05-01 17:14:49] Processing dataset: ADDEATH_SAS7BDAT
[2026-05-01 17:14:49] Processing dataset: ADDS_SAS7BDAT
[2026-05-01 17:14:50] Processing dataset: ADDV_SAS7BDAT
[2026-05-01 17:14:50] Processing dataset: ADECON_SAS7BDAT
[2026-05-01 17:14:52] Processing dataset: ADEQ5D_SAS7BDAT
[2026-05-01 17:14:52] Processing dataset: ADEX_SAS7BDAT
[2026-05-01 17:15:14] Processing dataset: ADHEMA_SAS7BDAT
[2026-05-01 17:15:52] Processing dataset: ADHO_SAS7BDAT
[2026-05-01 17:15:52] Processing dataset: ADIBDQ_SAS7BDAT
[2026-05-01 17:16:07] Processing dataset: ADIS_SAS7BDAT
[2026-05-01 17:16:17] Processing dataset: ADLBEF_SAS7BDAT
[2026-05-01 17:16:57] Processing dataset: ADMALIG_SAS7BDAT
[2026-05-01 17:16:57] Processing dataset: ADMAYO2_SAS7BDAT
|--------------------------------------------------|
|==================================================|
[2026-05-01 17:17:21] Processing dataset: ADPC_SAS7BDAT
[2026-05-01 17:17:36] Processing dataset: ADSAF_SAS7BDAT
[2026-05-01 17:17:36] Processing dataset: ADSF36_SAS7BDAT
[2026-05-01 17:17:58] Processing dataset: ADSG_SAS7BDAT
[2026-05-01 17:17:59] Processing dataset: ADSL_SAS7BDAT
[2026-05-01 17:18:01] Processing dataset: ADTFI_SAS7BDAT
[2026-05-01 17:18:01] Processing dataset: ADTFM_SAS7BDAT
[2026-05-01 17:18:02] Processing dataset: ADUCEIS_SAS7BDAT
[2026-05-01 17:18:05] Processing dataset: ADVSCORT_SAS7BDAT
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
[2026-05-01 17:18:07] WARNING: Some datasets failed to process:
[2026-05-01 17:18:07] Problem with `mutate()` column `PARAM_VALUE`.
ℹ `PARAM_VALUE = case_when(...)`.
x object 'AVALC' not found
[2026-05-01 17:18:07] Problem with `mutate()` column `PARAM_VALUE`.
ℹ `PARAM_VALUE = case_when(...)`.
x object 'AVALC' not found
[2026-05-01 17:18:07] Problem with `mutate()` column `PARAM_VALUE`.
ℹ `PARAM_VALUE = case_when(...)`.
x object 'AVALC' not found
[2026-05-01 17:18:07] Problem with `mutate()` column `PARAM_VALUE`.
ℹ `PARAM_VALUE = case_when(...)`.
x object 'AVALC' not found
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
[2026-05-01 17:18:35] Final dataset saved to: /mnt/analysis-ready-dataset.csv
> log_msg("Final RDS saved to: ", file.path(CONFIG$output_dir, CONFIG$output_rds))
[2026-05-01 17:18:35] Final RDS saved to: /mnt/analysis-ready-dataset.rds
> log_msg("Coverage report saved to: ", file.path(CONFIG$output_dir, CONFIG$coverage_report))
[2026-05-01 17:18:35] Coverage report saved to: /mnt/coverage_report.csv
> 
> log_msg("Final dataset dimensions: ", nrow(final_dataset), " rows x ", ncol(final_dataset), " columns")
[2026-05-01 17:18:35] Final dataset dimensions: 36300 rows x 1753 columns
> 
> print(coverage_report)
# A tibble: 23 x 9
   dataset    original_total_co… extracted_total_co… coverage_percent has_paramcd has_aval n_paramcd_extra…
   <chr>                   <int>               <int>            <dbl> <lgl>       <lgl>               <int>
 1 ADAE_SAS7…                123                 122             99.2 FALSE       FALSE                   0
 2 ADBDC_SAS…                 43                 104            242.  TRUE        TRUE                   65
 3 ADCHEM_SA…                 93                 107            115.  TRUE        TRUE                   19
 4 ADCM_SAS7…                 36                  35             97.2 TRUE        FALSE                   0
 5 ADCORT_SA…                 42                  41             97.6 FALSE       FALSE                   0
 6 ADDEATH_S…                 56                  55             98.2 FALSE       FALSE                   0
 7 ADDS_SAS7…                 72                  71             98.6 FALSE       FALSE                   0
 8 ADDV_SAS7…                 56                  55             98.2 FALSE       FALSE                   0
 9 ADEX_SAS7…                 91                  88             96.7 FALSE       FALSE                   0
10 ADHEMA_SA…                 93                 128            138.  TRUE        TRUE                   41
# … with 13 more rows, and 2 more variables: paramcd_extracted <chr>, is_100_percent_coverage <lgl>
