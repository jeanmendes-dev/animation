# animation

> # ===============================
> # RUN
> # ===============================
> run_pipeline()
[INFO] === UNIFI ARD PIPELINE (v3) INICIADO ===
[INFO] Study: CNTO1275UCO3001-UNIFI / load-1899
[INFO] Data path: /domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1899/data
[INFO] Lendo arquivos CSV via data.table::fread
[INFO] Carregados 33 datasets
[INFO] Spine criada com 0 linhas e 0 sujeitos únicos
[INFO] Processando dataset: adae.sas7bdat
[INFO] [adae.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adbdc.sas7bdat
[INFO] [adbdc.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adchem.sas7bdat
[INFO] [adchem.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adchem.sas7bdat] Pivot tall→wide: 38 colunas geradas por PARAMCD
[INFO] Processando dataset: adcm.sas7bdat
[INFO] [adcm.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adcort.sas7bdat
[INFO] [adcort.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: addeath.sas7bdat
[INFO] [addeath.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adds.sas7bdat
[INFO] [adds.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: addv.sas7bdat
[INFO] [addv.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adecon.sas7bdat
[INFO] [adecon.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adecon.sas7bdat] Pivot tall→wide: 10 colunas geradas por PARAMCD
[INFO] Processando dataset: adeff.sas7bdat
[INFO] [adeff.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adeq5d.sas7bdat
[INFO] [adeq5d.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adeq5d.sas7bdat] Pivot tall→wide: 14 colunas geradas por PARAMCD
[INFO] Processando dataset: adex.sas7bdat
[INFO] [adex.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adhema.sas7bdat
[INFO] [adhema.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adhema.sas7bdat] Pivot tall→wide: 84 colunas geradas por PARAMCD
[INFO] Processando dataset: adhist.sas7bdat
[INFO] [adhist.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adhist1.sas7bdat
[INFO] [adhist1.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adhist2.sas7bdat
[INFO] [adhist2.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adho.sas7bdat
[INFO] [adho.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adibdq.sas7bdat
[INFO] [adibdq.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adibdq.sas7bdat] Pivot tall→wide: 26 colunas geradas por PARAMCD
[INFO] Processando dataset: adis.sas7bdat
[INFO] [adis.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adis.sas7bdat] Pivot tall→wide: 20 colunas geradas por PARAMCD
[INFO] Processando dataset: adlbef.sas7bdat
[INFO] [adlbef.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adlbef.sas7bdat] Pivot tall→wide: 36 colunas geradas por PARAMCD
[INFO] Processando dataset: admayo.sas7bdat
[INFO] [admayo.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [admayo.sas7bdat] Pivot tall→wide: 90 colunas geradas por PARAMCD
[INFO] Processando dataset: admh.sas7bdat
[INFO] [admh.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adpc.sas7bdat
[INFO] [adpc.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adsaf.sas7bdat
[INFO] [adsaf.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adsaf.sas7bdat] Pivot tall→wide: 9 colunas geradas por PARAMCD
[INFO] Processando dataset: adsf36.sas7bdat
[INFO] [adsf36.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adsf36.sas7bdat] Pivot tall→wide: 44 colunas geradas por PARAMCD
[INFO] Processando dataset: adsg.sas7bdat
[INFO] [adsg.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adsl.sas7bdat
[INFO] [adsl.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adtfi.sas7bdat
[INFO] [adtfi.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adtfm.sas7bdat
[INFO] [adtfm.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: adtte.sas7bdat
[INFO] [adtte.sas7bdat] Wide → extract_wide_dataset()
[INFO] Processando dataset: aduceis.sas7bdat
[INFO] [aduceis.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [aduceis.sas7bdat] Pivot tall→wide: 14 colunas geradas por PARAMCD
[INFO] Processando dataset: advs.sas7bdat
[INFO] [advs.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [advs.sas7bdat] Pivot tall→wide: 7 colunas geradas por PARAMCD
[INFO] Processando dataset: advscort.sas7bdat
[INFO] [advscort.sas7bdat] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [advscort.sas7bdat] Pivot tall→wide: 3 colunas geradas por PARAMCD
Error: Join columns must be present in data.
x Problem with `USUBJID`.
Run `rlang::last_error()` to see where the error occurred.
In addition: There were 32 warnings (use warnings() to see them)
