# animation

> # ===============================
> # RUN
> # ===============================
> run_pipeline()
[INFO] === UNIFI ARD PIPELINE (load-1901) INICIADO ===
[INFO] Study: CNTO1275UCO3001-UNIFI / load-1901
[INFO] Data path: /domino/datasets/local/clinical-trial-data/CNTO1275UCO3001-UNIFI/load-1901/data
[INFO] Lendo arquivos CSV via data.table::fread
|--------------------------------------------------|
|==================================================|
|--------------------------------------------------|
|==================================================|
|--------------------------------------------------|
|==================================================|
[INFO] Carregados 27 datasets
[INFO] [adbdc] Tall sem AVISITN — sem pseudo-visita
[INFO] [adcm] Tall sem AVISITN — sem pseudo-visita
[INFO] Usando admayo2 como âncora principal da spine [load-1901]
[INFO] Complementando spine com: adchem
[INFO] Complementando spine com: adhema
[INFO] Complementando spine com: adlbef
[INFO] Complementando spine com: adsaf
[INFO] Complementando spine com: adeq5d
[INFO] Complementando spine com: adibdq
[INFO] Complementando spine com: adsf36
[INFO] Complementando spine com: adecon
[INFO] Complementando spine com: aduceis
[INFO] Complementando spine com: advscort
[INFO] Complementando spine com: adis
[INFO] Complementando spine com: adpc
[INFO] Spine criada com 53627 linhas e 1331 sujeitos únicos
[INFO] Processando dataset: adae
[INFO] [adae] Wide → extract_wide_dataset()
[INFO] Processando dataset: adbdc
[INFO] [adbdc] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adbdc] Pivot tall→wide: 130 colunas PARAMCD geradas
[INFO] Processando dataset: adchem
[INFO] [adchem] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adchem] Pivot tall→wide: 38 colunas PARAMCD geradas
[INFO] Processando dataset: adcm
[INFO] [adcm] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adcm] Pivot tall→wide: 25 colunas PARAMCD geradas
[INFO] Processando dataset: adcort
[INFO] [adcort] Evento multi-linha → pivot_wide_by_seq()
[INFO] Processando dataset: addeath
[INFO] [addeath] Wide → extract_wide_dataset()
[INFO] Processando dataset: adds
[INFO] [adds] Wide → extract_wide_dataset()
[INFO] Processando dataset: addv
[INFO] [addv] Wide → extract_wide_dataset()
[INFO] Processando dataset: adecon
[INFO] [adecon] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adecon] Pivot tall→wide: 15 colunas PARAMCD geradas
[INFO] Processando dataset: adeq5d
[INFO] [adeq5d] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adeq5d] Pivot tall→wide: 21 colunas PARAMCD geradas
[INFO] Processando dataset: adex
[INFO] [adex] Wide → extract_wide_dataset()
[INFO] Processando dataset: adhema
[INFO] [adhema] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adhema] Pivot tall→wide: 82 colunas PARAMCD geradas
[INFO] Processando dataset: adho
[INFO] [adho] Evento multi-linha → pivot_wide_by_seq()
[INFO] Processando dataset: adibdq
[INFO] [adibdq] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adibdq] Pivot tall→wide: 38 colunas PARAMCD geradas
[INFO] Processando dataset: adis
[INFO] [adis] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adis] Pivot tall→wide: 20 colunas PARAMCD geradas
[INFO] Processando dataset: adlbef
[INFO] [adlbef] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adlbef] Pivot tall→wide: 60 colunas PARAMCD geradas
[INFO] Processando dataset: admalig
[INFO] [admalig] Wide → extract_wide_dataset()
[INFO] Processando dataset: admayo2
[INFO] [admayo2] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [admayo2] Pivot tall→wide: 18 colunas PARAMCD geradas
[INFO] Processando dataset: adpc
[INFO] [adpc] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adpc] Pivot tall→wide: 2 colunas PARAMCD geradas
[INFO] Processando dataset: adsaf
[INFO] [adsaf] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adsaf] Pivot tall→wide: 10 colunas PARAMCD geradas
[INFO] Processando dataset: adsf36
[INFO] [adsf36] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [adsf36] Pivot tall→wide: 60 colunas PARAMCD geradas
[INFO] Processando dataset: adsg
[INFO] [adsg] Evento multi-linha → pivot_wide_by_seq()
[INFO] Processando dataset: adsl
[INFO] [adsl] Wide → extract_wide_dataset()
[INFO] Processando dataset: adtfi
[INFO] [adtfi] Wide → extract_wide_dataset()
[INFO] Processando dataset: adtfm
[INFO] [adtfm] Wide → extract_wide_dataset()
[INFO] Processando dataset: aduceis
[INFO] [aduceis] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [aduceis] Pivot tall→wide: 14 colunas PARAMCD geradas
[INFO] Processando dataset: advscort
[INFO] [advscort] Tall/BDS → pivot_wide_by_paramcd()
[INFO] [advscort] Pivot tall→wide: 7 colunas PARAMCD geradas
[INFO] ARD final: 53627 linhas × 1989 colunas
[INFO] ARD salvo em: /mnt/unifi_load1901_ml_dataset.csv
Error: Mapped vectors must have consistent lengths:
* `.x` has length 24
* `.y` has length 27
In addition: There were 14 warnings (use warnings() to see them)
