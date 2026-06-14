# Sample data

Committed so the pipeline runs without the full 1.2 GB download.

## `senatran_frota_sample.csv`

~75k rows sampled from the real April/2026 SENATRAN municipality dump
(1-in-N row sampling across the file, so all 27 UFs show up). Same schema and
same messiness as the real thing — including a few `Sem Informação` rows in the
UF/year columns, which the pipeline has to handle.

## `fipe_catalogo.json`

Cached response of the FIPE brand/model/version catalog (the real API output).
Used as the offline source for the FIPE extractor when `USE_SAMPLE=1`.
