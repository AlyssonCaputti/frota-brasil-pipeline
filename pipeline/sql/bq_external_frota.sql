-- External table do BigQuery sobre o Parquet no GCS.
--
-- O parquet que o to_parquet.py gera e a camada de origem barata: o BigQuery
-- le direto de la, sem uma copia carregada. Storage no GCS custa bem menos que
-- storage no BigQuery, e o dado bruto so e lido quando o staging roda.
--
-- Nao esta em producao: nao ha projeto GCP neste repo. Rodar com
--   bq query --use_legacy_sql=false < pipeline/sql/bq_external_frota.sql
-- trocando <PROJETO> e <BUCKET>.
--
-- O to_parquet.py grava um arquivo por mes. Para o hive_partitioning abaixo
-- funcionar, os arquivos precisam subir em prefixo com a chave no caminho:
--   gs://<BUCKET>/frota/mes_referencia=abril_2026/frota.parquet
-- Assim o BigQuery poda a particao pelo WHERE sem abrir os arquivos.

create schema if not exists `<PROJETO>.raw`
options (location = 'southamerica-east1');

create or replace external table `<PROJETO>.raw.frota_municipio`
with partition columns (
  mes_referencia string
)
options (
  format = 'PARQUET',
  uris = ['gs://<BUCKET>/frota/*'],
  hive_partition_uri_prefix = 'gs://<BUCKET>/frota',
  require_hive_partition_filter = true
);

-- require_hive_partition_filter = true e de proposito: sem filtro de mes a
-- consulta falha em vez de varrer o historico inteiro calado. Em tabela que
-- cresce um mes por vez, esquecer o WHERE e o erro caro.
