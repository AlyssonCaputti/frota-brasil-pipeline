{#
  Config comum dos marts: incremental por mês. No BigQuery vira partição mensal
  em mes_data + cluster por marca/modelo_base; no Postgres, delete+insert.

  protegido=true ignora --full-refresh, a não ser que venha junto
  --vars '{rebuild_historico: true}'.
#}
{% macro config_mart_mensal(protegido=false) %}
  {{ return({
      'full_refresh': false if protegido and not var('rebuild_historico', false) else none,
      'materialized': 'incremental',
      'unique_key': 'mes_referencia',
      'incremental_strategy': 'insert_overwrite' if target.type == 'bigquery' else 'delete+insert',
      'partition_by': {'field': 'mes_data', 'data_type': 'date', 'granularity': 'month'}
                      if target.type == 'bigquery' else none,
      'cluster_by': ['marca', 'modelo_base'] if target.type == 'bigquery' else none,
  }) }}
{% endmacro %}
