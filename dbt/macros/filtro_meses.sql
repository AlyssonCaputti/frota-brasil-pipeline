{#
  Filtro incremental por mes_referencia, usado pelos dois marts.

  Cada mes e uma particao logica, igual no raw: troco o mes inteiro
  (delete+insert com unique_key) em vez de comparar linha a linha - mesmo
  contrato do load_frota.py, que apaga so o mes que esta entrando.

  Comparo sempre com `this`, nunca com a fonte: se a consolidada olhasse o
  mart_frota_municipio, um mes que ja entrou na fonte e ainda nao nela seria
  pulado pra sempre.

  Em run full (1a vez ou --full-refresh) o filtro sai vazio e processa tudo.
  Pra refazer um mes so: dbt run --vars '{meses: ["maio_2026"]}'
#}
{% macro filtro_meses(col='mes_referencia') %}
  {%- set meses_var = var('meses', none) -%}
  {%- if meses_var -%}
    {#- lista explicita ganha de tudo -#}
    and {{ col }} in (
      {%- for m in meses_var %}'{{ m }}'{% if not loop.last %}, {% endif %}{% endfor -%}
    )
  {%- elif is_incremental() -%}
    and {{ col }} not in (
      select distinct mes_referencia from {{ this }}
    )
  {%- endif -%}
{% endmacro %}
