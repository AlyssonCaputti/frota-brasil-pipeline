{#
  Quais meses um mart incremental processa. Cada mês é uma partição: troco o
  mês inteiro (unique_key = mes_referencia), o mesmo contrato do load_frota.

  - --vars '{meses: ["maio_2026"]}': reprocessa só esses (é o que a DAG usa)
  - incremental sem vars: só os meses que ainda não estão no próprio mart
  - primeira carga: tudo
#}
{% macro filtro_meses(col='mes_referencia') %}
  {%- set meses = var('meses', none) -%}
  {%- if meses -%}
    and {{ col }} in ({% for m in meses %}'{{ m }}'{{ ", " if not loop.last }}{% endfor %})
  {%- elif is_incremental() -%}
    and {{ col }} not in (select distinct mes_referencia from {{ this }})
  {%- endif -%}
{% endmacro %}
