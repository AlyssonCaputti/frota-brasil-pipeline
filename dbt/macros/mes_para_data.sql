{#
  "abril_2026" -> 2026-04-01. O BigQuery só particiona por DATE, então derivo
  mes_data em vez de retipar mes_referencia, que é chave em toda a linhagem.
#}
{% macro mes_para_data(col='mes_referencia') %}
  {%- set meses = ['janeiro', 'fevereiro', 'marco', 'abril', 'maio', 'junho', 'julho',
                   'agosto', 'setembro', 'outubro', 'novembro', 'dezembro'] -%}
  cast(
    {{ dbt.split_part(col, "'_'", 2) }} || '-' ||
    case {{ dbt.split_part(col, "'_'", 1) }}
      {%- for m in meses %}
      when '{{ m }}' then '{{ "%02d" % loop.index }}'
      {%- endfor %}
    end || '-01'
  as date)
{% endmacro %}
