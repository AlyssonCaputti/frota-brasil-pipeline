{#
  Normalização de texto e as poucas funções que mudam entre Postgres e BigQuery.
  Cada divergência fica numa macro com um ramo por dialeto; o resto do SQL é comum.
#}

{% macro _sem_acento(col) %}
  {%- if target.type == 'bigquery' -%}
    regexp_replace(normalize(upper({{ col }}), NFD), r'\p{Mn}', '')
  {%- else -%}
    upper(unaccent({{ col }}))
  {%- endif -%}
{% endmacro %}

{# substitui todas as ocorrências; aceita \1 no replacement nos dois dialetos #}
{% macro _regexp_replace_g(col, pattern, replacement) %}
  {%- if target.type == 'bigquery' -%}
    regexp_replace({{ col }}, r'{{ pattern }}', r'{{ replacement }}')
  {%- else -%}
    regexp_replace({{ col }}, '{{ pattern }}', '{{ replacement }}', 'g')
  {%- endif -%}
{% endmacro %}

{% macro _regexp_match(col, pattern) %}
  {%- if target.type == 'bigquery' -%}
    regexp_contains({{ col }}, r'{{ pattern }}')
  {%- else -%}
    {{ col }} ~ '{{ pattern }}'
  {%- endif -%}
{% endmacro %}

{# 1o grupo capturado #}
{% macro _regexp_extract(col, pattern, case_insensitive=false) %}
  {%- if target.type == 'bigquery' -%}
    regexp_extract({{ col }}, r'{{ "(?i)" if case_insensitive }}{{ pattern }}')
  {%- else -%}
    (regexp_match({{ col }}, '{{ pattern }}'{{ ", 'i'" if case_insensitive }}))[1]
  {%- endif -%}
{% endmacro %}

{% macro _split_part(col, delim, n) %}
  {{ dbt.split_part(col, "'" ~ delim ~ "'", n) }}
{% endmacro %}

{# tudo depois da 1a ocorrência de sep, ou a coluna inteira se sep não aparece #}
{% macro _depois_de(col, sep) %}
  case
    when strpos({{ col }}, '{{ sep }}') > 0
    then substr({{ col }}, strpos({{ col }}, '{{ sep }}') + {{ sep | length }})
    else {{ col }}
  end
{% endmacro %}

{# mediana em agregação com GROUP BY; no BigQuery só existe aproximada #}
{% macro _mediana(col) %}
  {%- if target.type == 'bigquery' -%}
    approx_quantiles({{ col }}, 2)[offset(1)]
  {%- else -%}
    percentile_cont(0.5) within group (order by {{ col }})
  {%- endif -%}
{% endmacro %}

{# caixa alta, sem acento, só A-Z0-9 separados por um espaço #}
{% macro norm_txt(col) %}
  {{ _regexp_replace_g(_regexp_replace_g(_sem_acento(col), '[^A-Z0-9]+', ' '), '^ +| +$', '') }}
{% endmacro %}

{#
  Modelo-base: 1o token do nome normalizado, a chave do join SENATRAN x FIPE.
  Junta nomes que a normalização separa (HR-V -> HRV, T-Cross -> TCROSS) e
  mantém dois tokens nos prefixos compostos (GRAND SIENA, NEW CIVIC).
#}
{% macro model_base(col) %}
  {%- set junto -%}
    {{ _regexp_replace_g(_regexp_replace_g(_regexp_replace_g(
         norm_txt(col),
         '([CHWZ]R) V', '\\1V'),
         'T CROSS', 'TCROSS'),
         'HB 20', 'HB20') }}
  {%- endset -%}
  {%- set primeiro = dbt.split_part(junto, "' '", 1) -%}
  {%- set segundo = dbt.split_part(junto, "' '", 2) -%}
  case
    when {{ primeiro }} in ('GRAND', 'NEW', 'GREAT', 'PT')
     and not {{ _regexp_match(segundo, '^[0-9]') }}
     and length({{ segundo }}) > 2
    then {{ primeiro }} || ' ' || {{ segundo }}
    else {{ primeiro }}
  end
{% endmacro %}

{# teto do ano de fabricação, com um ano de folga; sobrescreva com --vars '{ano_max: 2030}' #}
{% macro ano_max() %}
  {{- var('ano_max', modules.datetime.date.today().year + 1) -}}
{% endmacro %}
