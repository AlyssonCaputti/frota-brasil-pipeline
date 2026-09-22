{#
  Normalizacao de texto - equivalente SQL do norm_txt() que era em python:
  caixa alta, remove acentos, troca -./ por espaco, tira o resto que nao
  for A-Z0-9, colapsa espacos.

  Multi-dialeto: Postgres usa unaccent (create extension unaccent) e
  regexp_replace com flag 'g'. BigQuery nao tem unaccent nem flag de regex
  (regexp_replace la ja substitui tudo por padrao) - o equivalente e
  normalize(str, NFD) + regexp_replace tirando os marks combinantes (\p{Mn}).
#}
{% macro _sem_acento(col) %}
  {%- if target.type == 'bigquery' -%}
    regexp_replace(normalize(upper({{ col }}), NFD), r'\p{Mn}', '')
  {%- else -%}
    upper(unaccent({{ col }}))
  {%- endif -%}
{% endmacro %}

{% macro _regexp_replace_g(col, pattern, replacement) %}
  {%- if target.type == 'bigquery' -%}
    regexp_replace({{ col }}, '{{ pattern }}', '{{ replacement }}')
  {%- else -%}
    regexp_replace({{ col }}, '{{ pattern }}', '{{ replacement }}', 'g')
  {%- endif -%}
{% endmacro %}

{#
  !~ do Postgres (regex nao-match) vira NOT REGEXP_CONTAINS no BigQuery.
  Usado em model_base pra saber se o 2o token comeca com digito.
#}
{% macro _nao_comeca_com_digito(col) %}
  {%- if target.type == 'bigquery' -%}
    not regexp_contains({{ col }}, r'^[0-9]')
  {%- else -%}
    {{ col }} !~ '^[0-9]'
  {%- endif -%}
{% endmacro %}

{#
  ~ do Postgres (regex match) vira REGEXP_CONTAINS no BigQuery.
  Usado na validacao de ano em stg_frota.
#}
{% macro _regexp_match(col, pattern) %}
  {%- if target.type == 'bigquery' -%}
    regexp_contains({{ col }}, r'{{ pattern }}')
  {%- else -%}
    {{ col }} ~ '{{ pattern }}'
  {%- endif -%}
{% endmacro %}

{#
  split_part cru e Postgres-only; BigQuery nao tem. dbt.split_part e
  cross-dialect mas exige o delimitador como literal SQL entre aspas.
#}
{% macro _split_part(col, delim, n) %}
  {{ dbt.split_part(col, "'" ~ delim ~ "'", n) }}
{% endmacro %}

{#
  substring(x from position(y in x) + 1) e sintaxe ANSI que o BigQuery nao
  aceita. substr()/strpos() sao funcoes (nao sintaxe) e existem nos dois
  dialetos com a mesma assinatura.
#}
{% macro _depois_do_primeiro(col, sep) %}
  substr({{ col }}, strpos({{ col }}, '{{ sep }}') + 1)
{% endmacro %}

{#
  Tudo depois da ULTIMA ocorrencia de sep, ou a coluna inteira se sep nao
  aparece. Usado pra tirar o prefixo de sigla da FIPE ("GM - Chevrolet").
  split_part(col, sep, -1) e uma extensao so do Postgres.
#}
{% macro _depois_do_ultimo(col, sep) %}
  case
    when strpos({{ col }}, '{{ sep }}') > 0
    then substr({{ col }}, strpos({{ col }}, '{{ sep }}') + {{ sep|length }})
    else {{ col }}
  end
{% endmacro %}

{#
  1o grupo capturado de uma regex. Postgres devolve array ((...)[1]);
  BigQuery ja devolve o grupo direto com regexp_extract.
#}
{% macro _regexp_extract(col, pattern, case_insensitive=false) %}
  {%- set pat = ('(?i)' ~ pattern) if (case_insensitive and target.type == 'bigquery') else pattern -%}
  {%- if target.type == 'bigquery' -%}
    regexp_extract({{ col }}, r'{{ pat }}')
  {%- else -%}
    (regexp_match({{ col }}, '{{ pattern }}'{{ ", 'i'" if case_insensitive else "" }}))[1]
  {%- endif -%}
{% endmacro %}

{#
  ~* do Postgres (regex match case-insensitive) vira REGEXP_CONTAINS com
  (?i) no BigQuery.
#}
{#
  Mediana agregada com GROUP BY. Postgres tem percentile_cont(0.5) within
  group (order by x), sintaxe que o BigQuery nem reconhece (la percentile_cont
  so existe como window function via OVER()). approx_quantiles(x, 2)[offset(1)]
  e o equivalente aproximado - aceitavel aqui, e so pra evitar que uma versao
  esportiva isolada puxe a potencia_cv_mediana.
#}
{% macro _mediana(col) %}
  {%- if target.type == 'bigquery' -%}
    approx_quantiles({{ col }}, 2)[offset(1)]
  {%- else -%}
    percentile_cont(0.5) within group (order by {{ col }})
  {%- endif -%}
{% endmacro %}

{% macro _regexp_match_i(col, pattern) %}
  {%- if target.type == 'bigquery' -%}
    regexp_contains({{ col }}, r'(?i){{ pattern }}')
  {%- else -%}
    {{ col }} ~* '{{ pattern }}'
  {%- endif -%}
{% endmacro %}

{% macro norm_txt(col) %}
  {{ _regexp_replace_g(
    _regexp_replace_g(_sem_acento(col), '[^A-Z0-9]+', ' '),
    '^ +| +$', ''
  ) }}
{% endmacro %}


{% macro model_base(col) %}
  {#
    modelo-base: 1o token canonico do modelo, com as correcoes de nome
    composto (CR-V -> CRV etc) que fazem o match SENATRAN<->FIPE bater.
    prefixos compostos (GRAND SIENA, NEW CIVIC) mantem 2 tokens.
  #}
  {% set corrigido %}
    {{ _regexp_replace_g(
      _regexp_replace_g(norm_txt(col), 'CR V|HR V|WR V|ZR V', 'CRV'),
      'T CROSS|HB 20', 'TCROSS'
    ) }}
  {% endset %}
  {% set primeiro_token = dbt.split_part(corrigido, "' '", 1) %}
  {% set segundo_token = dbt.split_part(corrigido, "' '", 2) %}
  case
    when {{ primeiro_token }} in ('GRAND', 'NEW', 'GREAT', 'PT')
     and {{ _nao_comeca_com_digito(segundo_token) }}
     and length({{ segundo_token }}) > 2
    then {{ primeiro_token }} || ' ' || {{ segundo_token }}
    else {{ primeiro_token }}
  end
{% endmacro %}
