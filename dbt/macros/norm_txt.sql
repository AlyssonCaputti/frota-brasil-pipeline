{#
  Normalizacao de texto - equivalente SQL do norm_txt() que era em python:
  caixa alta, remove acentos, troca -./ por espaco, tira o resto que nao
  for A-Z0-9, colapsa espacos.

  usa unaccent (create extension unaccent). se nao tiver a extension o
  translate cobre os acentos mais comuns do portugues - deixei o fallback
  comentado embaixo caso o ambiente nao deixe criar extension.
#}
{% macro norm_txt(col) %}
  regexp_replace(
    regexp_replace(
      upper(unaccent({{ col }})),
      '[^A-Z0-9]+', ' ', 'g'
    ),
    '^ +| +$', '', 'g'
  )
{% endmacro %}


{% macro model_base(col) %}
  {#
    modelo-base: 1o token canonico do modelo, com as correcoes de nome
    composto (CR-V -> CRV etc) que fazem o match SENATRAN<->FIPE bater.
    prefixos compostos (GRAND SIENA, NEW CIVIC) mantem 2 tokens.
  #}
  {% set corrigido %}
    regexp_replace(
      regexp_replace({{ norm_txt(col) }}, 'CR V|HR V|WR V|ZR V', 'CRV', 'g'),
      'T CROSS|HB 20', 'TCROSS', 'g'
    )
  {% endset %}
  case
    when split_part({{ corrigido }}, ' ', 1) in ('GRAND', 'NEW', 'GREAT', 'PT')
     and split_part({{ corrigido }}, ' ', 2) !~ '^[0-9]'
     and length(split_part({{ corrigido }}, ' ', 2)) > 2
    then split_part({{ corrigido }}, ' ', 1) || ' ' || split_part({{ corrigido }}, ' ', 2)
    else split_part({{ corrigido }}, ' ', 1)
  end
{% endmacro %}
