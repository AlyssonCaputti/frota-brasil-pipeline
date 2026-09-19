{#
  mes_referencia e texto ("abril_2026") porque e assim que o SENATRAN nomeia
  o arquivo e e a chave que o load_frota usa pra apagar so o mes que entra.
  Funciona bem no Postgres, mas nao serve de particao no BigQuery, que exige
  DATE/TIMESTAMP - dai esta macro derivar mes_data, o 1o dia do mes.

  Coluna derivada em vez de trocar o tipo de mes_referencia: a chave textual
  atravessa raw -> staging -> marts -> filtro_meses -> testes, e retipar tudo
  isso por causa da particao seria risco sem retorno.

  dbt.split_part e || funcionam nos dois bancos.
#}
{% macro mes_para_data(col='mes_referencia') %}
  cast(
    {{ dbt.split_part(col, "'_'", 2) }} || '-' ||
    case {{ dbt.split_part(col, "'_'", 1) }}
      when 'janeiro'   then '01'
      when 'fevereiro' then '02'
      when 'marco'     then '03'
      when 'abril'     then '04'
      when 'maio'      then '05'
      when 'junho'     then '06'
      when 'julho'     then '07'
      when 'agosto'    then '08'
      when 'setembro'  then '09'
      when 'outubro'   then '10'
      when 'novembro'  then '11'
      when 'dezembro'  then '12'
    end || '-01'
  as date)
{% endmacro %}
