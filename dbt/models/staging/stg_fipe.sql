{{ config(materialized='view') }}

/*
  Staging FIPE. A FIPE nao expoe motor/combustivel/potencia como campos -
  tudo vem grudado no nome da versao, ex:
    "MARRUA 2.8 12V 132cv TDI Diesel"
  Entao a gente extrai por regex:
    - motor: primeiro padrao "N.N" (cilindrada)
    - potencia: numero antes de "cv"
    - combustivel: heuristica por palavra-chave no nome
  modelo_base usa a MESMA macro do lado da frota, senao o join nao casa.
*/

with fonte as (
    select
        marca_codigo,
        marca_nome,
        modelo_codigo,
        modelo_nome
    from {{ source('raw', 'fipe_versoes') }}
)

select
    -- a FIPE prefixa duas marcas com a sigla antiga: "GM - Chevrolet" e
    -- "VW - VolksWagen". O norm_txt troca o hifen por espaco e sobra
    -- "GM CHEVROLET", que nunca casa com o CHEVROLET que o de-para produz -
    -- e ai VW e Chevrolet, #1 e #3 da frota, caem inteiras na whitelist.
    -- Corta o que vem antes de " - "; Mercedes-Benz e Rolls-Royce tem hifen
    -- colado e nao sao afetadas.
    {{ norm_txt(_depois_do_ultimo('marca_nome', ' - ')) }} as marca_fipe,
    modelo_nome,
    {{ model_base('modelo_nome') }}                 as modelo_base,
    -- cilindrada: primeiro "N.N" do nome
    {{ _regexp_extract('modelo_nome', '([0-9]\.[0-9])') }} as motor,
    -- potencia: numero antes de "cv" (case-insensitive)
    cast({{ _regexp_extract('modelo_nome', '([0-9]{2,3})\\s*cv', case_insensitive=true) }}
         as {{ dbt.type_bigint() }})                as potencia_cv,
    case
        when {{ _regexp_match_i('modelo_nome', 'diesel|tdi|crdi|dci') }}      then 'Diesel'
        when {{ _regexp_match_i('modelo_nome', 'eletric|ev\\b|100%') }}        then 'Eletrico'
        when {{ _regexp_match_i('modelo_nome', 'hybrid|hibrid') }}            then 'Hibrido'
        when {{ _regexp_match_i('modelo_nome', 'flex') }}                     then 'Flex'
        when {{ _regexp_match_i('modelo_nome', 'gasolina|gas\\.') }}          then 'Gasolina'
        when {{ _regexp_match_i('modelo_nome', '\\balcool\\b|etanol') }}      then 'Alcool'
    end                                             as combustivel
from fonte
