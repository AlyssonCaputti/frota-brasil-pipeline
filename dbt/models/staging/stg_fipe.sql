{{ config(materialized='view') }}

/*
  A FIPE não tem campo de motor, combustível ou potência: tudo vem no nome da
  versão ("MARRUA 2.8 12V 132cv TDI Diesel"). Extraio por regex.
  modelo_base usa a mesma macro do lado da frota, senão o join não casa.
*/

with fonte as (
    select
        marca_nome,
        modelo_nome,
        -- sem acento e com palavras separadas por espaço: "(Elétrico)" vira "ELETRICO"
        ' ' || {{ norm_txt('modelo_nome') }} || ' ' as nome_norm
    from {{ source('raw', 'fipe_versoes') }}
)

select
    -- "GM - Chevrolet" e "VW - VolksWagen": corto a sigla antes de " - "
    {{ norm_txt(_depois_de('marca_nome', ' - ')) }}  as marca_fipe,
    modelo_nome,
    {{ model_base('modelo_nome') }}                 as modelo_base,
    {{ _regexp_extract('modelo_nome', '([0-9]\.[0-9])') }} as motor,
    cast({{ _regexp_extract('modelo_nome', '([0-9]{2,3})\\s*cv', case_insensitive=true) }}
         as {{ dbt.type_bigint() }})                as potencia_cv,
    case
        when {{ _regexp_match('nome_norm', 'DIESEL|TDI|CRDI|DCI') }}       then 'Diesel'
        when {{ _regexp_match('nome_norm', 'ELETRIC| EV ') }}              then 'Eletrico'
        when {{ _regexp_match('nome_norm', 'HIBRID|HYBRID') }}             then 'Hibrido'
        when {{ _regexp_match('nome_norm', 'FLEX') }}                      then 'Flex'
        when {{ _regexp_match('nome_norm', 'GASOLINA| GAS ') }}            then 'Gasolina'
        when {{ _regexp_match('nome_norm', ' ALCOOL |ETANOL') }}           then 'Alcool'
    end                                             as combustivel
from fonte
