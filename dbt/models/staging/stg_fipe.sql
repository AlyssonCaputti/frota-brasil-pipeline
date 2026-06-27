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
    {{ norm_txt('marca_nome') }}                    as marca_fipe,
    modelo_nome,
    {{ model_base('modelo_nome') }}                 as modelo_base,
    -- cilindrada: primeiro "N.N" do nome
    (regexp_match(modelo_nome, '([0-9]\.[0-9])'))[1] as motor,
    -- potencia: numero antes de "cv" (case-insensitive)
    (regexp_match(modelo_nome, '([0-9]{2,3})\s*cv', 'i'))[1]::int as potencia_cv,
    case
        when modelo_nome ~* 'diesel|tdi|crdi|dci'      then 'Diesel'
        when modelo_nome ~* 'eletric|ev\b|100%'        then 'Eletrico'
        when modelo_nome ~* 'hybrid|hibrid'            then 'Hibrido'
        when modelo_nome ~* 'flex'                     then 'Flex'
        when modelo_nome ~* 'gasolina|gas\.'           then 'Gasolina'
        when modelo_nome ~* '\balcool\b|etanol'        then 'Alcool'
    end                                             as combustivel
from fonte
