{{ config(materialized='view') }}

/*
  Limpeza da frota SENATRAN. "Marca Modelo" é texto livre do cadastro:
    - separo marca e modelo no 1o "/"
    - "I/TOYOTA COROLLA": I, IMP e IMPORT marcam importado; a marca é o 1o token do resto
    - VW, VOLKS e VOLKSWAGEN viram uma marca só pela seed de_para_marca
  Linhas com marca ou ano inválido ficam aqui e saem no intermediate.
*/

with fonte as (
    select * from {{ source('raw', 'frota_municipio') }}
),

separado as (
    select
        *,
        {{ norm_txt(_split_part('marca_modelo', '/', 1)) }} as prefixo,
        {{ norm_txt(_depois_de('marca_modelo', '/')) }}     as resto
    from fonte
),

resolvido as (
    select
        *,
        case
            when prefixo in ('I', 'IMP', 'IMPORT') then {{ _split_part('resto', ' ', 1) }}
            else prefixo
        end as marca_bruta,
        case
            when prefixo in ('I', 'IMP', 'IMPORT') then nullif({{ _depois_de('resto', ' ') }}, resto)
            else resto
        end as modelo_norm
    from separado
),

-- distinct porque chaves diferentes da seed colidem depois de normalizar
-- (M.BENZ e M BENZ); sem isso o join duplica a linha da frota
de_para as (
    select distinct
        {{ norm_txt('marca_bruta') }} as marca_bruta_norm,
        marca_padrao
    from {{ ref('de_para_marca') }}
)

select
    r.uf,
    r.municipio,
    r.marca_modelo                                  as marca_modelo_raw,
    r.marca_bruta,
    dp.marca_padrao,
    r.modelo_norm,
    {{ model_base('r.modelo_norm') }}               as modelo_base,
    case
        when {{ _regexp_match('r.ano_fabricacao', '^[0-9]+$') }}
         and cast(r.ano_fabricacao as {{ dbt.type_bigint() }}) between 1900 and {{ ano_max() }}
        then cast(r.ano_fabricacao as {{ dbt.type_bigint() }})
    end                                             as ano,
    -- vem como " 7.0"
    cast(round(cast(nullif(trim(r.qtd_veiculos), '') as {{ dbt.type_numeric() }}))
         as {{ dbt.type_bigint() }})                as qtd_veiculos,
    r.mes_referencia
from resolvido r
left join de_para dp
    on r.marca_bruta = dp.marca_bruta_norm
