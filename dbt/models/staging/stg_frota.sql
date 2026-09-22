{{ config(materialized='view') }}

/*
  Staging da frota SENATRAN. Aqui acontece o grosso da limpeza:
    - split de "MARCA/MODELO" no primeiro "/"
    - prefixo de importado (I, IMP, IMPORT) nao e marca: a marca real e o
      1o token do modelo. ex: "I/TOYOTA COROLLA" -> marca TOYOTA, modelo COROLLA
    - de-para de marca (seed) pra unificar VW/VOLKS -> VOLKSWAGEN etc
    - modelo-base pra chave de join com a FIPE
    - cast de ano com validacao (1900..2027), qtd pra numero
  Linhas com UF/ano invalido continuam aqui; sao filtradas no intermediate.
*/

with fonte as (
    select
        uf,
        municipio,
        marca_modelo,
        ano_fabricacao,
        qtd_veiculos,
        mes_referencia
    from {{ source('raw', 'frota_municipio') }}
),

split_mm as (
    select
        *,
        {{ norm_txt(_split_part('marca_modelo', '/', 1)) }}   as prefixo_norm,
        {{ norm_txt(_depois_do_primeiro('marca_modelo', '/')) }} as resto_norm
    from fonte
),

marca_modelo_resolvido as (
    select
        *,
        -- se o prefixo for de importado, marca = 1o token do resto
        case
            when prefixo_norm in ('I', 'IMP', 'IMPORT')
            then {{ _split_part('resto_norm', ' ', 1) }}
            else prefixo_norm
        end as marca_bruta,
        case
            when prefixo_norm in ('I', 'IMP', 'IMPORT')
            then nullif({{ _depois_do_primeiro('resto_norm', ' ') }}, resto_norm)
            else resto_norm
        end as modelo_norm
    from split_mm
),

-- distinct pra proteger contra chaves colidindo na seed depois de normalizar
-- (ex.: "M.BENZ" e "M BENZ" viram os dois "M BENZ"). Sem isso o left join
-- fa fan-out e duplica a linha da frota - so nao aparecia no total porque
-- Mercedes-Benz nao sobrevive ao whitelist da FIPE, mas duplicaria de
-- verdade no dia em que uma marca colidente passar no filtro.
de_para_dedup as (
    select distinct
        {{ norm_txt('marca_bruta') }} as marca_bruta_norm,
        marca_padrao
    from {{ ref('de_para_marca') }}
),

com_marca as (
    select
        r.*,
        dp.marca_padrao
    from marca_modelo_resolvido r
    left join de_para_dedup dp
        on r.marca_bruta = dp.marca_bruta_norm
)

select
    uf,
    municipio,
    marca_modelo                                    as marca_modelo_raw,
    marca_bruta,
    marca_padrao,
    modelo_norm,
    {{ model_base('modelo_norm') }}                 as modelo_base,
    -- ano: so vale numerico entre 1900 e 2027, senao null
    case
        when {{ _regexp_match('ano_fabricacao', '^[0-9]+$') }}
         and cast(ano_fabricacao as {{ dbt.type_bigint() }}) between 1900 and 2027
        then cast(ano_fabricacao as {{ dbt.type_bigint() }})
    end                                             as ano,
    -- qtd vem como " 7.0" (com espaco e .0)
    cast(round(cast(nullif(trim(qtd_veiculos), '') as {{ dbt.type_numeric() }}))
         as {{ dbt.type_bigint() }})                as qtd_veiculos,
    mes_referencia
from com_marca
