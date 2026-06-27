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
        {{ norm_txt("split_part(marca_modelo, '/', 1)") }}          as prefixo_norm,
        {{ norm_txt("substring(marca_modelo from position('/' in marca_modelo) + 1)") }} as resto_norm
    from fonte
),

marca_modelo_resolvido as (
    select
        *,
        -- se o prefixo for de importado, marca = 1o token do resto
        case
            when prefixo_norm in ('I', 'IMP', 'IMPORT')
            then split_part(resto_norm, ' ', 1)
            else prefixo_norm
        end as marca_bruta,
        case
            when prefixo_norm in ('I', 'IMP', 'IMPORT')
            then nullif(substring(resto_norm from position(' ' in resto_norm) + 1), resto_norm)
            else resto_norm
        end as modelo_norm
    from split_mm
),

com_marca as (
    select
        r.*,
        dp.marca_padrao
    from marca_modelo_resolvido r
    left join {{ ref('de_para_marca') }} dp
        on r.marca_bruta = {{ norm_txt('dp.marca_bruta') }}
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
        when ano_fabricacao ~ '^[0-9]+$'
         and ano_fabricacao::int between 1900 and 2027
        then ano_fabricacao::int
    end                                             as ano,
    -- qtd vem como " 7.0" (com espaco e .0)
    round(nullif(trim(qtd_veiculos), '')::numeric)::bigint as qtd_veiculos,
    mes_referencia
from com_marca
