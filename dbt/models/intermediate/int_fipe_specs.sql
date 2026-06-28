{{ config(materialized='view') }}

/*
  Consolida a FIPE em 1 linha por (marca, modelo_base): as varias versoes
  de um modelo viram um resumo. Serve tanto de whitelist de carro quanto de
  fonte das specs no mart.
    - motor / combustivel: string agregada dos valores distintos
    - potencia: mediana (evita puxar por versao esportiva isolada)
*/

with fipe as (
    select * from {{ ref('stg_fipe') }}
)

select
    marca_fipe                                          as marca,
    modelo_base,
    count(*)                                            as n_versoes,
    string_agg(distinct motor, '; ' order by motor)     as motores,
    string_agg(distinct combustivel, '; ' order by combustivel) as combustiveis,
    percentile_cont(0.5) within group (order by potencia_cv) as potencia_cv_mediana
from fipe
where modelo_base <> ''
group by 1, 2
