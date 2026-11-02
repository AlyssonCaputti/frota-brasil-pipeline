{{ config(materialized='view') }}

-- Uma linha por (marca, modelo_base): é a whitelist de carros e a fonte das specs.
-- Potência pela mediana, para uma versão esportiva isolada não puxar o número.

select
    marca_fipe                                                   as marca,
    modelo_base,
    count(*)                                                     as n_versoes,
    string_agg(distinct motor, '; ' order by motor)              as motores,
    string_agg(distinct combustivel, '; ' order by combustivel)  as combustiveis,
    {{ _mediana('potencia_cv') }}                                as potencia_cv_mediana
from {{ ref('stg_fipe') }}
where modelo_base <> ''
group by 1, 2
