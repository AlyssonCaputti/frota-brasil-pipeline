{{ config(materialized='table') }}

/*
  Entregavel principal: frota nacional por marca/modelo_base/ano cruzada com
  as specs da FIPE. left join na spec pra nao perder frota quando a spec falta
  (fica null e conta como gap de cobertura - a gente mede isso no teste).
*/

with frota_nacional as (
    select
        marca,
        modelo_base,
        ano,
        sum(qtd_veiculos) as qtd_veiculos,
        mes_referencia
    from {{ ref('mart_frota_municipio') }}
    group by 1, 2, 3, 5
)

select
    f.marca,
    f.modelo_base,
    f.ano,
    f.qtd_veiculos,
    s.motores,
    s.combustiveis,
    s.potencia_cv_mediana,
    f.mes_referencia
from frota_nacional f
left join {{ ref('int_fipe_specs') }} s
    on f.marca = s.marca
   and f.modelo_base = s.modelo_base
