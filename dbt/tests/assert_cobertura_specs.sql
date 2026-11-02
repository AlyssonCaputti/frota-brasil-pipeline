-- Cobertura de combustível ponderada pela frota >= 70%, mês a mês. Se cair, o
-- de-para de marca ou o model_base pararam de casar com a FIPE.
with base as (
    select
        mes_referencia,
        sum(qtd_veiculos) as frota_total,
        sum(case when combustiveis is not null then qtd_veiculos else 0 end) as frota_com_spec
    from {{ ref('mart_consolidada') }}
    group by 1
)

select
    mes_referencia,
    frota_total,
    round(100.0 * frota_com_spec / nullif(frota_total, 0), 1) as cobertura_pct
from base
where cast(frota_com_spec as {{ dbt.type_numeric() }}) / nullif(frota_total, 0) < 0.70
