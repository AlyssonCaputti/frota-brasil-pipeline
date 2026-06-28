/*
  Teste de negocio: a cobertura de combustivel (ponderada por frota) na
  consolidada tem que ficar >= 70%. Se cair muito e sinal de que o de-para
  de marca ou o model_base quebraram e pararam de casar com a FIPE.

  dbt test passa quando a query NAO retorna linhas -> retorna linha (falha)
  so quando a cobertura fura o piso.
*/
with base as (
    select
        sum(qtd_veiculos)                                          as frota_total,
        sum(qtd_veiculos) filter (where combustiveis is not null)  as frota_com_spec
    from {{ ref('mart_consolidada') }}
)
select
    frota_total,
    frota_com_spec,
    round(100.0 * frota_com_spec / nullif(frota_total, 0), 1) as cobertura_pct
from base
where frota_com_spec::numeric / nullif(frota_total, 0) < 0.70
