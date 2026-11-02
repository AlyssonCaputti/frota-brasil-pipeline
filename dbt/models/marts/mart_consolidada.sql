{{ config(**config_mart_mensal()) }}

/*
  Frota nacional por marca / modelo_base / ano / mês com as specs da FIPE.
  left join para não perder frota sem spec: vira null e conta como gap no teste
  de cobertura.

  A FIPE não tem mês, então um mês já materializado guarda as specs de quando
  entrou. Para re-specar o histórico: dbt run --select mart_consolidada --full-refresh
  (lê do mart_frota_municipio, que é protegido contra full refresh).
*/

with frota_nacional as (
    select marca, modelo_base, ano, mes_referencia, sum(qtd_veiculos) as qtd_veiculos
    from {{ ref('mart_frota_municipio') }}
    where 1 = 1
        {{ filtro_meses() }}
    group by 1, 2, 3, 4
)

select
    f.marca,
    f.modelo_base,
    f.ano,
    f.qtd_veiculos,
    s.motores,
    s.combustiveis,
    s.potencia_cv_mediana,
    f.mes_referencia,
    {{ mes_para_data('f.mes_referencia') }} as mes_data
from frota_nacional f
left join {{ ref('int_fipe_specs') }} s
    on f.marca = s.marca
   and f.modelo_base = s.modelo_base
