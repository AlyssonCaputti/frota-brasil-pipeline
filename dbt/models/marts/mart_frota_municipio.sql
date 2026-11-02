-- Protegido contra full refresh: guarda o histórico, e o raw pode já ter sido
-- limpo pelo backfill. Ver config_mart_mensal.
{{ config(**config_mart_mensal(protegido=true)) }}

-- Frota de carros no grão mais fino: marca / modelo_base / ano / UF / município / mês.

select
    marca_padrao      as marca,
    modelo_base,
    ano,
    uf,
    municipio,
    sum(qtd_veiculos) as qtd_veiculos,
    mes_referencia,
    {{ mes_para_data('mes_referencia') }} as mes_data
from {{ ref('int_frota_carros') }}
where 1 = 1
    {{ filtro_meses() }}
group by 1, 2, 3, 4, 5, 7, 8
