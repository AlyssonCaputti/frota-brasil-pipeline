{{ config(materialized='table') }}

-- Frota de carros no grao mais fino: marca / modelo_base / ano / uf / municipio.
-- Este e o entregavel "Planilha 1" da versao antiga, agora como tabela.

select
    marca_padrao    as marca,
    modelo_base,
    ano,
    uf,
    municipio,
    sum(qtd_veiculos) as qtd_veiculos,
    mes_referencia
from {{ ref('int_frota_carros') }}
group by 1, 2, 3, 4, 5, 7
