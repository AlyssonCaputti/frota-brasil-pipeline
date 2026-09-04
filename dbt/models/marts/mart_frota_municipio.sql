{{
  config(
    materialized='incremental',
    unique_key='mes_referencia',
    incremental_strategy='delete+insert',
  )
}}

-- Frota de carros no grao mais fino: marca / modelo_base / ano / uf / municipio.
-- Este e o entregavel "Planilha 1" da versao antiga, agora como tabela.
--
-- Incremental por mes: 3 anos sao ~700M linhas, reconstruir tudo pra somar um
-- mes nao se paga. unique_key no mes, nao no grao - ver macro filtro_meses.

select
    marca_padrao    as marca,
    modelo_base,
    ano,
    uf,
    municipio,
    sum(qtd_veiculos) as qtd_veiculos,
    mes_referencia
from {{ ref('int_frota_carros') }}
where 1 = 1
    {{ filtro_meses() }}
group by 1, 2, 3, 4, 5, 7
