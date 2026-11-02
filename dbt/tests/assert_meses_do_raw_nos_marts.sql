-- Todo mês carregado no raw tem que chegar aos marts. Pega mart vazio (o 13/13
-- trivial do BigQuery Sandbox) e mês que o incremental pulou.
select distinct r.mes_referencia
from {{ source('raw', 'frota_municipio') }} r
where r.mes_referencia not in (select distinct mes_referencia from {{ ref('mart_consolidada') }})
