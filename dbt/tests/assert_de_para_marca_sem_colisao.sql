-- Chaves da seed que colidem depois de normalizar têm que apontar para a mesma
-- marca_padrao. Se apontarem para marcas diferentes, o distinct do stg_frota não
-- resolve e o join volta a duplicar frota.
select
    {{ norm_txt('marca_bruta') }} as marca_bruta_norm,
    count(distinct marca_padrao)  as padroes_distintos
from {{ ref('de_para_marca') }}
group by 1
having count(distinct marca_padrao) > 1
