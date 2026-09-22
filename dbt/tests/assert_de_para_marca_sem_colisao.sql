/*
  Teste de negocio: nenhuma chave normalizada da seed de_para_marca pode
  apontar pra mais de um marca_padrao.

  "M.BENZ" e "M BENZ" normalizam pra mesma chave e apontam pro mesmo
  marca_padrao (MERCEDES-BENZ) - isso e so redundancia, o distinct no
  stg_frota resolve. Mas se um dia uma chave colidir apontando pra
  marca_padrao DIFERENTES, o distinct nao resolve mais: o left join volta
  a dar fan-out e duplica frota. Esse teste pega esse caso antes que
  vire numero errado silencioso.
*/
select
    marca_bruta_norm,
    count(distinct marca_padrao) as padroes_distintos,
    string_agg(distinct marca_padrao, ', ') as padroes
from (
    select
        {{ norm_txt('marca_bruta') }} as marca_bruta_norm,
        marca_padrao
    from {{ ref('de_para_marca') }}
) t
group by 1
having count(distinct marca_padrao) > 1
