{{ config(materialized='view') }}

/*
  Filtro de automoveis + comerciais leves. A sacada: manter so os
  (marca, modelo_base) que existem no catalogo FIPE de carros. Isso derruba
  moto/caminhao/onibus/reboque de graca - inclusive as motos das marcas que
  tambem fazem carro (Honda CG vs Honda Civic), porque "CG" nao ta na FIPE
  de carros.

  Tambem filtra as linhas com marca/ano invalidos que sobraram do staging.
*/

with frota as (
    select * from {{ ref('stg_frota') }}
    where marca_padrao is not null
      and ano is not null
      and qtd_veiculos > 0
),

whitelist as (
    select distinct marca, modelo_base
    from {{ ref('int_fipe_specs') }}
)

select f.*
from frota f
inner join whitelist w
    on f.marca_padrao = w.marca
   and f.modelo_base  = w.modelo_base
