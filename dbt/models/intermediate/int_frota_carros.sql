{{ config(materialized='view') }}

/*
  O dump não tem tipo de veículo. Fico só com os (marca, modelo_base) que existem
  no catálogo FIPE de carros: moto, caminhão e ônibus caem sozinhos, inclusive a
  moto de quem também faz carro (Honda CG sai, Honda Civic fica).
*/

with whitelist as (
    select distinct marca, modelo_base from {{ ref('int_fipe_specs') }}
)

select f.*
from {{ ref('stg_frota') }} f
inner join whitelist w
    on f.marca_padrao = w.marca
   and f.modelo_base  = w.modelo_base
where f.ano is not null
  and f.qtd_veiculos > 0
