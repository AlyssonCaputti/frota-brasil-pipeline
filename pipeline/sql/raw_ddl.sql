-- Landing: tudo como texto, do jeito que veio. Cast e limpeza ficam no dbt.
create schema if not exists raw;

-- o norm_txt do dbt usa unaccent
create extension if not exists unaccent;

-- sem drop: cada mês é uma partição lógica por mes_referencia
create table if not exists raw.frota_municipio (
    uf              text,
    municipio       text,
    marca_modelo    text,
    ano_fabricacao  text,
    qtd_veiculos    text,
    mes_referencia  text
);

create table if not exists raw.fipe_versoes (
    marca_codigo   text,
    marca_nome     text,
    modelo_codigo  text,
    modelo_nome    text
);
