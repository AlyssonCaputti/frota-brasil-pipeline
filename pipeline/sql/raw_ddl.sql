-- schema de landing (dados crus, sem transformar)
create schema if not exists raw;

-- frota SENATRAN: carrega tudo como texto de proposito. limpeza/cast
-- fica pro dbt (staging), aqui a gente so pousa o dado do jeito que veio.
drop table if exists raw.frota_municipio;
create table raw.frota_municipio (
    uf              text,
    municipio       text,
    marca_modelo    text,
    ano_fabricacao  text,
    qtd_veiculos    text,
    mes_referencia  text
);

drop table if exists raw.fipe_versoes;
create table raw.fipe_versoes (
    marca_codigo   text,
    marca_nome     text,
    modelo_codigo  text,
    modelo_nome    text
);
