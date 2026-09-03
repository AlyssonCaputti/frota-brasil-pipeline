-- schema de landing (dados crus, sem transformar)
create schema if not exists raw;

-- frota SENATRAN: carrega tudo como texto de proposito. limpeza/cast
-- fica pro dbt (staging), aqui a gente so pousa o dado do jeito que veio.
-- nao dropa mais: cada mes e uma particao logica por mes_referencia e o
-- load apaga so o mes que esta entrando, senao carregar o ano inteiro
-- deixaria sobrar apenas o ultimo mes.
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
