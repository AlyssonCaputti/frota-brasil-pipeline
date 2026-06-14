# Data sources

Reference month: **April 2026** (`abril_2026`).

## 1. SENATRAN — fleet count

Portal: `dados.transportes.gov.br` (CKAN).

- Dataset id: `registro-nacional-de-veiculos-automotores-renavam`
- Resource used: `i_frota_por_uf_municipio_marca_e_modelo_ano_abril_2026.zip`
  - 128 MB zip -> ~1.14 GB TXT
  - UTF-8, `;` separator, ~22.4M rows, 133.8M vehicles
  - columns: `UF | Município | Marca Modelo | Ano Fabricação Veículo CRV | Qtd. Veículos`

To resolve the resource URL programmatically:

```
GET https://dados.transportes.gov.br/api/3/action/package_show?id=registro-nacional-de-veiculos-automotores-renavam
```

### Validation file

`Frota_por_uf_e_tipo_de_veiculo_Abril_2026.xlsx` (fleet by UF and vehicle type)
is used to sanity-check total coverage of the classified light-vehicle fleet.

## 2. FIPE — technical specs

Free API (parallelum): `https://parallelum.com.br/fipe/api/v1/carros`

Gives brand -> model -> version. Engine / fuel / power are parsed out of the
version name string (FIPE does not expose them as separate fields). ~107 brands,
~7.3k versions.

> SENATRAN does not publish technical specs per model; FIPE does not publish
> fleet counts. The whole point of the project is joining the two.

## 3. Wheel / tyre size

`jantes-e-pneus.com/size/{brand}/{model}/{year}/` — OEM wheel/tyre size.
Has anti-bot (403/405/429 on bursts), so coverage is partial. Not part of the
core dbt flow; kept as an optional enrichment step. See `docs/decisions.md`.
