# Pipeline internals

## dbt lineage

```
source: raw.frota_municipio ─> stg_frota ──────────────┐
source: raw.fipe_versoes ────> stg_fipe ─> int_fipe_specs ┤
                                                        ├─> int_frota_carros ─> mart_frota_municipio ─> mart_consolidada
                              seed: de_para_marca ──────┘
```

## The normalization rules (why they exist)

The SENATRAN `Marca Modelo` field is messy free text entered at registration:

- `VW/GOL`, `VOLKS/GOL`, `VOLKSWAGEN/GOL` — same car, three spellings.
- `I/TOYOTA COROLLA` — the `I/` prefix means "imported"; the real brand is the
  next token, not `I`.
- `GM/S10`, `CHEV/S10` — GM is Chevrolet in FIPE.
- `M.BENZ`, `MBENZ`, `M BENZ` — all Mercedes-Benz.

`stg_frota` handles all of these: split on the first `/`, detect the import
prefix, then map the brand through the `de_para_marca` seed. `model_base`
reduces `CITY Sedan EX 1.5 Flex` to `CITY` so it joins to FIPE on a stable key.

## The car filter (whitelist)

The municipality dump has no vehicle-type column. Instead of maintaining a
blocklist of motorcycle/truck models, `int_frota_carros` keeps only the
`(brand, model_base)` pairs that exist in the FIPE **car** catalog
(`int_fipe_specs`). Everything else — motorcycles, trucks, buses, trailers —
drops out automatically, including the motorcycles of brands that also make
cars (Honda CG stays out, Honda Civic stays in).

## Tests

- Schema tests on the marts (`not_null`, unique combination, ranges).
- `tests/assert_cobertura_specs.sql`: a business check that fails if the
  fleet-weighted fuel coverage in `mart_consolidada` drops below 70% — an early
  warning that the de-para or `model_base` stopped matching FIPE.
