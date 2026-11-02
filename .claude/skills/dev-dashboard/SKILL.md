---
name: dev-dashboard
description: Gera o dashboard de desenvolvimento do frota-brasil-pipeline, uma página publicada como Artifact com o que foi feito na branch (commits, diff), cada problema resolvido com a solução e a verificação, o status real de lint/tipos/testes/dbt e as pendências. Use quando pedirem "dev dashboard", "o que foi feito", "status do projeto" ou "resumo das mudanças".
---

# Dev dashboard

Monte uma página que mostre, sem enfeitar, o que mudou e o que está verificado.

## 1. Coletar

Rode da raiz do repo. Nada aqui escreve no repositório.

- `git rev-parse --abbrev-ref HEAD`, `git log --oneline main..HEAD`
- `git diff --shortstat main...HEAD` e `git status --short` (mudanças não commitadas contam)
- `.claude/skills/dev-dashboard/registro.json`: os itens resolvidos (área, severidade,
  problema, solução, arquivos, verificação). Se um item não estiver no registro mas
  aparecer no diff, inclua e marque como "sem registro".
- Checks, cada um com resultado e contagem:
  - `ruff check .` e `ruff format --check .`
  - `mypy pipeline`
  - `pytest -q` (sem Postgres ou Spark alguns testes pulam: mostre quantos)
  - `cd dbt && dbt parse` e, se houver Postgres no ar, `dbt test`

Se um check não rodar, escreva "não rodado" e o motivo. Nunca preencha um número
que você não viu na saída.

## 2. Montar a página

Carregue a skill `artifact-design` antes de escrever o HTML. Conteúdo, nesta ordem:

1. **Cabeçalho:** branch, data, número de commits e arquivos alterados.
2. **Indicadores:** problemas resolvidos por severidade, testes passando/pulados,
   linhas +/−.
3. **Status dos checks:** um bloco por check (ok / falhou / não rodado).
4. **Mudanças por área** (Dados, Orquestração, Build, Qualidade, BigQuery, Design,
   Segurança, Docs): uma tabela com problema → solução → arquivos → verificação,
   com filtro por área e severidade.
5. **Pendências e riscos:** o que ficou sem verificação real (ex.: itens com
   "não rodado" ou "leitura" na verificação).

Tom: primeira pessoa, técnico e direto, sem citar quem escreveu.

## 3. Publicar

Salve em `<scratchpad>/dev-dashboard.html` e publique com o Artifact tool. Nas
próximas rodadas, republique o mesmo arquivo para manter o link. Ao terminar uma
nova leva de mudanças, acrescente os itens em `registro.json`.
