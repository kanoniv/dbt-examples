# Approach 1: Pure dbt + SQL

Identity resolution built entirely in SQL using dbt models. This is how most data teams attempt the problem before reaching for specialized tooling.

## How It Works

The pipeline has 4 layers, each built as dbt models:

1. **Staging** (12 models) - Clean raw CSVs into consistent column names and types
2. **Normalization** (5 models) - Standardize emails, phones, names, companies, addresses
3. **Cross-matching** (8 models) - Blocking keys, candidate pairs, pair scoring, transitive closure, golden records
4. **Marts** (6 models) - Dimensional models for analytics: `dim_customers`, `fct_customer_activity`, `rpt_entity_360`

```
models/
  staging/          12 models - raw to clean
  intermediate/     25 models - normalize, deduplicate, match, resolve
  marts/             6 models - analytics-ready tables
```

## Running

Requires dbt and a PostgreSQL (or Snowflake) warehouse with the seed data loaded.

```bash
cd dbt-sql/
dbt seed --profiles-dir .     # Load CSVs into warehouse
dbt run --profiles-dir .      # Run all 41 models
```

## What It Gets Right

- **Familiar tooling** - SQL and dbt are tools every data team already knows. No new dependencies, no Python scripts, no Rust binaries. Everything runs in your existing warehouse.
- **Full visibility** - Every intermediate step is a table you can query. Want to see why two records matched? Check `int_xmatch__pair_scores`. Want to audit transitive closure? Query `int_xmatch__entity_graph`. There are no black boxes.
- **dbt ecosystem** - Tests, documentation, lineage graphs, incremental models, CI/CD - all the dbt tooling works out of the box. You can add `dbt-expectations` tests, track freshness, and slot this into your existing DAG.
- **Warehouse-native** - Runs where your data already lives. No data movement, no extract-load-transform round trips. If you're on Snowflake, it scales with your warehouse size.

## Challenges

- **2,800 lines of SQL** - The matching logic alone (blocking, scoring, clustering) is ~1,500 lines across 8 intermediate models. Adding a new source means writing 5-6 new models (staging, normalization, dedup, attribute mapping).
- **Hand-tuned weights** - Match scores use hardcoded weights (`email_match * 0.4 + name_match * 0.3 + ...`). There's no statistical learning - getting these right requires trial and error, and they silently degrade as data distributions change.
- **Soundex-only fuzzy matching** - SQL doesn't have Jaro-Winkler built in. Soundex is the practical limit, which misses obvious matches like "Smith" vs "Smth" or "Katherine" vs "Catherine".
- **Fragile transitive closure** - The 3-pass SQL approach (`int_xmatch__transitive_closure.sql`) converges for most cases but can miss chains longer than 3 hops. A true graph algorithm guarantees correctness; iterative SQL does not.
- **Manual survivorship** - Golden record assembly is 120 lines of window functions with hardcoded source priority. Changing "prefer CRM for email but Billing for company" means rewriting SQL, not editing a config.
- **No governance** - No built-in freshness checks, schema validation, PII tagging, or audit logging. You build all of that yourself or go without.
- **Slow iteration** - Every change to matching logic requires a full `dbt run`. On large datasets, this means minutes to hours before you see results. No interactive feedback loop.

## File Count

| Layer | Models | Lines |
|-------|--------|-------|
| Staging | 12 | ~350 |
| Normalization | 5 | ~500 |
| Dedup | 5 | ~250 |
| Cross-matching | 8 | ~850 |
| Resolution | 7 | ~350 |
| Marts | 6 | ~350 |
| **Total** | **41 + 4 YAML** | **~2,800** |
