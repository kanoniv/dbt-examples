# Kanoniv Examples

End-to-end examples comparing approaches to identity resolution with real-world datasets.

## Examples

| Example | Records | Sources | What's Inside |
|---------|---------|---------|---------------|
| [Customer Identity Resolution](./customer-identity-resolution/) | 6,539 | 5 | Same problem solved 3 ways: pure dbt/SQL, Splink, and Kanoniv |

## Customer Identity Resolution

Resolve 6,500 customer records across CRM, Billing, Support, App, and Partner systems. Three approaches compared side by side:

| Approach | What | Lines of Code | Runtime |
|----------|------|---------------|---------|
| [dbt-sql/](./customer-identity-resolution/dbt-sql/) | 5 hand-written SQL models in dbt | 350 | <1s (DuckDB) |
| [splink/](./customer-identity-resolution/splink/) | Splink + DuckDB (Python) | 440 | 2.6s |
| [kanoniv/](./customer-identity-resolution/kanoniv/) | Declarative YAML spec + Rust engine | 170 | 0.4s |

All three use the same [shared dataset](./customer-identity-resolution/data/) of 10 CSV files.

## Links

- [Kanoniv Documentation](https://kanoniv.com/docs)
- [Python SDK on PyPI](https://pypi.org/project/kanoniv/)
- [GitHub](https://github.com/kanoniv/kanoniv)
