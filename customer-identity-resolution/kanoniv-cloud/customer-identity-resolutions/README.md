# Kanoniv Cloud + dbt: Customer Identity Resolution

Resolve 6,500 records across 5 source systems into 2,000 golden customers
using the Kanoniv Cloud API.

## How it works

```
dbt run --select staging    # SQL: normalize 5 source tables
python reconcile.py         # Kanoniv Cloud: resolve identities
dbt run --select marts      # SQL: build customer 360 views
```

**Staging** (SQL) - Analytics engineers write the field mappings and normalization:
- `stg_crm_contacts` - direct mapping
- `stg_billing_accounts` - parse "Last, First" account names
- `stg_support_users` - parse "FIRSTNAME LASTNAME" display names
- `stg_app_signups` - direct mapping
- `stg_partner_leads` - direct mapping

**Reconcile** (1 command) - Kanoniv Cloud handles the hard part:
- Uploads staged data to the Kanoniv Cloud API
- Fellegi-Sunter probabilistic matching with EM training
- Email/phone exact matching + fuzzy name/company matching
- Automatic blocking to avoid O(n^2) comparisons
- Golden record assembly with survivorship rules

**Marts** (SQL) - Analytics-ready tables in Snowflake:
- `resolved_customers` - one row per real customer
- `customer_crosswalk` - source_id -> kanoniv_id mapping
- `customer_360` - enriched view with source coverage metrics

## Setup

```bash
pip install kanoniv[cloud] sqlalchemy snowflake-sqlalchemy dbt-snowflake

export SNOWFLAKE_PASSWORD=yourpassword
export KANONIV_API_KEY=kn_your_api_key    # Get one at app.kanoniv.com

dbt deps       # install dbt-kanoniv package
dbt seed       # load source CSVs into Snowflake
dbt run --select staging
python reconcile.py
dbt run --select marts
dbt test
```

## Snowflake queries

```sql
-- How many unique customers?
select count(*) from resolved.resolved_customers;

-- Customers in 3+ systems
select * from resolved.customer_360
where source_count >= 3
order by source_count desc;

-- Join invoices to resolved customers
select
    c.kanoniv_id,
    c.first_name,
    c.last_name,
    sum(i.amount_cents) / 100 as total_invoiced
from raw.billing_invoices i
join resolved.customer_crosswalk x
  on x.source_system = 'billing_accounts'
 and x.source_id = i.billing_account_id
join resolved.resolved_customers c
  on c.kanoniv_id = x.kanoniv_id
group by 1, 2, 3
order by total_invoiced desc
limit 20;
```

## What Kanoniv replaces

The `dbt-sql/` approach in this repo uses 5 SQL models and 350 lines
of hand-written blocking, scoring, clustering, and survivorship logic.

Kanoniv Cloud replaces all of that with:
- 5 staging models (30 lines each)
- 1 YAML spec (190 lines)
- 1 reconcile command

Same results. 6x less code. No probability math in SQL.
