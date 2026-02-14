-- Mart: Company Dimension
-- Resolved company entities with aggregated customer counts

with customers as (
    select * from {{ ref('dim_customers') }}
),

crm_companies as (
    select * from {{ ref('stg_crm__companies') }}
),

company_agg as (
    select
        company,
        count(*) as customer_count,
        count(case when billing_status = 'active' then 1 end) as active_customers,
        sum(coalesce(mrr_dollars, 0)) as total_mrr,
        avg(num_sources) as avg_source_coverage,
        max(last_product_login) as latest_product_activity
    from customers
    where company is not null
    group by company
)

select
    {{ dbt_utils.generate_surrogate_key(['ca.company']) }} as company_id,
    ca.company as company_name,
    cc.domain,
    cc.industry,
    cc.employee_count,
    ca.customer_count,
    ca.active_customers,
    ca.total_mrr,
    ca.avg_source_coverage,
    ca.latest_product_activity,
    current_timestamp as resolved_at
from company_agg ca
left join crm_companies cc
    on upper(trim(ca.company)) = upper(trim(cc.company_name))
