-- Mart: Customer Dimension
-- The final resolved customer dimension table
-- Each row = one real-world entity, fully resolved across all sources

-- This is what business teams query. 2,000+ base entities → ~1,500 resolved
-- after dedup and cross-source matching.

with golden as (
    select * from {{ ref('int_xmatch__golden_record') }}
),

metadata as (
    select * from {{ ref('int_resolve__entity_metadata') }}
),

-- Pull in billing data for financial enrichment
billing as (
    select
        eg.resolved_entity_id,
        max(ba.mrr_dollars) as mrr_dollars,
        max(ba.plan_name) as plan_name,
        max(ba.account_status) as billing_status
    from {{ ref('int_xmatch__entity_graph') }} eg
    join {{ ref('stg_billing__accounts') }} ba
        on eg.source_id = ba.billing_account_id
        and eg.source_system = 'billing'
    group by eg.resolved_entity_id
),

-- Pull in app data for product engagement
app_data as (
    select
        eg.resolved_entity_id,
        max(a.is_verified) as is_verified,
        max(a.last_login_at) as last_product_login,
        max(a.signup_source) as signup_source
    from {{ ref('int_xmatch__entity_graph') }} eg
    join {{ ref('stg_app__signups') }} a
        on eg.source_id = a.app_user_id
        and eg.source_system = 'app'
    group by eg.resolved_entity_id
)

select
    g.resolved_entity_id as customer_id,
    g.entity_number,
    g.golden_full_name as full_name,
    g.golden_first_name as first_name,
    g.golden_last_name as last_name,
    g.golden_email as email,
    g.golden_phone as phone,
    g.golden_company as company,
    m.num_sources,
    m.num_source_records,
    m.sources_present,
    m.quality_tier,
    b.mrr_dollars,
    b.plan_name,
    b.billing_status,
    a.is_verified as product_verified,
    a.last_product_login,
    a.signup_source,
    current_timestamp as resolved_at
from golden g
join metadata m on g.resolved_entity_id = m.resolved_entity_id
left join billing b on g.resolved_entity_id = b.resolved_entity_id
left join app_data a on g.resolved_entity_id = a.resolved_entity_id
