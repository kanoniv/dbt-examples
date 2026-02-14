-- Mart: Entity 360° View
-- Complete customer profile joining identity, activity, billing, and support
-- This is the "unified customer view" everyone talks about

-- MANUAL PAIN POINT: This report model joins across every domain.
-- It's the most fragile model in the pipeline. Any upstream change
-- in schema, matching logic, or survivorship rules cascades here.
-- At 40+ models deep, debugging a wrong value is a DAG traversal exercise.

with customers as (
    select * from {{ ref('dim_customers') }}
),

activity as (
    select
        customer_id,
        count(*) as total_activities,
        count(case when activity_type = 'support_ticket' then 1 end) as support_tickets,
        count(case when activity_type = 'app_event' then 1 end) as app_events,
        count(case when activity_type = 'invoice' then 1 end) as invoices,
        min(activity_at) as first_activity_at,
        max(activity_at) as last_activity_at,
        count(case when activity_type = 'support_ticket'
                    and priority in ('high', 'urgent') then 1 end) as high_priority_tickets
    from {{ ref('fct_customer_activity') }}
    group by customer_id
),

lineage as (
    select
        resolved_entity_id,
        count(*) as lineage_fields_tracked
    from {{ ref('int_resolve__lineage_tracker') }}
    group by resolved_entity_id
),

conflicts as (
    select
        resolved_entity_id,
        count(*) as field_conflicts,
        listagg(distinct field_name, ', ') within group (order by field_name) as conflicting_fields
    from {{ ref('int_resolve__conflict_log') }}
    group by resolved_entity_id
)

select
    c.customer_id,
    c.entity_number,
    c.full_name,
    c.email,
    c.phone,
    c.company,
    c.quality_tier,
    c.num_sources,
    c.num_source_records,
    c.sources_present,
    c.mrr_dollars,
    c.plan_name,
    c.billing_status,
    c.product_verified,
    c.last_product_login,
    c.signup_source,
    -- Activity summary
    coalesce(a.total_activities, 0) as total_activities,
    coalesce(a.support_tickets, 0) as support_tickets,
    coalesce(a.app_events, 0) as app_events,
    coalesce(a.invoices, 0) as invoices,
    a.first_activity_at,
    a.last_activity_at,
    coalesce(a.high_priority_tickets, 0) as high_priority_tickets,
    -- Data quality
    coalesce(cf.field_conflicts, 0) as field_conflicts,
    cf.conflicting_fields,
    coalesce(l.lineage_fields_tracked, 0) as lineage_fields_tracked,
    -- Health score: weighted composite
    round(
        (case c.quality_tier when 'gold' then 30 when 'silver' then 20 else 10 end)
        + (case when c.billing_status = 'active' then 20 else 0 end)
        + (case when c.product_verified then 15 else 0 end)
        + least(coalesce(a.app_events, 0), 20)  -- Cap at 20 points
        - (coalesce(a.high_priority_tickets, 0) * 5)  -- Penalty for support issues
        - (coalesce(cf.field_conflicts, 0) * 2)  -- Penalty for data conflicts
    , 1) as customer_health_score,
    c.resolved_at
from customers c
left join activity a on c.customer_id = a.customer_id
left join lineage l on c.customer_id = l.resolved_entity_id
left join conflicts cf on c.customer_id = cf.resolved_entity_id
