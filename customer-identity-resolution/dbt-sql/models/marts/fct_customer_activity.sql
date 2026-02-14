-- Mart: Customer Activity Fact
-- Unified activity timeline per resolved customer entity
-- Combines support tickets, app events, invoices, and partner interactions

with entity_graph as (
    select * from {{ ref('int_xmatch__entity_graph') }}
),

-- Support activity
support_activity as (
    select
        eg.resolved_entity_id as customer_id,
        'support_ticket' as activity_type,
        t.ticket_id as activity_id,
        t.subject as activity_detail,
        t.priority,
        t.ticket_status as status,
        t.created_at as activity_at
    from entity_graph eg
    join {{ ref('stg_support__tickets') }} t
        on eg.source_id = t.support_user_id
        and eg.source_system = 'support'
),

-- App activity
app_activity as (
    select
        eg.resolved_entity_id as customer_id,
        'app_event' as activity_type,
        e.event_id as activity_id,
        e.event_type as activity_detail,
        null as priority,
        null as status,
        e.event_at as activity_at
    from entity_graph eg
    join {{ ref('stg_app__events') }} e
        on eg.source_id = e.app_user_id
        and eg.source_system = 'app'
),

-- Billing activity
billing_activity as (
    select
        eg.resolved_entity_id as customer_id,
        'invoice' as activity_type,
        i.invoice_id as activity_id,
        'Invoice: ' || i.amount_dollars || ' ' || i.currency as activity_detail,
        null as priority,
        i.invoice_status as status,
        i.issued_at as activity_at
    from entity_graph eg
    join {{ ref('stg_billing__invoices') }} i
        on eg.source_id = i.billing_account_id
        and eg.source_system = 'billing'
)

select * from support_activity
union all select * from app_activity
union all select * from billing_activity
