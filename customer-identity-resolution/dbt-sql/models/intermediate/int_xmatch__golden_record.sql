-- Intermediate: Golden Record Assembly
-- For each resolved entity cluster, pick the "best" value for each field
-- Creates the single source of truth record

-- MANUAL PAIN POINT: Survivorship rules are business-specific and fragile.
-- "Use CRM for names, billing for address, app for engagement data"
-- These priorities change when sources change. Each field has its own logic.

with clusters as (
    select * from {{ ref('int_xmatch__transitive_closure') }}
),

spine as (
    select * from {{ ref('int_unified__contact_spine') }}
),

clustered as (
    select
        c.resolved_entity_id,
        c.entity_number,
        s.*
    from clusters c
    join spine s on c.contact_spine_id = s.contact_spine_id
),

-- Source priority for survivorship
-- CRM > Billing > App > Support > Partners
golden as (
    select
        resolved_entity_id,
        entity_number,

        -- Name: prefer CRM, then billing, then most complete
        first_value(normalized_first_name) over (
            partition by resolved_entity_id
            order by
                case source_system
                    when 'crm' then 1
                    when 'billing' then 2
                    when 'app' then 3
                    when 'support' then 4
                    when 'partners' then 5
                end,
                identity_signal_count desc
            rows between unbounded preceding and unbounded following
        ) as golden_first_name,

        first_value(normalized_last_name) over (
            partition by resolved_entity_id
            order by
                case source_system
                    when 'crm' then 1
                    when 'billing' then 2
                    when 'app' then 3
                    when 'support' then 4
                    when 'partners' then 5
                end,
                identity_signal_count desc
            rows between unbounded preceding and unbounded following
        ) as golden_last_name,

        -- Email: prefer corporate over personal, then CRM priority
        first_value(normalized_email) over (
            partition by resolved_entity_id
            order by
                case email_type when 'corporate' then 0 else 1 end,
                case source_system
                    when 'crm' then 1
                    when 'billing' then 2
                    when 'app' then 3
                    when 'support' then 4
                    when 'partners' then 5
                end
            rows between unbounded preceding and unbounded following
        ) as golden_email,

        -- Phone: prefer CRM, then support (most likely to have phone)
        first_value(normalized_phone) over (
            partition by resolved_entity_id
            order by
                case source_system
                    when 'crm' then 1
                    when 'support' then 2
                    when 'billing' then 3
                    when 'app' then 4
                    when 'partners' then 5
                end,
                case when normalized_phone is not null then 0 else 1 end
            rows between unbounded preceding and unbounded following
        ) as golden_phone,

        -- Company: prefer CRM
        first_value(normalized_company) over (
            partition by resolved_entity_id
            order by
                case source_system
                    when 'crm' then 1
                    when 'billing' then 2
                    when 'partners' then 3
                    when 'support' then 4
                    when 'app' then 5
                end,
                case when normalized_company is not null then 0 else 1 end
            rows between unbounded preceding and unbounded following
        ) as golden_company,

        source_system,
        source_id,
        contact_spine_id,
        row_number() over (partition by resolved_entity_id order by source_system) as rn
    from clustered
)

select distinct
    resolved_entity_id,
    entity_number,
    golden_first_name,
    golden_last_name,
    golden_first_name || ' ' || golden_last_name as golden_full_name,
    golden_email,
    golden_phone,
    golden_company
from golden
where rn = 1
