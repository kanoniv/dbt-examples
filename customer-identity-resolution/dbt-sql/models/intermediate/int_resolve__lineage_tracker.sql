-- Intermediate: Lineage Tracker
-- Full provenance: for each golden record field, which source did it come from?
-- Required for audit trails and debugging survivorship decisions

-- MANUAL PAIN POINT: When a stakeholder asks "where did this email come from?",
-- you need to trace back through survivorship rules → golden record → source.
-- Without explicit lineage tracking, this is a multi-hour investigation.

with entity_graph as (
    select * from {{ ref('int_xmatch__entity_graph') }}
),

golden as (
    select * from {{ ref('int_xmatch__golden_record') }}
),

-- For each golden field, find which source record contributed it
name_lineage as (
    select
        eg.resolved_entity_id,
        'full_name' as field_name,
        g.golden_full_name as golden_value,
        eg.source_id as contributing_source_id,
        eg.source_system as contributing_source_system,
        row_number() over (
            partition by eg.resolved_entity_id
            order by
                case eg.source_system
                    when 'crm' then 1 when 'billing' then 2 when 'app' then 3
                    when 'support' then 4 when 'partners' then 5
                end
        ) as rn
    from entity_graph eg
    join golden g on eg.resolved_entity_id = g.resolved_entity_id
    where eg.source_first_name is not null
      and lower(eg.source_first_name) = lower(g.golden_first_name)
),

email_lineage as (
    select
        eg.resolved_entity_id,
        'email' as field_name,
        g.golden_email as golden_value,
        eg.source_id as contributing_source_id,
        eg.source_system as contributing_source_system,
        row_number() over (
            partition by eg.resolved_entity_id
            order by
                case eg.source_system
                    when 'crm' then 1 when 'billing' then 2 when 'app' then 3
                    when 'support' then 4 when 'partners' then 5
                end
        ) as rn
    from entity_graph eg
    join golden g on eg.resolved_entity_id = g.resolved_entity_id
    where eg.source_email = g.golden_email
),

phone_lineage as (
    select
        eg.resolved_entity_id,
        'phone' as field_name,
        g.golden_phone as golden_value,
        eg.source_id as contributing_source_id,
        eg.source_system as contributing_source_system,
        row_number() over (
            partition by eg.resolved_entity_id
            order by
                case eg.source_system
                    when 'crm' then 1 when 'support' then 2 when 'billing' then 3
                    when 'app' then 4 when 'partners' then 5
                end
        ) as rn
    from entity_graph eg
    join golden g on eg.resolved_entity_id = g.resolved_entity_id
    where eg.source_phone = g.golden_phone
)

select resolved_entity_id, field_name, golden_value,
       contributing_source_id, contributing_source_system
from name_lineage where rn = 1
union all
select resolved_entity_id, field_name, golden_value,
       contributing_source_id, contributing_source_system
from email_lineage where rn = 1
union all
select resolved_entity_id, field_name, golden_value,
       contributing_source_id, contributing_source_system
from phone_lineage where rn = 1
