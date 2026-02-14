-- Intermediate: Conflict Log
-- Track field-level conflicts within each resolved entity
-- When source A says "Acme Corp" and source B says "ACME", log it

-- MANUAL PAIN POINT: Field conflicts reveal data quality issues upstream,
-- but surfacing them requires comparing every field across every source record
-- in a cluster. This is N×M comparisons per entity, per field.

with entity_graph as (
    select * from {{ ref('int_xmatch__entity_graph') }}
),

-- Find entities where golden value differs from source value
name_conflicts as (
    select
        resolved_entity_id,
        entity_number,
        source_id,
        source_system,
        'first_name' as field_name,
        source_first_name as source_value,
        golden_full_name as golden_value
    from entity_graph
    where source_first_name is not null
      and lower(source_first_name) != lower(
          split_part(golden_full_name, ' ', 1)
      )
),

email_conflicts as (
    select
        resolved_entity_id,
        entity_number,
        source_id,
        source_system,
        'email' as field_name,
        source_email as source_value,
        golden_email as golden_value
    from entity_graph
    where source_email is not null
      and golden_email is not null
      and source_email != golden_email
),

company_conflicts as (
    select
        resolved_entity_id,
        entity_number,
        source_id,
        source_system,
        'company' as field_name,
        source_company as source_value,
        golden_company as golden_value
    from entity_graph
    where source_company is not null
      and golden_company is not null
      and source_company != golden_company
),

phone_conflicts as (
    select
        resolved_entity_id,
        entity_number,
        source_id,
        source_system,
        'phone' as field_name,
        source_phone as source_value,
        golden_phone as golden_value
    from entity_graph
    where source_phone is not null
      and golden_phone is not null
      and source_phone != golden_phone
)

select
    resolved_entity_id,
    entity_number,
    source_id,
    source_system,
    field_name,
    source_value,
    golden_value,
    current_timestamp as detected_at
from name_conflicts
union all select *, current_timestamp from email_conflicts
union all select *, current_timestamp from company_conflicts
union all select *, current_timestamp from phone_conflicts
