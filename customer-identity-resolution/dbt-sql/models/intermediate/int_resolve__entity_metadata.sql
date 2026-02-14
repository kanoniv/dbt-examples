-- Intermediate: Entity Metadata
-- Enrich each resolved entity with aggregated metadata from all source records

with entity_graph as (
    select * from {{ ref('int_xmatch__entity_graph') }}
),

-- Count source presence per entity
source_coverage as (
    select
        resolved_entity_id,
        entity_number,
        golden_full_name,
        golden_email,
        golden_phone,
        golden_company,
        count(distinct source_system) as num_sources,
        count(*) as num_source_records,
        listagg(distinct source_system, ', ') within group (order by source_system) as sources_present,
        avg(identity_signal_count) as avg_signal_count,
        min(identity_signal_count) as min_signal_count,
        max(identity_signal_count) as max_signal_count
    from entity_graph
    group by resolved_entity_id, entity_number,
             golden_full_name, golden_email, golden_phone, golden_company
)

select
    resolved_entity_id,
    entity_number,
    golden_full_name,
    golden_email,
    golden_phone,
    golden_company,
    num_sources,
    num_source_records,
    sources_present,
    avg_signal_count,
    -- Entity quality tier
    case
        when num_sources >= 4 and golden_email is not null and golden_phone is not null
        then 'gold'
        when num_sources >= 2 and golden_email is not null
        then 'silver'
        when num_sources >= 1
        then 'bronze'
        else 'unresolved'
    end as quality_tier,
    -- Merge ratio: how many source records collapsed into this entity
    num_source_records::float / nullif(num_sources, 0) as avg_records_per_source
from source_coverage
