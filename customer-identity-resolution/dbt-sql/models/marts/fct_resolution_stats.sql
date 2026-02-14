-- Mart: Resolution Statistics
-- High-level summary of the identity resolution pipeline
-- Used for executive dashboards and data quality monitoring

with customers as (
    select * from {{ ref('dim_customers') }}
),

quality as (
    select * from {{ ref('int_resolve__match_quality') }}
),

unmatched as (
    select * from {{ ref('int_resolve__unmatched_records') }}
),

review_queue as (
    select * from {{ ref('int_xmatch__review_queue') }}
),

conflicts as (
    select * from {{ ref('int_resolve__conflict_log') }}
)

select
    -- Entity counts
    (select count(*) from customers) as total_resolved_entities,
    (select count(*) from customers where quality_tier = 'gold') as gold_entities,
    (select count(*) from customers where quality_tier = 'silver') as silver_entities,
    (select count(*) from customers where quality_tier = 'bronze') as bronze_entities,

    -- Source coverage
    (select avg(num_sources) from customers) as avg_sources_per_entity,
    (select avg(num_source_records) from customers) as avg_records_per_entity,

    -- Match quality
    (select pair_count from quality where metric_type = 'confirmed_matches') as confirmed_match_pairs,
    (select avg_score from quality where metric_type = 'confirmed_matches') as avg_match_score,

    -- Review queue
    (select count(*) from review_queue) as pending_reviews,

    -- Unmatched
    (select count(distinct contact_spine_id) from unmatched) as unmatched_records,
    (select count(distinct contact_spine_id) from unmatched
     where unmatched_reason = 'insufficient_signals') as unmatched_insufficient_signals,
    (select count(distinct contact_spine_id) from unmatched
     where unmatched_reason = 'no_email_or_phone') as unmatched_no_contact_info,

    -- Conflicts
    (select count(*) from conflicts) as total_field_conflicts,
    (select count(distinct resolved_entity_id) from conflicts) as entities_with_conflicts,

    -- Pipeline metadata
    current_timestamp as computed_at
