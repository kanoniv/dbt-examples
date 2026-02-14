-- Intermediate: Match Quality Metrics
-- Aggregate match quality statistics for reporting and debugging

-- MANUAL PAIN POINT: Without proper observability into match quality,
-- you're flying blind. Did the latest threshold change improve precision?
-- Are certain source pairs producing more false positives? You have to
-- build all this instrumentation yourself.

with decisions as (
    select * from {{ ref('int_xmatch__match_decisions') }}
),

clusters as (
    select * from {{ ref('int_xmatch__transitive_closure') }}
),

-- Score distribution of confirmed matches
match_stats as (
    select
        'confirmed_matches' as metric_type,
        count(*) as pair_count,
        avg(total_score) as avg_score,
        min(total_score) as min_score,
        max(total_score) as max_score,
        percentile_cont(0.25) within group (order by total_score) as p25_score,
        percentile_cont(0.50) within group (order by total_score) as median_score,
        percentile_cont(0.75) within group (order by total_score) as p75_score
    from decisions
    where final_decision = 'match'
),

-- Score distribution of review queue
review_stats as (
    select
        'review_queue' as metric_type,
        count(*) as pair_count,
        avg(total_score) as avg_score,
        min(total_score) as min_score,
        max(total_score) as max_score,
        percentile_cont(0.25) within group (order by total_score) as p25_score,
        percentile_cont(0.50) within group (order by total_score) as median_score,
        percentile_cont(0.75) within group (order by total_score) as p75_score
    from decisions
    where final_decision = 'review'
),

-- Cluster size distribution
cluster_sizes as (
    select
        resolved_entity_id,
        count(*) as cluster_size
    from clusters
    group by resolved_entity_id
),

cluster_stats as (
    select
        'cluster_sizes' as metric_type,
        count(*) as pair_count,  -- reused column = num clusters
        avg(cluster_size) as avg_score,
        min(cluster_size) as min_score,
        max(cluster_size) as max_score,
        percentile_cont(0.25) within group (order by cluster_size) as p25_score,
        percentile_cont(0.50) within group (order by cluster_size) as median_score,
        percentile_cont(0.75) within group (order by cluster_size) as p75_score
    from cluster_sizes
)

select * from match_stats
union all select * from review_stats
union all select * from cluster_stats
