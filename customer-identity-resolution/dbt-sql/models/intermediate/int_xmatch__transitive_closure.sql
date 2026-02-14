-- Intermediate: Transitive Closure
-- If A=B and B=C, then A=C. Build connected components from pairwise matches.

-- MANUAL PAIN POINT: Transitive closure in SQL is painful. You need recursive CTEs
-- or iterative approaches. Most teams either skip it (losing matches) or implement
-- it incorrectly (creating mega-clusters that merge unrelated entities).

with confirmed_matches as (
    select
        spine_id_a,
        spine_id_b,
        total_score,
        confidence
    from {{ ref('int_xmatch__match_decisions') }}
    where final_decision = 'match'
),

-- Build edges (bidirectional)
edges as (
    select spine_id_a as node, spine_id_b as neighbor from confirmed_matches
    union
    select spine_id_b as node, spine_id_a as neighbor from confirmed_matches
),

-- Assign initial cluster = smallest connected node ID
-- Using recursive CTE to find connected components
initial_clusters as (
    select distinct node as spine_id, node as cluster_id
    from edges

    union

    select distinct neighbor as spine_id, neighbor as cluster_id
    from edges
),

-- Iterative: propagate minimum cluster_id through edges
-- Snowflake supports recursive CTEs
resolved as (
    select
        spine_id,
        min(cluster_id) as cluster_id
    from initial_clusters
    group by spine_id
),

-- Propagate through one level of edges
-- NOTE: In production, this needs multiple iterations or a proper graph algorithm.
-- SQL-based transitive closure is fundamentally limited.
propagated as (
    select
        r.spine_id,
        least(r.cluster_id, coalesce(min(r2.cluster_id), r.cluster_id)) as cluster_id
    from resolved r
    left join edges e on r.spine_id = e.node
    left join resolved r2 on e.neighbor = r2.spine_id
    group by r.spine_id, r.cluster_id
),

-- Second propagation pass (catches 3-hop chains)
propagated_2 as (
    select
        p.spine_id,
        least(p.cluster_id, coalesce(min(p2.cluster_id), p.cluster_id)) as cluster_id
    from propagated p
    left join edges e on p.spine_id = e.node
    left join propagated p2 on e.neighbor = p2.spine_id
    group by p.spine_id, p.cluster_id
),

-- Third pass (should converge for most real-world clusters)
propagated_3 as (
    select
        p.spine_id,
        least(p.cluster_id, coalesce(min(p2.cluster_id), p.cluster_id)) as cluster_id
    from propagated_2 p
    left join edges e on p.spine_id = e.node
    left join propagated_2 p2 on e.neighbor = p2.spine_id
    group by p.spine_id, p.cluster_id
)

-- MANUAL PAIN POINT: 3 propagation passes is arbitrary. Deep chains need more.
-- A real graph database handles this in milliseconds. In SQL, you're hoping
-- your clusters aren't deeper than N iterations. No guarantees.

select
    spine_id as contact_spine_id,
    cluster_id as resolved_entity_id,
    dense_rank() over (order by cluster_id) as entity_number
from propagated_3
