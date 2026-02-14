-- Intermediate: Candidate Pairs
-- Generate cross-source candidate pairs using blocking keys
-- Only pairs from DIFFERENT sources are considered (cross-source matching)

-- MANUAL PAIN POINT: This is where the quadratic explosion lives.
-- Each blocking strategy generates different candidate pairs.
-- Too many candidates = hours of compute. Too few = missed matches.
-- Tuning this is trial-and-error with no automated feedback loop.

with blocks as (
    select * from {{ ref('int_xmatch__blocking_keys') }}
),

-- Self-join within each block to generate candidate pairs
-- Only keep cross-source pairs (different source_system)
candidate_pairs as (
    select distinct
        a.contact_spine_id as spine_id_a,
        b.contact_spine_id as spine_id_b,
        a.source_system as source_a,
        b.source_system as source_b,
        a.block_key
    from blocks a
    join blocks b
        on a.block_key = b.block_key
        and a.source_system < b.source_system  -- Cross-source only, avoid duplicates
        and a.contact_spine_id != b.contact_spine_id
)

-- Deduplicate pairs (same pair may appear via multiple blocking keys)
select
    spine_id_a,
    spine_id_b,
    source_a,
    source_b,
    listagg(distinct block_key, ' | ') within group (order by block_key) as matching_blocks,
    count(distinct block_key) as num_blocking_keys
from candidate_pairs
group by spine_id_a, spine_id_b, source_a, source_b
