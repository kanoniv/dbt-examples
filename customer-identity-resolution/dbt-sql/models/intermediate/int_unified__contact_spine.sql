-- Intermediate: Unified Contact Spine
-- Combines all normalized identifiers into a single contact spine
-- Each row = one source record with all its normalized identity signals

-- MANUAL PAIN POINT: This is the "glue" model -- it joins across 5+ normalization
-- models, handles null coalescing, and creates the input for matching.
-- Any upstream schema change cascades here. This model breaks constantly.

with names as (
    select * from {{ ref('int_normalize__names') }}
),

emails as (
    select * from {{ ref('int_normalize__emails') }}
),

phones as (
    select * from {{ ref('int_normalize__phones') }}
),

companies as (
    select * from {{ ref('int_normalize__companies') }}
),

addresses as (
    select * from {{ ref('int_normalize__addresses') }}
),

-- Join all identity signals per source record
spine as (
    select
        n.source_id,
        n.source_system,
        n.normalized_first_name,
        n.normalized_last_name,
        n.normalized_full_name,
        n.first_name_soundex,
        n.last_name_soundex,
        e.normalized_email,
        e.email_type,
        e.corporate_domain,
        p.normalized_phone,
        p.area_code,
        c.normalized_company,
        c.company_match_key,
        a.normalized_street,
        a.normalized_city,
        a.normalized_state,
        a.normalized_zip,
        a.address_block_key,
        -- Completeness score: how many identity signals does this record have?
        (case when n.normalized_first_name is not null then 1 else 0 end
         + case when e.normalized_email is not null then 1 else 0 end
         + case when p.normalized_phone is not null then 1 else 0 end
         + case when c.normalized_company is not null then 1 else 0 end
         + case when a.normalized_zip is not null then 1 else 0 end
        ) as identity_signal_count
    from names n
    left join emails e
        on n.source_id = e.source_id
        and n.source_system = e.source_system
    left join phones p
        on n.source_id = p.source_id
        and n.source_system = p.source_system
    left join companies c
        on n.source_id = c.source_id
        and n.source_system = c.source_system
    left join addresses a
        on n.source_id = a.source_id
        and n.source_system = a.source_system
)

select
    {{ dbt_utils.generate_surrogate_key(['source_id', 'source_system']) }} as contact_spine_id,
    *
from spine
