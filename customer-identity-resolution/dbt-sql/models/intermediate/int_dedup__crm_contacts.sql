-- Intermediate: CRM Contact Deduplication
-- Find and merge duplicate contacts within the CRM source
-- Uses email exact match + fuzzy name matching to identify duplicates

-- MANUAL PAIN POINT: CRM is ~15% duplicates. Sales reps create new contacts
-- instead of finding existing ones. Each dedup rule is hand-tuned SQL.

with contacts as (
    select
        s.contact_spine_id,
        s.source_id,
        s.normalized_first_name,
        s.normalized_last_name,
        s.normalized_email,
        s.normalized_phone,
        s.normalized_company,
        c.created_at,
        c.is_active
    from {{ ref('int_unified__contact_spine') }} s
    join {{ ref('stg_crm__contacts') }} c
        on s.source_id = c.crm_contact_id
    where s.source_system = 'crm'
),

-- Find pairs that match on email
email_matches as (
    select
        a.contact_spine_id as spine_id_a,
        b.contact_spine_id as spine_id_b,
        a.source_id as source_id_a,
        b.source_id as source_id_b,
        'email_exact' as match_reason
    from contacts a
    join contacts b
        on a.normalized_email = b.normalized_email
        and a.contact_spine_id < b.contact_spine_id
    where a.normalized_email is not null
),

-- Find pairs that match on phone + last name soundex
phone_name_matches as (
    select
        a.contact_spine_id as spine_id_a,
        b.contact_spine_id as spine_id_b,
        a.source_id as source_id_a,
        b.source_id as source_id_b,
        'phone_name_fuzzy' as match_reason
    from contacts a
    join contacts b
        on a.normalized_phone = b.normalized_phone
        and a.last_name_soundex = b.last_name_soundex
        and a.contact_spine_id < b.contact_spine_id
    where a.normalized_phone is not null
),

all_matches as (
    select * from email_matches
    union
    select * from phone_name_matches
),

-- Pick the "survivor" record: prefer active, then oldest
ranked as (
    select
        contact_spine_id,
        source_id,
        normalized_email,
        normalized_first_name,
        normalized_last_name,
        created_at,
        is_active,
        row_number() over (
            partition by coalesce(normalized_email, normalized_phone)
            order by
                case when is_active then 0 else 1 end,
                created_at asc
        ) as survivor_rank
    from contacts
)

select
    contact_spine_id,
    source_id as crm_contact_id,
    normalized_email,
    normalized_first_name,
    normalized_last_name,
    case when survivor_rank = 1 then true else false end as is_survivor,
    survivor_rank
from ranked
