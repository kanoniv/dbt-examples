-- Intermediate: Support User Deduplication
-- Find duplicate support users within the support source
-- Support is the messiest -- users create new accounts when they forget passwords

-- MANUAL PAIN POINT: Support users often have phone-only or email-only records.
-- Matching phone-only to email-only requires fuzzy name matching on display_name,
-- which is unreliable. Many false negatives here.

with users as (
    select
        s.contact_spine_id,
        s.source_id,
        s.normalized_first_name,
        s.normalized_last_name,
        s.first_name_soundex,
        s.last_name_soundex,
        s.normalized_email,
        s.normalized_phone,
        s.normalized_company,
        u.created_at,
        u.last_seen_at
    from {{ ref('int_unified__contact_spine') }} s
    join {{ ref('stg_support__users') }} u
        on s.source_id = u.support_user_id
    where s.source_system = 'support'
),

-- Find email-based duplicates
email_matches as (
    select
        a.contact_spine_id as spine_id_a,
        b.contact_spine_id as spine_id_b,
        'email' as match_type
    from users a
    join users b
        on a.normalized_email = b.normalized_email
        and a.contact_spine_id < b.contact_spine_id
    where a.normalized_email is not null
),

-- Find phone-based duplicates with name similarity
phone_matches as (
    select
        a.contact_spine_id as spine_id_a,
        b.contact_spine_id as spine_id_b,
        'phone_name' as match_type
    from users a
    join users b
        on a.normalized_phone = b.normalized_phone
        and a.last_name_soundex = b.last_name_soundex
        and a.contact_spine_id < b.contact_spine_id
    where a.normalized_phone is not null
      and a.last_name_soundex is not null
),

all_matches as (
    select * from email_matches
    union
    select * from phone_matches
),

-- Build cluster: pick oldest active user as survivor
ranked as (
    select
        u.contact_spine_id,
        u.source_id as support_user_id,
        u.normalized_email,
        u.normalized_phone,
        u.normalized_first_name,
        u.normalized_last_name,
        row_number() over (
            partition by coalesce(u.normalized_email, u.normalized_phone, u.contact_spine_id)
            order by u.last_seen_at desc, u.created_at asc
        ) as survivor_rank
    from users u
)

select
    contact_spine_id,
    support_user_id,
    normalized_email,
    normalized_phone,
    normalized_first_name,
    normalized_last_name,
    case when survivor_rank = 1 then true else false end as is_survivor,
    survivor_rank
from ranked
