-- Intermediate: App Signup Deduplication
-- Find duplicate app signups (users who signed up multiple times)
-- App data is keyed on email, so exact email match is primary dedup strategy

-- MANUAL PAIN POINT: 10% of app users signed up with personal email first,
-- then again with work email. Email-only dedup misses these entirely.
-- Would need name+company fuzzy matching to catch them.

with signups as (
    select
        s.contact_spine_id,
        s.source_id,
        s.normalized_first_name,
        s.normalized_last_name,
        s.normalized_email,
        a.signup_source,
        a.is_verified,
        a.created_at,
        a.last_login_at
    from {{ ref('int_unified__contact_spine') }} s
    join {{ ref('stg_app__signups') }} a
        on s.source_id = a.app_user_id
    where s.source_system = 'app'
),

-- Find email duplicates
email_dupes as (
    select normalized_email, count(*) as cnt
    from signups
    where normalized_email is not null
    group by normalized_email
    having count(*) > 1
),

-- Rank: prefer verified, then most recently active
ranked as (
    select
        s.contact_spine_id,
        s.source_id as app_user_id,
        s.normalized_email,
        s.normalized_first_name,
        s.normalized_last_name,
        s.is_verified,
        case when d.normalized_email is not null then true else false end as is_duplicate,
        row_number() over (
            partition by s.normalized_email
            order by
                case when s.is_verified then 0 else 1 end,
                s.last_login_at desc,
                s.created_at asc
        ) as survivor_rank
    from signups s
    left join email_dupes d on s.normalized_email = d.normalized_email
)

select
    contact_spine_id,
    app_user_id,
    normalized_email,
    normalized_first_name,
    normalized_last_name,
    is_duplicate,
    case when survivor_rank = 1 then true else false end as is_survivor,
    survivor_rank
from ranked
