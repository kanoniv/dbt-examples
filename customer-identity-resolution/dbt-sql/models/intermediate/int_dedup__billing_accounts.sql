-- Intermediate: Billing Account Deduplication
-- Find duplicate billing accounts within the billing source
-- Billing is relatively clean but has some accounts for same person under different plans

-- MANUAL PAIN POINT: Billing accounts sometimes get created fresh on plan change
-- instead of upgrading. Same email = same person, but different account IDs.

with accounts as (
    select
        s.contact_spine_id,
        s.source_id,
        s.normalized_first_name,
        s.normalized_last_name,
        s.normalized_email,
        s.normalized_company,
        b.account_status,
        b.mrr_cents,
        b.created_at
    from {{ ref('int_unified__contact_spine') }} s
    join {{ ref('stg_billing__accounts') }} b
        on s.source_id = b.billing_account_id
    where s.source_system = 'billing'
),

-- Find exact email duplicates
email_dupes as (
    select
        normalized_email,
        count(*) as dupe_count
    from accounts
    where normalized_email is not null
    group by normalized_email
    having count(*) > 1
),

-- Rank to pick survivor: prefer active + highest MRR
ranked as (
    select
        a.contact_spine_id,
        a.source_id as billing_account_id,
        a.normalized_email,
        a.normalized_first_name,
        a.normalized_last_name,
        a.account_status,
        a.mrr_cents,
        case when d.normalized_email is not null then true else false end as is_duplicate,
        row_number() over (
            partition by a.normalized_email
            order by
                case a.account_status when 'active' then 0 when 'past_due' then 1 else 2 end,
                a.mrr_cents desc,
                a.created_at asc
        ) as survivor_rank
    from accounts a
    left join email_dupes d
        on a.normalized_email = d.normalized_email
)

select
    contact_spine_id,
    billing_account_id,
    normalized_email,
    normalized_first_name,
    normalized_last_name,
    account_status,
    is_duplicate,
    case when survivor_rank = 1 then true else false end as is_survivor,
    survivor_rank
from ranked
