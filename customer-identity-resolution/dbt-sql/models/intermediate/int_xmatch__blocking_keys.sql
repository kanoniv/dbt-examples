-- Intermediate: Blocking Keys
-- Generate blocking keys to reduce the O(n²) comparison space
-- Without blocking, 7,000 × 7,000 = 49M comparisons. With blocking: ~50k.

-- MANUAL PAIN POINT: Choosing blocking keys is an art, not a science.
-- Too broad = missed matches. Too narrow = too many comparisons.
-- Every new source or field change requires re-evaluating the blocking strategy.

with survivors as (
    -- Only use deduplicated survivors from each source
    select contact_spine_id, source_id, source_system,
           normalized_first_name, normalized_last_name,
           first_name_soundex, last_name_soundex,
           normalized_email, normalized_phone,
           normalized_company, company_match_key,
           normalized_zip, identity_signal_count
    from {{ ref('int_unified__contact_spine') }}
    where identity_signal_count >= 2  -- Skip records with too few signals
),

-- Block 1: Exact email
email_blocks as (
    select
        contact_spine_id,
        source_system,
        'email|' || normalized_email as block_key
    from survivors
    where normalized_email is not null
),

-- Block 2: Exact phone
phone_blocks as (
    select
        contact_spine_id,
        source_system,
        'phone|' || normalized_phone as block_key
    from survivors
    where normalized_phone is not null
),

-- Block 3: Last name soundex + first 3 chars of first name
name_blocks as (
    select
        contact_spine_id,
        source_system,
        'name|' || last_name_soundex || '|' || left(lower(normalized_first_name), 3) as block_key
    from survivors
    where last_name_soundex is not null
      and normalized_first_name is not null
      and length(normalized_first_name) >= 3
),

-- Block 4: Company match key + last name soundex
company_name_blocks as (
    select
        contact_spine_id,
        source_system,
        'company_name|' || company_match_key || '|' || last_name_soundex as block_key
    from survivors
    where company_match_key is not null
      and last_name_soundex is not null
),

-- Block 5: Zip code + last name soundex
zip_name_blocks as (
    select
        contact_spine_id,
        source_system,
        'zip_name|' || normalized_zip || '|' || last_name_soundex as block_key
    from survivors
    where normalized_zip is not null
      and last_name_soundex is not null
)

select * from email_blocks
union all select * from phone_blocks
union all select * from name_blocks
union all select * from company_name_blocks
union all select * from zip_name_blocks
