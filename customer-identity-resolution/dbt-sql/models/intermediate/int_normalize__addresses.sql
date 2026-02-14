-- Intermediate: Address Normalization
-- Standardize street addresses from billing records
-- Handles: abbreviations, case, whitespace, format parsing

-- MANUAL PAIN POINT: Address normalization is notoriously hard.
-- "123 Main Street" vs "123 Main St" vs "123 Main St." -- all the same.
-- Add apartment numbers, suites, and PO boxes and it's a regex nightmare.
-- Real systems use USPS CASS validation or Google Geocoding API.

with billing_addresses as (
    select
        billing_account_id as source_id,
        'billing' as source_system,
        billing_address as raw_address
    from {{ ref('stg_billing__accounts') }}
    where billing_address is not null and trim(billing_address) != ''
),

parsed as (
    select
        source_id,
        source_system,
        raw_address,
        -- Split "street, city, state zip" format
        trim(split_part(raw_address, ',', 1)) as street,
        trim(split_part(raw_address, ',', 2)) as city,
        -- "state zip" is after the second comma
        trim(split_part(trim(split_part(raw_address, ',', 3)), ' ', 1)) as state,
        trim(split_part(trim(split_part(raw_address, ',', 3)), ' ', 2)) as zip_code
    from billing_addresses
),

normalized as (
    select
        source_id,
        source_system,
        raw_address,
        -- Normalize street abbreviations
        replace(replace(replace(replace(replace(replace(
            upper(street),
            ' STREET', ' ST'),
            ' AVENUE', ' AVE'),
            ' DRIVE', ' DR'),
            ' LANE', ' LN'),
            ' ROAD', ' RD'),
            ' BOULEVARD', ' BLVD')
        as normalized_street,
        initcap(city) as normalized_city,
        upper(state) as normalized_state,
        -- Standardize zip to 5 digits
        left(regexp_replace(zip_code, '[^0-9]', ''), 5) as normalized_zip
    from parsed
)

select
    source_id,
    source_system,
    raw_address,
    normalized_street,
    normalized_city,
    normalized_state,
    normalized_zip,
    -- Build standardized full address
    normalized_street || ', ' || normalized_city || ', ' ||
        normalized_state || ' ' || normalized_zip as normalized_full_address,
    -- Zip+city block key for dedup matching
    normalized_zip || '|' || lower(normalized_city) as address_block_key
from normalized
where normalized_zip is not null and length(normalized_zip) = 5
