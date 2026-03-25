{% snapshot snap_store %}

{{
    config(
        target_schema='DEV',
        unique_key='store_number',
        strategy='check',
        check_cols=[
            'store_name',
            'chain',
            'address',
            'city',
            'zip_code',
            'store_location',
            'county_number',
            'county'
        ]
    )
}}

with latest_store_attributes as (
    select
        store_number,
        store_name,
        case
            when upper(store_name) like '%HY-VEE%' then 'HY-VEE'
            when upper(store_name) like '%SAM''S CLUB%' then 'SAM''S CLUB'
            when upper(store_name) like '%SMOKIN'' JOE%' then 'SMOKIN'' JOE''S'
            when upper(store_name) like '%CASH SAVER%' then 'CASH SAVER'
            when upper(store_name) like '%KWIK STAR%' then 'KWIK STAR'
            when upper(store_name) like '%PRICE CHOPPER%' then 'PRICE CHOPPER'
            when upper(store_name) like '%JIFFY EXPRESS%' then 'JIFFY EXPRESS'
            when upper(store_name) like '%JIFFY MART%' then 'JIFFY MART'
            when upper(store_name) like '%QUIK TRIP%' then 'QUIK TRIP'
            when upper(store_name) like '%YESWAY%' then 'YESWAY'
            when upper(store_name) like '%TOBACCO OUTLET PLUS%' then 'TOBACCO OUTLET PLUS'
            when upper(store_name) like '%TOBACCO HUT %' then 'TOBACCO HUT'
            when upper(store_name) like '%MOES MART%' then 'MOES MART'
            when upper(store_name) like '%KWIK SHOP%' then 'KWIK SHOP'
            when upper(store_name) like '%KIMMES%' then 'KIMMES'
            when upper(store_name) like '%HARTIG DRUG%' then 'HARTIG DRUG'
            when upper(store_name) like '%CIRCLE K%' then 'CIRCLE K'
            when upper(store_name) like '%BUCKY''S EXPRESS%' then 'BUCKY''S EXPRESS'
            when upper(store_name) like '%BREW OIL%' then 'BREW OIL'
            when upper(store_name) like '%BIG 10 MART%' then 'BIG 10 MART'
            when upper(store_name) like '%TARGET%' then 'TARGET'
            when upper(store_name) like '%WALMART%' then 'WALMART'
            when upper(store_name) like '%WAL-MART%' then 'WALMART'
            when upper(store_name) like '%WALGREENS%' then 'WALGREENS'
            when upper(store_name) like '%CVS%' then 'CVS'
            when upper(store_name) like '%COSTCO%' then 'COSTCO'
            when upper(store_name) like '%FAREWAY%' then 'FAREWAY'
            when upper(store_name) like '%CASEY%' then 'CASEY''S'
            when upper(store_name) like '%KUM & GO%' then 'KUM & GO'
            else 'INDEPENDENT'
        end as chain,
        address,
        city,
        zip_code,
        store_location,
        county_number,
        county,
        row_number() over (
            partition by store_number
            order by loaded_at desc
        ) as rn
    from {{ ref('int_iowa_liquor_sales') }}
    where store_number is not null
)

select
    store_number,
    store_name,
    chain,
    address,
    city,
    zip_code,
    store_location,
    county_number,
    county
from latest_store_attributes
where rn = 1

{% endsnapshot %}