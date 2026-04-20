-- Intermediate historical item pricing model.
-- Owns package-size normalization and price-position derivation before the final mart exposes the fields.

with base as (
    select
        h.item_number,
        h.item_description,
        h.category,
        h.category_name,
        coalesce(m.category_family, 'Other') as category_family,
        h.vendor_number,
        h.vendor_name,
        h.bottle_volume_ml,
        h.pack,
        h.state_bottle_cost,
        h.state_bottle_retail,
        h.business_valid_from,
        h.business_valid_to,
        h.is_current,
        h.source_loaded_at
    from {{ ref('int_item_business_history') }} h
    left join {{ ref('category_family_map') }} m
        on h.category = m.category
),

with_size as (
    select
        *,
        case
            when coalesce(item_description, '') ilike any (
                '%TRI PACK%',
                '%TRIPACK%',
                '%TRAY PACK%',
                '%GIFT PACK%',
                '%GIFT SET%',
                '%SAMPLER%',
                '%VARIETY PACK%',
                '%COMBO PACK%'
            ) then true
            when regexp_like(
                upper(coalesce(item_description, '')),
                '.*([0-9]+[ -]?PACK|[0-9]+PK).*'
            ) then true
            else false
        end as is_bundle_pack,
        case
            when bottle_volume_ml is null or bottle_volume_ml <= 0 then null
            when bottle_volume_ml between 680 and 720 then 750
            else bottle_volume_ml
        end as effective_bottle_volume_ml,
        case
            when bottle_volume_ml is null or bottle_volume_ml <= 0 then 'unknown'
            when bottle_volume_ml <= 50 then 'micro_trial'
            when bottle_volume_ml <= 100 then 'mini_trial'
            when bottle_volume_ml <= 200 then 'large_trial'
            when bottle_volume_ml <= 500 then 'half_size'
            when bottle_volume_ml <= 800 then 'standard'
            else 'jumbo_value'
        end as package_size_tier
    from base
),

with_pricing as (
    select
        *,
        case
            when is_bundle_pack then null
            when state_bottle_retail is not null and effective_bottle_volume_ml > 0
                then round((state_bottle_retail / effective_bottle_volume_ml) * 100, 2)
            else null
        end as price_per_100ml,
        case
            when state_bottle_retail is null then 'unknown'
            when state_bottle_retail < 15 then '0_to_15'
            when state_bottle_retail < 30 then '15_to_30'
            when state_bottle_retail < 45 then '30_to_45'
            when state_bottle_retail < 60 then '45_to_60'
            when state_bottle_retail < 100 then '60_to_100'
            when state_bottle_retail < 250 then '100_to_250'
            when state_bottle_retail < 1000 then '250_to_1000'
            else '1000_plus'
        end as retail_price_tier,
        case
            when is_bundle_pack then 'bundle_unknown'
            when effective_bottle_volume_ml is null or effective_bottle_volume_ml <= 0 or state_bottle_retail is null then 'unknown'
            when (state_bottle_retail / effective_bottle_volume_ml) * 100 < 2 then '0_to_2'
            when (state_bottle_retail / effective_bottle_volume_ml) * 100 < 4 then '2_to_4'
            when (state_bottle_retail / effective_bottle_volume_ml) * 100 < 6 then '4_to_6'
            when (state_bottle_retail / effective_bottle_volume_ml) * 100 < 8 then '6_to_8'
            when (state_bottle_retail / effective_bottle_volume_ml) * 100 < 12 then '8_to_12'
            when (state_bottle_retail / effective_bottle_volume_ml) * 100 < 20 then '12_to_20'
            else '20_plus'
        end as price_per_100ml_tier
    from with_size
)

select
    *,
    case
        when retail_price_tier = 'unknown' or price_per_100ml_tier = 'unknown' or package_size_tier = 'unknown' then 'unknown'
        when is_bundle_pack then 'bundle_pack'
        when package_size_tier in ('micro_trial', 'mini_trial', 'large_trial')
             and (state_bottle_retail >= 100 or price_per_100ml >= 20) then 'trial_luxury'
        when package_size_tier in ('micro_trial', 'mini_trial', 'large_trial')
             and (state_bottle_retail >= 30 or price_per_100ml >= 8) then 'trial_premium'
        when package_size_tier in ('micro_trial', 'mini_trial', 'large_trial') then 'trial_value'
        when package_size_tier in ('standard', 'half_size') and state_bottle_retail >= 1000 and price_per_100ml >= 20 then 'icon_collectible'
        when package_size_tier in ('standard', 'half_size') and state_bottle_retail >= 250 and price_per_100ml >= 12 then 'luxury'
        when package_size_tier in ('standard', 'half_size') and state_bottle_retail >= 100 and price_per_100ml >= 8 then 'ultra_premium'
        when package_size_tier in ('standard', 'half_size') and state_bottle_retail >= 60 and price_per_100ml >= 6 then 'super_premium'
        when package_size_tier = 'jumbo_value' and price_per_100ml < 4 then 'value_oversized'
        when package_size_tier = 'standard' and state_bottle_retail >= 30 and price_per_100ml >= 4 then 'premium'
        when package_size_tier = 'half_size' and state_bottle_retail >= 30 and price_per_100ml >= 2 then 'premium'
        when package_size_tier = 'standard' and state_bottle_retail >= 15 and price_per_100ml >= 1.5 then 'standard'
        when package_size_tier = 'half_size' and state_bottle_retail >= 15 and price_per_100ml >= 1 then 'standard'
        when package_size_tier = 'standard' and state_bottle_retail <= 15 then 'budget'
        when package_size_tier = 'half_size' and state_bottle_retail <= 15 then 'budget'
        when package_size_tier = 'jumbo_value' and state_bottle_retail < 15 and price_per_100ml < 2 then 'high_volume'
        when package_size_tier = 'jumbo_value' and state_bottle_retail >= 15 and price_per_100ml >= 2 then 'premium_bulk'
        else 'other'
    end as price_position_segment
from with_pricing
