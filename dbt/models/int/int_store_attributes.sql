with ranked_source as (
    select
        store_number,
        store_name,
        case
            -- Grocery
            when upper(store_name) like '%HY-VEE%'                then 'HY-VEE'
            when upper(store_name) like '%FAREWAY%'               then 'FAREWAY'
            when upper(store_name) like '%PRICE CHOPPER%'         then 'PRICE CHOPPER'
            when upper(store_name) like '%CASH SAVER%'            then 'CASH SAVER'
            -- Warehouse/Club
            when upper(store_name) like '%SAM''S CLUB%'           then 'SAM''S CLUB'
            when upper(store_name) like '%COSTCO%'                then 'COSTCO'
            -- Mass Merchandise
            when upper(store_name) like '%WALMART%'               then 'WALMART'
            when upper(store_name) like '%WAL-MART%'              then 'WALMART'
            when upper(store_name) like '%TARGET%'                then 'TARGET'
            -- Drug
            when upper(store_name) like '%WALGREENS%'             then 'WALGREENS'
            when upper(store_name) like '%CVS%'                   then 'CVS'
            when upper(store_name) like '%HARTIG DRUG%'           then 'HARTIG DRUG'
            -- Convenience/Gas
            when upper(store_name) like '%CASEY%'                 then 'CASEY''S'
            when upper(store_name) like '%KWIK STAR%'             then 'KWIK STAR'
            when upper(store_name) like '%KUM & GO%'              then 'KUM & GO'
            when upper(store_name) like '%QUIK TRIP%'             then 'QUIK TRIP'
            when upper(store_name) like '%CIRCLE K%'              then 'CIRCLE K'
            when upper(store_name) like '%YESWAY%'                then 'YESWAY'
            when upper(store_name) like '%KWIK SHOP%'             then 'KWIK SHOP'
            when upper(store_name) like '%MAVERIK%'               then 'MAVERIK'
            -- Specialty/Tobacco
            when upper(store_name) like '%SMOKIN'' JOE%'          then 'SMOKIN'' JOE''S'
            when upper(store_name) like '%TOBACCO OUTLET PLUS%'   then 'TOBACCO OUTLET PLUS'
            when upper(store_name) like '%TOBACCO HUT%'           then 'TOBACCO HUT'
            when upper(store_name) like '%HAWKEYE%'               then 'HAWKEYE'
            else 'Small/Ind Chain'
        end as chain,
        case
            when upper(store_name) like '%(ET)%'                  then 'Winery/Distillery'
            when upper(store_name) like '%DISTILL%'               then 'Winery/Distillery'
            when upper(store_name) like '%WINERY%'                then 'Winery/Distillery'
            when upper(store_name) like '%VINEYARD%'              then 'Winery/Distillery'
            when upper(store_name) like '%BREWING CO%'            then 'Winery/Distillery'
            when upper(store_name) like '%CASINO%'                then 'On-Premise'
            when upper(store_name) like '%HOTEL%'                 then 'On-Premise'
            when upper(store_name) like '%RESORT%'                then 'On-Premise'
            when upper(store_name) like '%ADVENTURELAND%'         then 'On-Premise'
            when upper(store_name) like '%HY-VEE%'                then 'Grocery'
            when upper(store_name) like '%FAREWAY%'               then 'Grocery'
            when upper(store_name) like '%PRICE CHOPPER%'         then 'Grocery'
            when upper(store_name) like '%CASH SAVER%'            then 'Grocery'
            when upper(store_name) like '%SAM''S CLUB%'           then 'Warehouse/Club'
            when upper(store_name) like '%COSTCO%'                then 'Warehouse/Club'
            when upper(store_name) like '%WALMART%'               then 'Mass Merchandise'
            when upper(store_name) like '%WAL-MART%'              then 'Mass Merchandise'
            when upper(store_name) like '%TARGET%'                then 'Mass Merchandise'
            when upper(store_name) like '%WALGREENS%'             then 'Drug'
            when upper(store_name) like '%CVS%'                   then 'Drug'
            when upper(store_name) like '%HARTIG DRUG%'           then 'Drug'
            when upper(store_name) like '%HAWKEYE%'               then 'Specialty/Tobacco'
            when upper(store_name) like '%SMOKIN'' JOE%'          then 'Specialty/Tobacco'
            when upper(store_name) like '%KWIK SPIRITS%'          then 'Liquor Store'
            when upper(store_name) like '%KIMMES%'                then 'Liquor Store'
            when upper(store_name) like '%PRIME MART%'            then 'Convenience/Gas'
            when upper(store_name) like '%MEGA SAVER%'            then 'Convenience/Gas'
            when upper(store_name) like '%MAVERIK%'               then 'Convenience/Gas'
            when upper(store_name) like '%KINGS MART%'            then 'Convenience/Gas'
            when upper(store_name) like '%JET STOP%'              then 'Convenience/Gas'
            when upper(store_name) like '%INDY 66%'               then 'Convenience/Gas'
            when upper(store_name) like '%FAST AND FRESH%'        then 'Convenience/Gas'
            when upper(store_name) like '%BANI''S%'               then 'Convenience/Gas'
            when upper(store_name) like '%KWIK STAR%'             then 'Convenience/Gas'
            when upper(store_name) like '%CASEY%'                 then 'Convenience/Gas'
            when upper(store_name) like '%KUM & GO%'              then 'Convenience/Gas'
            when upper(store_name) like '%QUIK TRIP%'             then 'Convenience/Gas'
            when upper(store_name) like '%CIRCLE K%'              then 'Convenience/Gas'
            when upper(store_name) like '%YESWAY%'                then 'Convenience/Gas'
            when upper(store_name) like '%KWIK SHOP%'             then 'Convenience/Gas'
            when upper(store_name) like '%JIFFY EXPRESS%'         then 'Convenience/Gas'
            when upper(store_name) like '%JIFFY MART%'            then 'Convenience/Gas'
            when upper(store_name) like '%BUCKY''S EXPRESS%'      then 'Convenience/Gas'
            when upper(store_name) like '%BREW OIL%'              then 'Convenience/Gas'
            when upper(store_name) like '%BREW #%'                then 'Convenience/Gas'
            when upper(store_name) like '%BIG 10 MART%'           then 'Convenience/Gas'
            when upper(store_name) like '%MOES MART%'             then 'Convenience/Gas'
            when upper(store_name) like '%BP %'                   then 'Convenience/Gas'
            when upper(store_name) like '%ALL STOP%'              then 'Convenience/Gas'
            when upper(store_name) like '%CONVENIENCE%'           then 'Convenience/Gas'
            when upper(store_name) like '%MINI MART%'             then 'Convenience/Gas'
            when upper(store_name) like '%QUICK STOP%'            then 'Convenience/Gas'
            when upper(store_name) like '%FAS MART%'              then 'Convenience/Gas'
            when upper(store_name) like '%KWIK STOP%'             then 'Convenience/Gas'
            when upper(store_name) like '%SUPER QUICK%'           then 'Convenience/Gas'
            when upper(store_name) like '%SUPER STOP%'            then 'Convenience/Gas'
            when upper(store_name) like '%FAST BREAK%'            then 'Convenience/Gas'
            when upper(store_name) like '%TRUCK STOP%'            then 'Convenience/Gas'
            when upper(store_name) like '%TRAVEL CENTER%'         then 'Convenience/Gas'
            when upper(store_name) like '%TRAVEL PLAZA%'          then 'Convenience/Gas'
            when upper(store_name) like '%SHELL%'                 then 'Convenience/Gas'
            when upper(store_name) like '%SINCLAIR%'              then 'Convenience/Gas'
            when upper(store_name) like '%CONOCO%'                then 'Convenience/Gas'
            when upper(store_name) like '%PHILLIPS 66%'           then 'Convenience/Gas'
            when upper(store_name) like '%AMPRIDE%'               then 'Convenience/Gas'
            when upper(store_name) like '%CENEX%'                 then 'Convenience/Gas'
            when upper(store_name) like '%GASLAND%'               then 'Convenience/Gas'
            when upper(store_name) like '%DYNO''S%'               then 'Convenience/Gas'
            when upper(store_name) like '%NEW STAR%'              then 'Convenience/Gas'
            when upper(store_name) like '%THE SPOT%'              then 'Convenience/Gas'
            when upper(store_name) like '%THE DEPOT%'             then 'Convenience/Gas'
            when upper(store_name) like '%CUBBY''S%'              then 'Convenience/Gas'
            when upper(store_name) like '%GOPUFF%'                then 'Convenience/Gas'
            when upper(store_name) like '%GOODSTOP%'              then 'Convenience/Gas'
            when upper(store_name) like '%MCDERMOTT OIL%'         then 'Convenience/Gas'
            when upper(store_name) like '%WASPY''S%'              then 'Convenience/Gas'
            when upper(store_name) like '%SPARKY''S%'             then 'Convenience/Gas'
            when upper(store_name) like '%RUSH STOP%'             then 'Convenience/Gas'
            when upper(store_name) like '%ESSENTIALS%'            then 'Convenience/Gas'
            when upper(store_name) like '%EZ STOP%'               then 'Convenience/Gas'
            when upper(store_name) like '%ONE STOP%'              then 'Convenience/Gas'
            when upper(store_name) like '%PIT STOP%'              then 'Convenience/Gas'
            when upper(store_name) like '%STOP N SHOP%'           then 'Convenience/Gas'
            when upper(store_name) like '%PANTRY%'                then 'Convenience/Gas'
            when upper(store_name) like '%BEVERAGE%'              then 'Liquor Store'
            when upper(store_name) like '%BOOZE%'                 then 'Liquor Store'
            when upper(store_name) like '%CORK%'                  then 'Liquor Store'
            when upper(store_name) like '%CELLAR%'                then 'Liquor Store'
            when upper(store_name) like '%BOTTLE%'                then 'Liquor Store'
            when upper(store_name) like '%BEER THIRTY%'           then 'Liquor Store'
            when upper(store_name) like '%BEER ON FLOYD%'         then 'Liquor Store'
            when upper(store_name) like '%BEER CITY%'             then 'Liquor Store'
            when upper(store_name) like '%WINE%'                  then 'Liquor Store'
            when upper(store_name) like '%WHISKEY%'               then 'Liquor Store'
            when upper(store_name) like '%BOOTLEG%'               then 'Liquor Store'
            when upper(store_name) like '%BREWSKI%'               then 'Liquor Store'
            when upper(store_name) like '%LAST CALL%'             then 'Liquor Store'
            when upper(store_name) like '%IGA%'                   then 'Grocery'
            when upper(store_name) like '%SUPER VALU%'            then 'Grocery'
            when upper(store_name) like '%FOODLAND%'              then 'Grocery'
            when upper(store_name) like '%THRIFTWAY%'             then 'Grocery'
            when upper(store_name) like '%SHOP N SAVE%'           then 'Grocery'
            when upper(store_name) like '%FAMILY FARE%'           then 'Grocery'
            when upper(store_name) like '%OSCO%'                  then 'Drug'
            when upper(store_name) like '%APOTHECARY%'            then 'Drug'
            when upper(store_name) like '%GROCERY%'               then 'Grocery'
            when upper(store_name) like '%GROCER%'                then 'Grocery'
            when upper(store_name) like '%MARKET%'                then 'Grocery'
            when upper(store_name) like '%FOOD %'                 then 'Grocery'
            when upper(store_name) like '%FOODS%'                 then 'Grocery'
            when upper(store_name) like '%MART%'                  then 'Grocery'
            when upper(store_name) like '%LIQUOR%'                then 'Liquor Store'
            when upper(store_name) like '%SPIRITS%'               then 'Liquor Store'
            when upper(store_name) like '%SMOK%'                  then 'Specialty/Tobacco'
            when upper(store_name) like '%TOBACCO%'               then 'Specialty/Tobacco'
            when upper(store_name) like '%QUIK %'                 then 'Convenience/Gas'
            when upper(store_name) like '%FUEL%'                  then 'Convenience/Gas'
            when upper(store_name) like '%PETRO%'                 then 'Convenience/Gas'
            when upper(store_name) like '% GAS%'                  then 'Convenience/Gas'
            when upper(store_name) like '%GAS %'                  then 'Convenience/Gas'
            when upper(store_name) like '%STATION%'               then 'Convenience/Gas'
            -- On-Premise additions
            when upper(store_name) like '%PRAIRIE MEADOWS%'       then 'On-Premise'
            when upper(store_name) like '%WATERING HOLE%'         then 'On-Premise'
            when upper(store_name) like '%SPORTS BAR%'            then 'On-Premise'
            when upper(store_name) like '%INN & %'                then 'On-Premise'
            when upper(store_name) like '%INN /%'                 then 'On-Premise'
            -- Liquor Store additions
            when upper(store_name) like '%CENTRAL CITY%'          then 'Liquor Store'
            when upper(store_name) like '%ANOTHER ROUND%'         then 'Liquor Store'
            when upper(store_name) like '%OFFSALE%'               then 'Liquor Store'
            when upper(store_name) like '%OFF SALE%'              then 'Liquor Store'
            when upper(store_name) like '%DISTRIBUT%'             then 'Liquor Store'
            when upper(store_name) like '%SIP &%'                 then 'Liquor Store'
            when upper(store_name) like '%BODEGA%'                then 'Liquor Store'
            when upper(store_name) like '%THE COOLER%'            then 'Liquor Store'
            when upper(store_name) like '%BOONEDOCK%'             then 'Liquor Store'
            -- Convenience/Gas additions
            when upper(store_name) like 'BREW %'                  then 'Convenience/Gas'
            when upper(store_name) like '%OASIS%'                 then 'Convenience/Gas'
            when upper(store_name) like '%EXPRESS%'               then 'Convenience/Gas'
            when upper(store_name) like '%XPRESS%'                then 'Convenience/Gas'
            when upper(store_name) like '%QUICK SHOP%'            then 'Convenience/Gas'
            when upper(store_name) like '%CORNER STOP%'           then 'Convenience/Gas'
            when upper(store_name) like '%CORNER STORE%'          then 'Convenience/Gas'
            when upper(store_name) like '%COUNTRY STORE%'         then 'Convenience/Gas'
            when upper(store_name) like '%GENERAL STORE%'         then 'Convenience/Gas'
            when upper(store_name) like '%CROSSROADS%'            then 'Convenience/Gas'
            -- Grocery additions
            when upper(store_name) like '% FOOD'                  then 'Grocery'
            when upper(store_name) like '%SUPER SAVER%'           then 'Grocery'
            when upper(store_name) like '%SAVE MORE%'             then 'Grocery'
            else                                                       'Unknown'
        end as store_channel,
        address,
        city,
        zip_code,
        store_location,
        county_number,
        county,
        row_number() over (
            partition by store_number
            order by order_date desc nulls last, loaded_at desc
        ) as rn
    from {{ ref('int_iowa_liquor_sales') }}
    where store_number is not null
)

select
    store_number,
    store_name,
    chain,
    store_channel,
    address,
    city,
    zip_code,
    store_location,
    county_number,
    county
from ranked_source
where rn = 1
