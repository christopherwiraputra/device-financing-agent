with source as (
    select * from {{ ref('device_financing_messy') }}
),

-- first pass: convert all N/A strings to null before any casting
nullified as (
    select
        application_id,
        application_date,
        device_type,
        credit_tier,
        employment_type,
        region,
        defaulted,

        case when trim(cast(device_age_years as varchar))
            in ('N/A', 'NA', 'null', 'NULL', '')
            then null else device_age_years end              as device_age_years_raw,

        case when trim(cast(device_msrp as varchar))
            in ('N/A', 'NA', 'null', 'NULL', '')
            then null else cast(device_msrp as varchar) end  as device_msrp_raw,

        case when trim(cast(credit_score as varchar))
            in ('N/A', 'NA', 'null', 'NULL', '')
            then null else cast(credit_score as varchar) end as credit_score_raw,

        case when trim(cast(monthly_income as varchar))
            in ('N/A', 'NA', 'null', 'NULL', '')
            then null else cast(monthly_income as varchar) end as monthly_income_raw,

        case when trim(cast(dti_ratio as varchar))
            in ('N/A', 'NA', 'null', 'NULL', '')
            then null else cast(dti_ratio as varchar) end    as dti_ratio_raw,

        lease_term_months,
        monthly_payment,
        residual_value,
        existing_monthly_debt,
        credit_utilization,
        prior_leases_completed,
        prior_defaults

    from source
),

cleaned as (
    select
        application_id,

        -- fix future dates
        case
            when cast(application_date as date) > current_date then null
            else cast(application_date as date)
        end as application_date,

        -- standardize device type
        case
            when lower(replace(replace(device_type, ' ', ''), '-', ''))
                in ('iphone14', 'appleiphone14')             then 'iPhone 14'
            when lower(replace(replace(device_type, ' ', ''), '-', ''))
                in ('iphone13', 'appleiphone13')             then 'iPhone 13'
            when lower(replace(replace(device_type, ' ', ''), '-', ''))
                in ('samsunggalaxys23', 'galaxys23', 'samsungs23') then 'Samsung Galaxy S23'
            when lower(replace(replace(device_type, ' ', ''), '-', ''))
                in ('samsunggalaxys22', 'galaxys22', 'samsungs22') then 'Samsung Galaxy S22'
            when lower(replace(replace(device_type, ' ', ''), '-', ''))
                in ('ipadpro', 'appleipadpro')               then 'iPad Pro'
            when lower(replace(replace(device_type, ' ', ''), '-', ''))
                in ('macbookair')                            then 'MacBook Air'
            when lower(replace(replace(device_type, ' ', ''), '-', ''))
                in ('dellxps13', 'dellxps-13')               then 'Dell XPS 13'
            when lower(replace(replace(device_type, ' ', ''), '-', ''))
                in ('microsoftsurfacepro', 'mssurfacepro', 'surfacepro') then 'Microsoft Surface Pro'
            else null
        end as device_type,

        -- clean device age
        case
            when device_age_years_raw is null then null
            when cast(device_age_years_raw as integer) between 0 and 10
                then cast(device_age_years_raw as integer)
            else null
        end as device_age_years,

        -- clean msrp
        case
            when device_msrp_raw is null              then null
            when cast(device_msrp_raw as integer) between 100 and 5000
                then cast(device_msrp_raw as integer)
            else null
        end as device_msrp,

        -- device msrp quality flag
        case
            when device_msrp_raw is null              then 'missing'
            when cast(device_msrp_raw as integer) < 0 then 'negative_value'
            when cast(device_msrp_raw as integer) not between 100 and 5000
                                                      then 'out_of_range'
            else 'valid'
        end as device_msrp_quality_flag,

        -- standardize lease term
        case
            when trim(cast(lease_term_months as varchar)) in ('12', '12 months') then 12
            when trim(cast(lease_term_months as varchar)) in ('24', '24 months', '2 years') then 24
            when trim(cast(lease_term_months as varchar)) in ('36', '36 months', '3 years') then 36
            else null
        end as lease_term_months,

        -- lease term quality flag
        case
            when lease_term_months is null then 'missing'
            when trim(cast(lease_term_months as varchar))
                not in ('12', '12 months', '24', '24 months', '2 years',
                        '36', '36 months', '3 years') then 'invalid_term'
            else 'valid'
        end as lease_term_quality_flag,

        -- clean monthly payment
        case
            when monthly_payment is null then null
            when cast(monthly_payment as float) > 0
                then round(cast(monthly_payment as float), 2)
            else null
        end as monthly_payment,

        -- clean residual value
        case
            when residual_value is null then null
            when cast(residual_value as float) > 0
                then round(cast(residual_value as float), 2)
            else null
        end as residual_value,

        -- standardize credit tier
        case
            when lower(credit_tier) in ('excellent', 'exc') then 'excellent'
            when lower(credit_tier) in ('good', 'gd')       then 'good'
            when lower(credit_tier) in ('fair', 'average')  then 'fair'
            when lower(credit_tier) in ('poor', 'bad', 'low') then 'poor'
            else null
        end as credit_tier,

        -- clean credit score
        case
            when credit_score_raw is null then null
            when cast(credit_score_raw as integer) between 300 and 850
                then cast(credit_score_raw as integer)
            else null
        end as credit_score,

        -- credit score quality flag
        case
            when credit_score_raw is null then 'missing'
            when cast(credit_score_raw as integer) < 0 then 'negative_value'
            when cast(credit_score_raw as integer) not between 300 and 850
                                                    then 'out_of_range'
            else 'valid'
        end as credit_score_quality_flag,

        -- clean monthly income
        case
            when monthly_income_raw is null then null
            when cast(monthly_income_raw as integer) >= 0
                then cast(monthly_income_raw as integer)
            else null
        end as monthly_income,

        -- monthly income quality flag
        case
            when monthly_income_raw is null          then 'missing'
            when cast(monthly_income_raw as integer) < 0 then 'negative_value'
            else 'valid'
        end as monthly_income_quality_flag,

        -- standardize employment type
        case
            when lower(replace(cast(employment_type as varchar), ' ', '_'))
                in ('full_time', 'fulltime', 'ft')      then 'full_time'
            when lower(replace(cast(employment_type as varchar), ' ', '_'))
                in ('part_time', 'parttime', 'pt')      then 'part_time'
            when lower(replace(cast(employment_type as varchar), ' ', '_'))
                in ('self_employed', 'selfemployed', 'freelance') then 'self_employed'
            when lower(cast(employment_type as varchar))
                in ('student')                          then 'student'
            when lower(cast(employment_type as varchar))
                in ('unemployed', 'n/a', 'none')        then 'unemployed'
            else null
        end as employment_type,

        -- clean existing debt
        case
            when existing_monthly_debt is null then null
            when cast(existing_monthly_debt as integer) >= 0
                then cast(existing_monthly_debt as integer)
            else null
        end as existing_monthly_debt,

        -- clean dti
        case
            when dti_ratio_raw is null then null
            when cast(dti_ratio_raw as float) between 0 and 5
                then round(cast(dti_ratio_raw as float), 3)
            else null
        end as dti_ratio,

        -- dti quality flag
        case
            when dti_ratio_raw is null then 'missing'
            when cast(dti_ratio_raw as float) < 0 then 'negative_value'
            when cast(dti_ratio_raw as float) > 5  then 'out_of_range'
            else 'valid'
        end as dti_ratio_quality_flag,

        -- clean utilization
        case
            when credit_utilization is null then null
            when cast(credit_utilization as float) between 0 and 1
                then round(cast(credit_utilization as float), 2)
            else null
        end as credit_utilization,

        -- clean prior leases
        case
            when prior_leases_completed is null then null
            when cast(prior_leases_completed as integer) >= 0
                then cast(prior_leases_completed as integer)
            else null
        end as prior_leases_completed,

        -- clean prior defaults
        case
            when prior_defaults is null then null
            when cast(prior_defaults as integer) >= 0
                then cast(prior_defaults as integer)
            else null
        end as prior_defaults,

        -- standardize region
        case
            when lower(cast(region as varchar)) in ('west', 'w')                   then 'West'
            when lower(cast(region as varchar)) in ('northeast', 'ne', 'north east') then 'Northeast'
            when lower(cast(region as varchar)) in ('southeast', 'se', 'south east') then 'Southeast'
            when lower(cast(region as varchar)) in ('midwest', 'mw', 'mid west')     then 'Midwest'
            when lower(cast(region as varchar)) in ('southwest', 'sw', 'south west') then 'Southwest'
            else null
        end as region,

        defaulted

    from nullified
),

-- remove duplicates
deduped as (
    select *,
        row_number() over (
            partition by application_id
            order by application_date
        ) as row_num
    from cleaned
)

select * exclude (row_num)
from deduped
where row_num = 1
