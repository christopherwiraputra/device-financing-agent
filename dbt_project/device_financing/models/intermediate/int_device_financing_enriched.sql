with staged as (
    select * from {{ ref('stg_device_financing') }}
),

-- compute segment averages for imputation
segment_averages as (
    select
        credit_tier,
        employment_type,
        round(avg(credit_score), 0)        as avg_credit_score,
        round(avg(dti_ratio), 3)           as avg_dti_ratio,
        round(avg(credit_utilization), 2)  as avg_credit_utilization,
        round(avg(monthly_income), 0)      as avg_monthly_income
    from staged
    where credit_score is not null
      and dti_ratio is not null
      and credit_utilization is not null
      and monthly_income is not null
    group by 1, 2
),

-- device msrp lookup (no imputation -- known values per device)
device_msrp_reference as (
    select 'iPhone 14'              as device_type, 799  as reference_msrp union all
    select 'iPhone 13'              as device_type, 599  as reference_msrp union all
    select 'Samsung Galaxy S23'     as device_type, 849  as reference_msrp union all
    select 'Samsung Galaxy S22'     as device_type, 649  as reference_msrp union all
    select 'iPad Pro'               as device_type, 1099 as reference_msrp union all
    select 'MacBook Air'            as device_type, 1299 as reference_msrp union all
    select 'Dell XPS 13'            as device_type, 1199 as reference_msrp union all
    select 'Microsoft Surface Pro'  as device_type, 1599 as reference_msrp
),

joined as (
    select
        s.*,
        sa.avg_credit_score,
        sa.avg_dti_ratio,
        sa.avg_credit_utilization,
        sa.avg_monthly_income,
        d.reference_msrp
    from staged s
    left join segment_averages sa
        on s.credit_tier = sa.credit_tier
        and s.employment_type = sa.employment_type
    left join device_msrp_reference d
        on s.device_type = d.device_type
),

enriched as (
    select
        application_id,
        application_date,
        device_type,
        device_age_years,

        -- use reference msrp if raw is missing
        coalesce(device_msrp, reference_msrp)       as device_msrp,
        case
            when device_msrp is null then true
            else false
        end                                          as device_msrp_imputed,

        lease_term_months,
        monthly_payment,
        residual_value,
        credit_tier,

        -- original cleaned values
        credit_score,
        dti_ratio,
        monthly_income,
        credit_utilization,

        -- imputed values (used by agent)
        coalesce(credit_score, avg_credit_score)     as credit_score_imputed,
        coalesce(dti_ratio, avg_dti_ratio)           as dti_ratio_imputed,
        coalesce(monthly_income, avg_monthly_income) as monthly_income_imputed,
        coalesce(credit_utilization, avg_credit_utilization) as credit_utilization_imputed,

        -- imputation flags
        case when credit_score is null then true else false end
                                                     as credit_score_imputed_flag,
        case when dti_ratio is null then true else false end
                                                     as dti_ratio_imputed_flag,
        case when monthly_income is null then true else false end
                                                     as monthly_income_imputed_flag,
        case when credit_utilization is null then true else false end
                                                     as credit_utilization_imputed_flag,

        -- quality flags from staging
        credit_score_quality_flag,
        dti_ratio_quality_flag,
        monthly_income_quality_flag,
        device_msrp_quality_flag,
        lease_term_quality_flag,

        employment_type,
        existing_monthly_debt,
        prior_leases_completed,
        prior_defaults,
        region,
        defaulted,

        -- risk tier based on imputed credit score
        case
            when coalesce(credit_score, avg_credit_score) >= 750 then 'low_risk'
            when coalesce(credit_score, avg_credit_score) >= 670 then 'moderate_risk'
            when coalesce(credit_score, avg_credit_score) >= 580 then 'elevated_risk'
            else 'high_risk'
        end as risk_tier,

        -- dti risk flag using imputed value
        case
            when coalesce(dti_ratio, avg_dti_ratio) > 0.5 then true
            else false
        end as high_dti_flag,

        -- payment to income ratio using imputed income
        case
            when coalesce(monthly_income, avg_monthly_income) > 0
            and monthly_payment is not null
            then round(
                monthly_payment / coalesce(monthly_income, avg_monthly_income), 3)
            else null
        end as payment_to_income_ratio,

        -- prior default flag
        case
            when prior_defaults > 0 then true
            else false
        end as has_prior_default,

        -- experienced lessee
        case
            when prior_leases_completed >= 2 then true
            else false
        end as is_experienced_lessee,

        -- device value tier using imputed msrp
        case
            when coalesce(device_msrp, reference_msrp) >= 1200 then 'premium'
            when coalesce(device_msrp, reference_msrp) >= 700  then 'mid_range'
            else 'budget'
        end as device_value_tier,

        -- composite risk score using imputed values
        round(
            (case
                when coalesce(credit_score, avg_credit_score) >= 750 then 0
                when coalesce(credit_score, avg_credit_score) >= 670 then 1
                when coalesce(credit_score, avg_credit_score) >= 580 then 2
                else 3
            end) * 0.4
            + (case
                when coalesce(dti_ratio, avg_dti_ratio) > 0.5  then 2
                when coalesce(dti_ratio, avg_dti_ratio) > 0.36 then 1
                else 0
            end) * 0.3
            + (case when prior_defaults > 0 then 2 else 0 end) * 0.2
            + (case
                when employment_type = 'unemployed' then 2
                when employment_type = 'student'    then 1
                else 0
            end) * 0.1
        , 2) as composite_risk_score

    from joined
)

select * from enriched
