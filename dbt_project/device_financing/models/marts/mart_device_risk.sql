with enriched as (
    select * from {{ ref('int_device_financing_enriched') }}
),

segment_benchmarks as (
    select
        credit_tier,
        device_type,
        device_value_tier,
        lease_term_months,
        risk_tier,

        -- volume metrics
        count(application_id)                                    as total_applications,
        count(case when defaulted = 1 then 1 end)               as total_defaults,
        count(case when has_prior_default then 1 end)           as prior_default_count,
        count(case when high_dti_flag then 1 end)               as high_dti_count,
        count(case when is_experienced_lessee then 1 end)       as experienced_lessee_count,

        -- default rate
        round(
            count(case when defaulted = 1 then 1 end) * 1.0
            / nullif(count(application_id), 0), 3
        )                                                        as default_rate,

        -- financial metrics
        round(avg(monthly_payment), 2)                          as avg_monthly_payment,
        round(avg(device_msrp), 2)                              as avg_device_msrp,
        round(avg(residual_value), 2)                           as avg_residual_value,
        round(avg(monthly_income), 2)                           as avg_monthly_income,
        round(avg(dti_ratio), 3)                                as avg_dti_ratio,
        round(avg(credit_utilization), 3)                       as avg_credit_utilization,
        round(avg(credit_score), 0)                             as avg_credit_score,
        round(avg(composite_risk_score), 2)                     as avg_composite_risk_score,
        round(avg(payment_to_income_ratio), 3)                  as avg_payment_to_income_ratio,

        -- residual value retention
        round(
            avg(residual_value) / nullif(avg(device_msrp), 0), 3
        )                                                        as avg_residual_value_ratio

    from enriched
    group by 1, 2, 3, 4, 5
)

select * from segment_benchmarks