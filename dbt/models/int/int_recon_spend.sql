{{
  config(
    materialized = 'ephemeral',
    tags = ['recon', 'spend']
  )
}}

with spend as (

    select
        budget_group_1,
        budget_group_2,
        region,

        category_level_1,
        track_group,
        pillar_group,
        content_group,

        platform,
        objective,

        month,
        year,

        spend,

        objective_status

    from {{ ref('stg_ads_spend') }}
),

budget as (

    select
        budget_group_1,
        budget_group_2,
        region,

        category_level_1,
        track_group,
        pillar_group,
        content_group,

        platform,
        objective,

        month,
        year,

        start_date,
        end_date,

        initial_budget,
        adjusted_budget,
        additional_budget,
        actual_budget,

        grouped_marketing_budget,
        grouped_supplier_budget,
        grouped_store_budget,
        grouped_customer_budget,
        grouped_recruitment_budget,

        total_effective_time,
        total_passed_time

    from {{ ref('stg_budget_allocation') }}
)

select
    coalesce(b.budget_group_1, s.budget_group_1)     as budget_group_1,
    coalesce(b.budget_group_2, s.budget_group_2)     as budget_group_2,
    coalesce(b.region, s.region)                     as region,

    coalesce(b.category_level_1, s.category_level_1) as category_level_1,
    coalesce(b.track_group, s.track_group)           as track_group,
    coalesce(b.pillar_group, s.pillar_group)         as pillar_group,
    coalesce(b.content_group, s.content_group)       as content_group,

    coalesce(b.platform, s.platform)                 as platform,
    coalesce(b.objective, s.objective)               as objective,

    coalesce(b.month, s.month)                       as month,
    coalesce(b.year, s.year)                         as year,

    b.initial_budget,
    b.adjusted_budget,
    b.additional_budget,
    b.actual_budget,

    b.grouped_marketing_budget,
    b.grouped_supplier_budget,
    b.grouped_store_budget,
    b.grouped_customer_budget,
    b.grouped_recruitment_budget,

    b.start_date,
    b.end_date,
    b.total_effective_time,
    b.total_passed_time,

    s.spend,
    s.objective_status,

    case
        when coalesce(s.spend, 0) > 0
            and coalesce(b.actual_budget, 0) = 0
            and lower(coalesce(s.objective_status, '')) = 'active'
        then '🔴 Spend without Budget'

        when coalesce(s.spend, 0) > 0
            and coalesce(b.actual_budget, 0) = 0
            and lower(coalesce(s.objective_status, '')) != 'active'
        then '⚪ Spend without Budget'

        when coalesce(b.actual_budget, 0) = 0
        then '🚫 No Budget'

        when coalesce(b.actual_budget, 0) > 0
            and current_date() < b.start_date
        then '🕓 Not Yet Started'

        when coalesce(b.actual_budget, 0) > 0
            and current_date() >= b.start_date
            and coalesce(s.spend, 0) = 0
            and (s.objective_status is null or trim(s.objective_status) = '')
            and date_diff(current_date(), b.start_date, day) <= 3
        then '⚪ Not Set'

        when coalesce(b.actual_budget, 0) > 0
            and current_date() between b.start_date and b.end_date
            and coalesce(s.spend, 0) = 0
            and (s.objective_status is null or trim(s.objective_status) = '')
            and date_diff(current_date(), b.start_date, day) > 3
        then '⚠️ Delayed'

        when coalesce(b.actual_budget, 0) > 0
            and current_date() > b.end_date
            and coalesce(s.spend, 0) = 0
        then '🔒 Ended without Spend'

        when coalesce(b.actual_budget, 0) > 0
            and coalesce(s.spend, 0) >= b.actual_budget * 1.01
            and lower(coalesce(s.objective_status, '')) = 'active'
        then '🔴 Over Budget'

        when coalesce(b.actual_budget, 0) > 0
            and coalesce(s.spend, 0) >= b.actual_budget * 1.01
            and lower(coalesce(s.objective_status, '')) != 'active'
        then '⚪ Over Budget'


        when coalesce(b.actual_budget, 0) > 0
            and safe_divide(coalesce(s.spend, 0), b.actual_budget) > 0.99
            and coalesce(s.spend, 0) < b.actual_budget * 1.01
        then '🔵 Completed'

        when coalesce(b.actual_budget, 0) > 0
            and lower(coalesce(s.objective_status, '')) = 'active'
            and safe_divide(coalesce(s.spend, 0), b.actual_budget) between 0.95 and 0.99
        then '🟢 Near Completion'

        when coalesce(b.actual_budget, 0) > 0
            and lower(coalesce(s.objective_status, '')) = 'active'
            and safe_divide(coalesce(s.spend, 0), b.actual_budget) < 0.95
            and date_diff(b.end_date, b.start_date, day) > 0
            and safe_divide(coalesce(s.spend, 0), b.actual_budget)
                < safe_divide(
                    date_diff(current_date(), b.start_date, day),
                    date_diff(b.end_date, b.start_date, day)
                ) - 0.3
        then '📉 Low Spend'

        when coalesce(b.actual_budget, 0) > 0
            and lower(coalesce(s.objective_status, '')) = 'active'
            and safe_divide(coalesce(s.spend, 0), b.actual_budget) < 0.95
            and date_diff(b.end_date, b.start_date, day) > 0
            and safe_divide(coalesce(s.spend, 0), b.actual_budget)
                > safe_divide(
                    date_diff(current_date(), b.start_date, day),
                    date_diff(b.end_date, b.start_date, day)
                ) + 0.3
        then '📈 High Spend'

        when coalesce(b.actual_budget, 0) > 0
            and coalesce(s.spend, 0) > 0
            and lower(coalesce(s.objective_status, '')) != 'active'
            and coalesce(s.spend, 0) < b.actual_budget * 0.99
        then '⚪ Off'

        when coalesce(b.actual_budget, 0) > 0
            and coalesce(s.spend, 0) > 0
            and lower(coalesce(s.objective_status, '')) = 'active'
        then '🟢 In Progress'

        else '❓ Unrecognized'
    end as status

from budget b
full outer join spend s
    on  b.budget_group_1   = s.budget_group_1
    and b.budget_group_2   = s.budget_group_2
    and b.region           = s.region
    and b.category_level_1 = s.category_level_1
    and b.track_group      = s.track_group
    and b.pillar_group     = s.pillar_group
    and b.content_group    = s.content_group
    and b.platform         = s.platform
    and b.objective        = s.objective
    and b.month            = s.month
    and b.year             = s.year