{{
  config(
    materialized = 'ephemeral',
    tags = ['recon', 'spend']
  )
}}

{% set company = var('company') %}
{% set mart_prefix = company ~ '_dataset_' %}

{% if execute %}

    {% set tables_query %}
        select
            table_catalog as project_id,
            table_schema  as dataset_id,
            table_name
        from `{{ target.project }}.region-asia-southeast1.INFORMATION_SCHEMA.TABLES`
        where
            table_schema like '{{ mart_prefix }}%'
            and lower(table_schema) not like '%recon%'
            and lower(table_name) like '%campaign%'
    {% endset %}

    {% set results = run_query(tables_query) %}
    {% set tables = results.rows if results is not none else [] %}

{% else %}
    {% set tables = [] %}
{% endif %}

{% if tables | length == 0 %}

select
    cast(null as string)  as platform,
    cast(null as string)  as budget_group_1,
    cast(null as string)  as budget_group_2,
    cast(null as string)  as region,

    cast(null as string)  as category_level_1,
    cast(null as string)  as track_group,
    cast(null as string)  as pillar_group,
    cast(null as string)  as content_group,

    cast(null as string)  as objective,
    cast(null as string)  as month,

    cast(null as numeric) as spend,
    cast(null as string)  as objective_status

where false

{% else %}

with union_campaign as (

    {% for row in tables %}

    select
        platform,
        budget_group_1,
        budget_group_2,
        region,

        category_level_1,
        track_group,
        pillar_group,
        content_group,

        objective,
        month,

        spend,
        campaign_status
    from `{{ row[0] }}.{{ row[1] }}.{{ row[2] }}`

    {% if not loop.last %} union all {% endif %}

    {% endfor %}

)

select
    platform,
    budget_group_1,
    budget_group_2,
    region,

    category_level_1,
    track_group,
    pillar_group,
    content_group,

    objective,
    month,

    sum(spend) as spend,

    case
        when max(
            case
                when campaign_status = '🟢' then 1
                else 0
            end
        ) = 1
        then 'active'
        else 'inactive'
    end as objective_status

from union_campaign
group by
    platform,
    budget_group_1,
    budget_group_2,
    region,
    category_level_1,
    track_group,
    pillar_group,
    content_group,
    objective,
    month

{% endif %}