{{
  config(
    materialized = 'ephemeral',
    tags = ['stg', 'budget', 'allocation']
  )
}}

{% set company = var('company') %}
{% set raw_schema = company ~ '_dataset_budget_raw' %}
{% set table_prefix = company ~ '_table_budget_m' %}

{% if execute %}
    {% set tables_query %}
        select table_name
        from `{{ target.project }}.{{ raw_schema }}.INFORMATION_SCHEMA.TABLES`
        where table_name like '{{ table_prefix }}____'
    {% endset %}

    {% set results = run_query(tables_query) %}
    {% set table_names = results.columns[0].values() if results is not none else [] %}
{% else %}
    {% set table_names = [] %}
{% endif %}

{% if table_names | length == 0 %}

select
    cast(null as string)  as budget_group_1,
    cast(null as string)  as budget_group_2,
    cast(null as string)  as region,

    cast(null as string)  as category_level_1,
    cast(null as string)  as track_group,
    cast(null as string)  as pillar_group,
    cast(null as string)  as content_group,

    cast(null as string)  as month,
    cast(null as date)    as start_date,
    cast(null as date)    as end_date,

    cast(null as string)  as platform,
    cast(null as string)  as objective,

    cast(null as numeric) as initial_budget,
    cast(null as numeric) as adjusted_budget,
    cast(null as numeric) as additional_budget

where false

{% else %}

{% for table_name in table_names %}

select
    budget_group_1,
    budget_group_2,
    region,

    category_level_1,
    track_group,
    pillar_group,
    content_group,

    month,

    date(start_date) as start_date,
    date(end_date)   as end_date,

    platform,
    objective,

    initial_budget,
    adjusted_budget,
    additional_budget,
    actual_budget

from `{{ target.project }}.{{ raw_schema }}.{{ table_name }}`
{% if not loop.last %} union all {% endif %}

{% endfor %}

{% endif %}