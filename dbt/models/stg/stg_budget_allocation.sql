{{ 
  config(
    materialized = 'ephemeral',
    tags = ['stg', 'budget', 'campaign']
  ) 
}}

{% set company = var('company') %}
{% set raw_schema = company ~ '_dataset_budget_raw' %}
{% set table_prefix = company ~ '_table_budget_' %}

{% if execute %}

    {% set tables_query %}
        select table_name
        from `{{ target.project }}.{{ raw_schema }}.INFORMATION_SCHEMA.TABLES`
        where table_name like '{{ table_prefix }}m______'
    {% endset %}

    {% set results = run_query(tables_query) %}
    {% set table_names = results.columns[0].values() if results is not none else [] %}

{% else %}
    {% set table_names = [] %}
{% endif %}

{% if table_names | length == 0 %}

select
    cast(null as string)  as department,
    cast(null as string)  as account,

    cast(null as string)  as ma_ngan_sach_cap_1,
    cast(null as string)  as ma_chuong_trinh,
    cast(null as string)  as ma_noi_dung,

    cast(null as numeric) as budget_amount,

    cast(null as int64)   as year,
    cast(null as string)  as month

where false

{% else %}

{% for table_name in table_names %}

select
    split('{{ table_name }}', '_')[offset(3)] as department,
    split('{{ table_name }}', '_')[offset(4)] as account,

    ma_ngan_sach_cap_1,
    ma_chuong_trinh,
    ma_noi_dung,

    budget_amount,

    year,
    month

from `{{ target.project }}.{{ raw_schema }}.{{ table_name }}`

{% if not loop.last %} union all {% endif %}

{% endfor %}

{% endif %}