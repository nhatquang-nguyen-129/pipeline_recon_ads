{{
  config(
    materialized = 'table',
    cluster_by = [
      'budget_group_1',
      'region',
      'category_level_1',
      'track_group',
      'pillar_group',
      'content_group'
    ],
    tags = ['mart', 'ads', 'budget', 'recon']
  )
}}

select
    *
from {{ ref('int_recon_spend') }}