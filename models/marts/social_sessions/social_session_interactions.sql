{% set partitions_to_replace = partition_day_intervals(
    date_type="date", 
    n_partitions=2
  )
%}

{{
  config(
    materialized = "incremental",
    incremental_strategy = "insert_overwrite",
    partition_by = {"field": "date_interaction", "data_type": "date"},
    partitions = partitions_to_replace,
    cluster_by = ["game", "country_code"],
    tags = ["social_sessions", "daily"]
  )
}}

with social_instances as (
    select * 
    from {{ ref("social_session_instances") }}
    {% if is_incremental() %}
        where date in ({{ partitions_to_replace | join(",") }})
    {% elif target.name == "local" %}
        where date >= {{ local_days_interval() }}
    {% else %}
        where date >= '2022-01-01'
    {% endif %}
), 

social_creators as (
    select * 
    from {{ ref("social_session_creators") }}
), 

enriched as (
    select 
        instances.date as date_interaction, 
        instances.game, 
        instances.country_code, 
        instances.user_id, 
        creators.acquisition_date as acquisition_date, 
        instances.social_session_id, 
        creators.date as session_creation_date, 
        creators.user_id as session_creator, 
        date_diff(instances.date, creators.date, DAY) as session_age 
    from social_instances as instances 
    left join social_creators as creators 
        on instances.social_session_id = creators.social_session_id 
        and instances.game = creators.game 
        and instances.country_code = creators.country_code
), 

final as (
    select
        * 
    from enriched 
    where not 
        (user_id = session_creator and session_age = 0)
)

select * from final 