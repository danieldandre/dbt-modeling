{% set partitions_to_replace = partition_day_intervals(
    date_type="date", 
    n_partitions=1
  )
%}

{{
  config(
    materialized = "incremental",
    partition_by = {"field": "date", "data_type": "date"},
    tags = ["social_sessions", "daily"], 
    unique_key = 'id', 
    merge_update_columns = ['id']
  )
}}

with social_session_instances as (
    select 
        *, 
        {{ dbt_utils.surrogate_key(["user_id", "game", "social_session_id"]) }} as id
    from {{ ref("social_session_instances") }}
    {% if is_incremental() %}
        where date in ( {{ partitions_to_replace | join(",") }})
        and {{ dbt_utils.surrogate_key(["user_id", "game", "social_session_id"]) }} not in (select id from {{ this }})
    {% elif target.name == "local" %}
        where date >= {{ local_days_interval() }}
    {% else %}
        where date >= "2022-01-01"
    {% endif %}
), 

play_session_properties as ( --Could be replaced with user_properties at this point
    select distinct
        first_user_id as user_id,
        first_game as game,
        min(acquisition_date) over (
            partition by first_user_id, first_game
        ) as acquisition_date
    from {{ ref("play_session_properties")}}
    where first_channel = 'anonymous_channel'
), 

filtered as (
    select * except (rank)
    from (
        select 
            *, 
            rank() over (partition by game, social_session_id order by time asc) as rank
        from social_session_instances
        where social_session_id is not null
    ) where rank = 1
), 

enriched as (
    select 
        filtered.*, 
        properties.acquisition_date, 
    from filtered
    left join play_session_properties as properties 
        on (filtered.user_id = properties.user_id
        and filtered.game = properties.game)
)

select * 
from enriched