{% set partitions_to_replace = partition_day_intervals(
    date_type="date"
  )
%}

{{
  config(
    materialized = "incremental",
    incremental_strategy = "insert_overwrite", 
    partitions = partitions_to_replace,
    partition_by = {"field": "date", "data_type": "date"},
    cluster_by = ["game", "country_code"], 
    tags = ["social_sessions", "daily"]
  )
}}

with stg_channel_pubsuptopic as (
    select 
        *
    from {{ ref("stg_channel_pubsuptopic") }} 
    {% if is_incremental() %}
        where date in ( {{ partitions_to_replace | join(",") }})
    {% elif target.name == "local" %}
        where date >= {{ local_days_interval() }}
    {% else %}
        where date >= "2022-01-01"
    {% endif %}
), 

filtered as (
    select distinct * except (rank)
    from (
        select 
            time, 
            user_id, 
            game, 
            country_code, 
            social_session_id, 
            rank() over (partition by date, game, social_session_id, user_id order by time asc) as rank
        from stg_channel_facebook_instant
        where 
            social_session_id is not null 
            and event in ('game_play_start', 'context_change', 'player_online_message', 'social_display_show', 'social_plugin_invite')
    ) where rank = 1
), 

final as (
    select 
        date(time) as date, 
        *
    from filtered
)

select * 
from final 