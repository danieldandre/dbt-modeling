{% set partitions_to_replace = partition_day_intervals(
    date_type="date"
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

with social_session_interactions as (
    select * 
    from {{ ref("social_session_interactions") }} 

    {% if is_incremental() %}
        where date_interaction in ({{ partitions_to_replace | join(",") }})
    {% elif target.name == "local" %}
        where date_interaction >= {{ local_days_interval() }}
    {% endif %}
), 

channel_campaign_users as (
    select distinct 
        cohort, 
        campaign_name, 
        adset_id, 
        ad_id, 
        ad_name, 
        country_code, 
        game, 
        user_id 
    from {{ ref("user_acquisition_labelling_channel") }}
), 

{% set days = [0, 1, 2, 3, 4, 5, 6, 7] %}

final as (
    select 
        interactions.date_interaction, 
        interactions.country_code, 
        interactions.game,
        interactions.session_creator,
        interactions.social_session_id,
        -- Appending campaign info
        channel.campaign_name,
        channel.adset_id,
        channel.ad_id,
        channel.ad_name,
        -- New user acquisitions
        count(distinct if(acquisition_date = date_interaction, interactions.user_id, null)) as total_social_installs_new,
        {% for day in days %}
            count(distinct if(session_age = {{day}} and acquisition_date = date_interaction, interactions.user_id, null)) as total_users_age{{day}}, 
        {% endfor %}

        -- Social retargets
        count(distinct interactions.user_id) as total_social_retarget,
        {% for day in days %}
            count(distinct if(session_age = {{day}}, interactions.user_id, null)) as d{{day}}_social_retarget
        {% endfor %}
        
    from social_session_interactions as interactions
    left join channel_campaign_users as channel
        on interactions.session_creator = channel.user_id 
        and interactions.country_code = channel.country_code
    where channel.campaign_name is not null
    group by 
        interactions.date_interaction, 
        interactions.country_code, 
        interactions.game, 
        interactions.session_creator, 
        interactions.social_session_id, 
        channel.campaign_name, 
        channel.adset_id, 
        channel.ad_id, 
        channel.ad_name
)

select * 
from final