{% set partitions_to_scan = partition_day_intervals(
    date_type="date"
  ) 
%}

{{
  config(
    materialized = 'incremental',
    partition_by = {"field": "acquisition_date", "data_type": "date"},
    tags = ["channels", "daily"], 
    unique_key = 'id',
    merge_update_columns = ['id'],
  )
}}

with stg_channel_pubsubtopic1 as (
    
    select
        {{ dbt_utils.surrogate_key(["play_session_id", "channel"]) }} as id,
        date,
        time,
        global_user_id,
        cast(user_id as string) as user_id,
        country_code,
        country_name,
        play_session_id,
        channel,
        game,
        app_version,
        app_build, 
        event,
        web_url,
        entry_point, 
        REGEXP_EXTRACT(event, 'ab_(.*)')  as ab_test,
        REGEXP_EXTRACT(_params, CONCAT(r'"', event, '":"(.*?)"')) as ab_group
    from {{ ref("stg_channel_pubsubtopic1") }}
    {% if is_incremental() %}
        where date in ({{ partitions_to_scan | join(",") }})
        and {{ dbt_utils.surrogate_key(["play_session_id", "channel"]) }} not in (
            select id from {{ this }}
        )
    {% elif target.name == "local" %}
        where date >= {{ local_days_interval() }}
    {% else %}
        where date >= date('2020-09-10')
    {% endif %}
),

stg_channel_pubsubtopic2 as (
    
    select
        {{ dbt_utils.surrogate_key(["play_session_id", "channel"]) }} as id,
        date,
        time,
        global_user_id,
        cast(user_id as string) as user_id,
        country_code,
        country_name,
        play_session_id,
        channel,
        game,
        app_version,
        app_build, 
        event,
        web_url,
        entry_point, 
        REGEXP_EXTRACT(event, 'ab_(.*)')  as ab_test,
        REGEXP_EXTRACT(_params, CONCAT(r'"', event, '":"(.*?)"')) as ab_group
    from {{ ref("stg_channel_pubsubtopic2") }}
    {% if is_incremental() %}
        where date in ({{ partitions_to_scan | join(",") }})
        and {{ dbt_utils.surrogate_key(["play_session_id", "channel"]) }} not in (
            select id from {{ this }}
        )
    {% elif target.name == "local" %}
        where date >= {{ local_days_interval() }}
    {% else %}
        where date >= date('2020-09-10')
    {% endif %}
),

stg_channel_pubsubtopic3 as (
    
    select
        {{ dbt_utils.surrogate_key(["play_session_id", "channel"]) }} as id,
        date,
        time,
        anonymous_user_id as global_user_id,
        cast(user_id as string) as user_id,
        country_code,
        country_name,
        play_session_id,
        channel,
        game,
        app_version,
        app_build, 
        event,
        web_url,
        entry_point, 
        REGEXP_EXTRACT(event, 'ab_(.*)')  as ab_test,
        REGEXP_EXTRACT(_params, CONCAT(r'"', event, '":"(.*?)"')) as ab_group
    from {{ ref("stg_channel_pubsubtopic2") }}
    {% if is_incremental() %}
        where date in ({{ partitions_to_scan | join(",") }})
        and {{ dbt_utils.surrogate_key(["play_session_id", "channel"]) }} not in (
            select id from {{ this }}
        )
    {% elif target.name == "local" %}
        where date >= {{ local_days_interval() }}
    {% else %}
        where date >= date('2022-04-01') -- krunker release
    {% endif %}

),

stg_channel_pubsubtopic4 as (
    
    select
        {{ dbt_utils.surrogate_key(["play_session_id", "channel"]) }} as id, 
        date,
        time,
        global_user_id,
        cast(user_id as string) as user_id,
        country_code,
        country_name,
        play_session_id,
        channel,
        game,
        app_version,
        app_build, 
        event,
        web_url,
        entry_point, 
        REGEXP_EXTRACT(event, 'ab_(.*)')  as ab_test,
        REGEXP_EXTRACT(_params, CONCAT(r'"', event, '":"(.*?)"')) as ab_group
    from {{ ref("stg_channel_pubsubtopic4") }}
    {% if is_incremental() %}
        where date in ({{ partitions_to_scan | join(",") }})
        and {{ dbt_utils.surrogate_key(["play_session_id", "channel"]) }} not in (
            select id from {{ this }}
        )
    {% elif target.name == "local" %}
        where date >= {{ local_days_interval() }}
    {% else %}
        where date >= date('2020-09-10')
    {% endif %}

),

stg_channel_pubsubtopic5 as (
    
    select
        {{ dbt_utils.surrogate_key(["play_session_id", "channel"]) }} as id,
        date,
        time,
        global_user_id,
        cast(user_id as string) as user_id,
        country_code,
        country_name,
        play_session_id,
        channel,
        game,
        app_version,
        app_build, 
        event,
        web_url,
        facebook_entrypoint as entry_point, 
        REGEXP_EXTRACT(event, 'ab_(.*)')  as ab_test,
        REGEXP_EXTRACT(_params, CONCAT(r'"', event, '":"(.*?)"')) as ab_group
    from {{ ref("stg_channel_pubsubtopic5") }}
    {% if is_incremental() %}
        where date in ({{ partitions_to_scan | join(",") }})
        and {{ dbt_utils.surrogate_key(["play_session_id", "channel"]) }} not in (
            select id from {{ this }}
        )
    {% elif target.name == "local" %}
        where date >= {{ local_days_interval() }}
    {% else %}
        where date >= date('2020-09-10')
    {% endif %}

),

combined as (
    select * from stg_channel_pubsubtopic1
    union all
    select * from stg_channel_pubsubtopic2
    union all
    select * from stg_channel_pubsubtopic3
    union all
    select * from stg_channel_pubsubtopic4
    union all
    select * from stg_channel_pubsubtopic5
),

filtered as (
    select * from combined
    where event in (
        "session_start",
        "local_notification_open", 
        "device_info",
        "entry_point",
        "game_play_start", 
        "iap_request_payment_success"
    ) or event like 'ab_%'
),

properties as (
    select
        id,
        play_session_id,
        first_value(date) over (
            partition by play_session_id order by time
            rows between unbounded preceding and unbounded following
        ) as acquisition_date,
        first_value(time) over (
            partition by play_session_id order by time
            rows between unbounded preceding and unbounded following
        ) as acquisition_time,
        first_value(country_code) over (
            partition by play_session_id order by time
            rows between unbounded preceding and unbounded following
        ) as first_country_code,
        first_value(country_name) over (
            partition by play_session_id order by time
            rows between unbounded preceding and unbounded following
        ) as first_country_name,
        first_value(app_version) over (
            partition by play_session_id order by time
            rows between unbounded preceding and unbounded following
        ) as first_app_version,
        first_value(app_build) over (
            partition by play_session_id order by time
            rows between unbounded preceding and unbounded following
        ) as first_app_build,
        first_value(global_user_id) over (
            partition by play_session_id order by time
            rows between unbounded preceding and unbounded following
        ) as first_global_user_id,
        first_value(channel) over (
            partition by play_session_id order by time
            rows between unbounded preceding and unbounded following
        ) as first_channel,
        first_value(game) over (
            partition by play_session_id order by time
            rows between unbounded preceding and unbounded following
        ) as first_game,
        first_value(user_id ignore nulls) over (
            partition by play_session_id order by time
            rows between unbounded preceding and unbounded following
        ) as first_user_id,
        first_value(if(event in ('device_info', 'entry_point'), time, null) ignore nulls) over (
            partition by play_session_id order by time 
            rows between unbounded preceding and unbounded following
        ) as entry_timestamp,
        first_value(if(event = 'iap_request_payment_success', time, null) ignore nulls) over (
            partition by play_session_id order by time 
            rows between unbounded preceding and unbounded following
        ) as first_purchase_time,
        first_value(if(event = 'game_play_start', time, null) ignore nulls) over (
            partition by play_session_id order by time 
            rows between unbounded preceding and unbounded following
        ) as game_play_start_timestamp,
        first_value(if(event = 'local_notification_open', 1, null) ignore nulls) over (
            partition by play_session_id order by time 
            rows between unbounded preceding and unbounded following
        ) as entry_point_local_notification_open,
        lower(max(web_url) over (
            partition by play_session_id order by time 
            rows between unbounded preceding and unbounded following)
        ) as web_url,
        first_value(entry_point ignore nulls) over (
            partition by play_session_id order by time 
            rows between unbounded preceding and unbounded following
        ) as first_entry_point,
        lower(max(ab_test) over (
            partition by play_session_id order by time 
            rows between unbounded preceding and unbounded following)
        ) as ab_test,
        lower(max(ab_group) over (
            partition by play_session_id order by time 
            rows between unbounded preceding and unbounded following)
        ) as ab_group
    from filtered
)

select distinct * from properties