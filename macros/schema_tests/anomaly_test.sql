{% test anomaly_test(
    model,
    metric_column,
    time_column,
    group_by,
    threshold,
    lookback_days
) %}

{#-- Convert group_by to list if it’s not already --#}
{% if group_by is string %}
  {% set group_by_columns = [group_by] %}
{% else %}
  {% set group_by_columns = group_by %}
{% endif %}

with base as (
    select
        {{ time_column }} as dt,
        {{ metric_column }} as metric_value,
        {% for col in group_by_columns %}
            {{ col }} as {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ model }}
),

-- yesterday's value
yesterday as (
    select
        dt,
        metric_value,
        {% for col in group_by_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from base
    where dt = date_sub(current_date(), interval 1 day)
),

-- past N days (excluding yesterday)
history as (
    select
        {% for col in group_by_columns %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        avg(metric_value) as moving_avg
    from base
    where dt between date_sub(current_date(), interval {{ lookback_days + 1 }} day)
                 and date_sub(current_date(), interval 2 day)
    group by {{ group_by_columns | join(', ') }}
),

final as (
    select
        y.*,
        h.moving_avg
    from yesterday y
    left join history h
    using ({{ group_by_columns | join(', ') }})
)

select *
from final
where moving_avg is not null
  and (metric_value < moving_avg * (1 - {{ threshold }} / 100.0)
       or metric_value > moving_avg * (1 + {{ threshold }} / 100.0))

{% endtest %}
