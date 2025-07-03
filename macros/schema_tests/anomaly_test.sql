{% test anomaly_test(
    model,
    metric_column,
    threshold,
    lookback_days,
    dimension_column=None,
    time_column="Date",
    column_name=None
) %}

{# Convert dimension_column to list if it's a string #}
{% set group_by = [] %}
{% if dimension_column is string %}
  {% set group_by = [dimension_column] %}
{% elif dimension_column is iterable %}
  {% set group_by = dimension_column %}
{% endif %}

-- Base: Aggregate metric_value at time + dimension level
with base as (
    select
        {{ time_column }} as dt,
        {% for col in group_by %}
            {{ col }},
        {% endfor %}
        sum({{ metric_column }}) as metric_value
    from {{ model }}
    where {{ time_column }} between date_sub(current_date(), interval {{ lookback_days + 1 }} day)
                              and date_sub(current_date(), interval 1 day)
    group by dt, {% for col in group_by %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
),

-- Moving average for each dimension group over the lookback window (excluding yesterday)
moving_avg_calc as (
    select
        {% for col in group_by %}
            {{ col }},
        {% endfor %}
        avg(metric_value) as moving_avg
    from base
    where dt between date_sub(current_date(), interval {{ lookback_days }} day)
               and date_sub(current_date(), interval 2 day)
    group by {% for col in group_by %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
),

-- Yesterday's values
yesterday_data as (
    select
        {% for col in group_by %}
            b.{{ col }},
        {% endfor %}
        b.metric_value,
        m.moving_avg,
        b.dt
    from base b
    left join moving_avg_calc m
    on {% for col in group_by %}
           b.{{ col }} = m.{{ col }}{% if not loop.last %} and {% endif %}
       {% endfor %}
    where b.dt = date_sub(current_date(), interval 1 day)
)

-- Return rows where deviation exceeds threshold
select *
from yesterday_data
where moving_avg is not null
  and (metric_value < moving_avg * (1 - {{ threshold }} / 100.0)
       or metric_value > moving_avg * (1 + {{ threshold }} / 100.0))

{% endtest %}
