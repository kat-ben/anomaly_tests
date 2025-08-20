{% macro anomaly_test(
    model,
    metric_column,
    time_column="date",
    group_by=None,
    tolerance=20,
    rolling_window=30,
    column_name=None
) %}

{% set group_cols = [] %}
{% if group_by is string %}
  {% set group_cols = [group_by] %}
{% elif group_by is iterable %}
  {% set group_cols = group_by %}
{% endif %}

with base as (
    select
        {{ time_column }} as dt,
        {% if group_cols %}
            {% for col in group_cols %}
                {{ col }} as {{ col }},
            {% endfor %}
        {% endif %}
        sum({{ metric_column }}) as metric_value
    from {{ model }}
    where {{ time_column }} between date_sub(current_date(), interval {{ rolling_window + 1 }} day)
                              and current_date()
    group by dt
        {% if group_cols %}
            {% for col in group_cols %}
                , {{ col }}
            {% endfor %}
        {% endif %}
),

recent_value as (
    select *
    from base
    where dt = date_sub(current_date(), interval 1 day)
),

history as (
    select *
    from base
    where dt < date_sub(current_date(), interval 1 day)
),

agg_history as (
    select
        {% if group_cols %}
            {% for col in group_cols %}
                {{ col }},
            {% endfor %}
        {% endif %}
        avg(metric_value) as moving_avg
    from history
    group by
        {% if group_cols %}
            {% for col in group_cols %}
                {{ col }}{% if not loop.last %}, {% endif %}
            {% endfor %}
        {% else %}
            1
        {% endif %}
),

final as (
    select
        r.dt,
        r.metric_value,
        h.moving_avg,
        {% if group_cols %}
            {% for col in group_cols %}
                r.{{ col }}{% if not loop.last %}, {% endif %}
            {% endfor %}
        {% endif %}
    from recent_value r
    left join agg_history h
        {% if group_cols %}
            on 
            {% for col in group_cols %}
                r.{{ col }} = h.{{ col }}{% if not loop.last %} and {% endif %}
            {% endfor %}
        {% else %}
            on 1 = 1
        {% endif %}
)

select *
from final
where moving_avg is not null
  and (
    metric_value > moving_avg * (1 + {{ tolerance }} / 100.0)
    or metric_value < moving_avg * (1 - {{ tolerance }} / 100.0)
  )

{% endmacro %}
