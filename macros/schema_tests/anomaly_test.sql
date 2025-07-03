{% test anomaly_test(
    model,
    metric_column,
    group_by=None,
    threshold,
    lookback_days,
    time_column="Date"
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
        {{ metric_column }} as metric_value,
        {% for col in group_cols %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ model }}
    where {{ time_column }} between date_sub(current_date(), interval {{ lookback_days + 1 }} day)
                              and date_sub(current_date(), interval 1 day)
),

agg_base as (
    select
        {% for col in group_cols %}
            {{ col }},{% endfor %}
        avg(metric_value) as moving_avg
    from base
    group by {% for col in group_cols %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
),

yesterday as (
    select
        {% for col in group_cols %}
            {{ col }},{% endfor %}
        sum({{ metric_column }}) as metric_value
    from {{ model }}
    where {{ time_column }} = date_sub(current_date(), interval 1 day)
    group by {% for col in group_cols %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
),

comparison as (
    select
        y.*,
        b.moving_avg
    from yesterday y
    left join agg_base b using ({{ group_cols | join(", ") }})
)

select *
from comparison
where moving_avg is not null
  and (
    metric_value < (moving_avg * (1 - {{ threshold }} / 100))
    or
    metric_value > (moving_avg * (1 + {{ threshold }} / 100))
)

{% endtest %}
