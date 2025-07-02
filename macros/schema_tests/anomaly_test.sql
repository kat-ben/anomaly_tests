{% test anomaly_test(
    model,
    metric_column,
    partition_by="date",
    dimension_column=[],
    threshold=20,
    column_name=None 
) %}

{% set dim_cols = [] %}
{% if dimension_column is string %}
  {% set dim_cols = [dimension_column] %}
{% elif dimension_column is iterable %}
  {% set dim_cols = dimension_column %}
{% endif %}

with base as (
    select
        {{ partition_by }} as dt,
        {{ metric_column }} as metric_value,
        {% for col in dim_cols %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %}
    from {{ model }}
),

yesterday_data as (
    select *
    from base
    where dt = date_sub(current_date(), interval 1 day)
),

past_30_days as (
    select
        {% for col in dim_cols %}
            {{ col }}{% if not loop.last %}, {% endif %}
        {% endfor %},
        avg(metric_value) as moving_avg
    from base
    where dt between date_sub(current_date(), interval 31 day)
                 and date_sub(current_date(), interval 2 day)
    group by {% if dim_cols|length > 0 %}{{ dim_cols | join(', ') }}{% else %}1{% endif %}
)

select y.*
from yesterday_data y
join past_30_days p
on {% for col in dim_cols %}
     y.{{ col }} = p.{{ col }}{% if not loop.last %} and {% endif %}
   {% endfor %}
where p.moving_avg is not null
  and abs(y.metric_value - p.moving_avg) > (p.moving_avg * {{ threshold }} / 100)

{% endtest %}
