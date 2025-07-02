{% test test_anomaly_test(
    model,
    metric_column,
    partition_by="date",
    dimension_column=None,
    threshold=3,
    column_name=None 
) %}

{%- set dim_cols = [] -%}
{%- if dimension_column is string -%}
  {%- set dim_cols = [dimension_column] -%}
{%- elif dimension_column is iterable -%}
  {%- set dim_cols = dimension_column -%}
{%- endif -%}

with base as (

  select
    {{ partition_by }} as dt,
    {{ metric_column }} as metric_value,
    {% for col in dim_cols %}
      {{ col }} as {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %}
  from {{ model }}

),

recent as (
  select
    dt,
    metric_value,
    {% for col in dim_cols %}
      {{ col }}{% if not loop.last %},{% endif %}
    {% endfor %},
    avg(metric_value) over (
      partition by {% for col in dim_cols %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
      order by dt
      rows between 30 preceding and 1 preceding
    ) as moving_avg
  from base
)

select *
from recent
where moving_avg is not null
  and abs(metric_value - moving_avg) > (moving_avg * {{ threshold }} / 100)

{% endtest %}
