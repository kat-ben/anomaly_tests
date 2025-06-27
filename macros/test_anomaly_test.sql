{% test anomaly_test(model, metric_column, partition_by="date", dimension_column="ad_channel", threshold=3) %}
  
  with base as (
    select
      {{ dimension_column }} as dimension,
      {{ partition_by }} as dt,
      {{ metric_column }} as metric_value
    from {{ model }}
  ),

  recent as (
    select
      dimension,
      dt,
      metric_value,
      avg(metric_value) over (
        partition by dimension
        order by dt
        rows between 30 preceding and 1 preceding
      ) as moving_avg
    from base
  )

  select *
  from recent
  where abs(metric_value - moving_avg) > (moving_avg * {{ threshold }} / 100)

{% endtest %}

