## 📊 Anomaly Detection Macro for Metric Monitoring

This reusable dbt test macro detects anomalies by comparing **yesterday’s value** of any metric (e.g., `spends`, `sales`, `orders`) to a configurable **moving average** (e.g., over the past 30 days), flagging deviations beyond a specified **threshold**.



### ✅ Features

- Works with **any numerical metric**
- Compares **yesterday's value** to a configurable **rolling average**
- Supports **multi-column grouping** (e.g., `platform`, `region`)
- Configurable **deviation threshold** (e.g., alert if > ±20%)
- Customizable **lookback window** (e.g., 30, 60, or 90 days)
- Compatible with models using different **date column names**
- Designed for **org-wide reuse across multiple dbt projects**



### 🔧 Installation

Add to your `packages.yml`:

```yml
  - git: "https://github.com/kat-ben/anomaly_tests.git"
    revision: main
```



###  To install the package:
```yml
  dbt deps
```


### Usage Example
In your properties.yml:

```yml
version: 2

models:
  - name: order_metrics
    tests:
      - anomaly_test:
          metric_column: "orders"
          partition_by: "order_date"
          group_by: ["platform", "region"]
          tolerance: 25
          rolling_window: 60
```
⚠️ This test will raise an error if yesterday's value for orders deviates by more than ±25% from the average over the previous 60 days, within each unique combination of platform and region.
