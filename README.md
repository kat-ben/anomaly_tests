## 📊 Anomaly Detection Macro for Metric Monitoring

This reusable dbt test macro detects anomalies by comparing **yesterday’s value** of any metric (e.g., `spends`, `sales`, `orders`) to a configurable **moving average** (e.g., over the past 30 days), flagging deviations beyond a specified **threshold**.

---

### ✅ Features

- Works with **any numerical metric**
- Compares **yesterday's value** to a configurable **rolling average**
- Supports **multi-column grouping** (e.g., `platform`, `region`)
- Configurable **deviation threshold** (e.g., alert if > ±20%)
- Customizable **lookback window** (e.g., 30, 60, or 90 days)
- Compatible with models using different **date column names**
- Designed for **org-wide reuse across multiple dbt projects**

---

### 🔧 Installation

Add to your `packages.yml`:

```yml
packages:
  - git: "https://github.com/kat-ben/anomaly_tests.git"
    revision: main

###  Run
``dbt deps

### Usage Example
In your properties.yml:

```models:
  - name: order_metrics
    description: "Model containing daily order data"
    tests:
      - anomaly_test:
          metric_column: "orders"
          partition_by: "order_date"
          dimension_column: ["platform", "region"]
          threshold: 25
          lookback_days: 60



