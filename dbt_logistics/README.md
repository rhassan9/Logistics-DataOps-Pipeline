# dbt Logistics — Analytics Engineering Layer

This dbt project transforms raw CSV source tables (loaded by the Python ETL pipeline) into a **Kimball Star Schema** optimized for Power BI Import Mode.

## Quick Start
```bash
dbt debug       # Verify PostgreSQL connection
dbt run         # Build all staging views and mart tables
dbt test        # Run data quality tests
dbt docs generate && dbt docs serve   # Browse interactive documentation
```

## Model Layers
| Layer | Schema | Materialization | Purpose |
|---|---|---|---|
| `models/staging/stg_*` | `staging` | View | Type casting and column standardization |
| `models/marts/dim_*` | `marts` | Table | Star Schema Dimension tables |
| `models/marts/fact_*` | `marts` | Table | Star Schema Fact tables with calculated columns |
