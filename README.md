# Data Quality Validation Framework

> *"Bad data is worse than no data. At least with no data, you know you're guessing."*


## Why Data Quality matters?

Being a data enthusiast, my focus has always been clear: **it doesn't matter how complex the system is or how large the scale, data quality is non-negotiable.** 

Every system is as good as the good data that flows into it. It does not matter if we have the most elegant architecture, the fastest cluster, and the most optimized queries if the data coming in is broken. Data quality issues don't just cause bugs. They cause wrong business decisions, eroded trust, and hours of firefighting that should have been prevented at the source.

This is a data quality validation and correction framework, which is production-grade, tested, and ready to be used in any Databricks PySpark pipeline with minimal effort.

## What's in This Repo?

**1. Source Data Validation and cleaning** - Can be utilized in the beginning of a data pipeline to validate source data - catching null values, invalid values, schema issues before anything moves downstream further.

**2. Delta Table Writer** - Once ths ource data is cleaned and validated, this package handle reliable writing process. Evry record is added with audit column which enables tracking when it is written and by whom. This package also takes care surrogate key generation and data validation based on each write strategy.

## Source data validation and cleaning
The Source data validation and cleaning sits at the entry point of data pipeline, it validates and cleans the source DataFrame before its further processed and data is written into data warehouse.This process is driven by metadata registry : ```data_ops.meta.tables``` and ```data_ops.meta.datapoints```

```python
from data_ops.data_quality import validate_source_data

validate_source_data(
    df=<source_dataframe>,
    target_table="<target_table_name>",
    primary_key=[<list_of_primary_keys>],
    validate_primary_key=<True_or_False>,
    validate_column=<True_or_False>,
    drop_nulls={
        "enabled": <True_or_False>,
        "check_config": "<required_non_key_or_custom_columns>",
        "action": "<drop_or_fail_or_warn>"
    }
)
```

### How the validation works

#### Primary Key Validation

If `validate_primary_key` is set to `true`, the validator runs three checks in sequence. 
- First, it confirms the PK column actually exists in the DataFrame — if it does not, a `ValueError` is raised immediately.
- Second, any rows where the PK is null are dropped.
- Third, duplicate PK values are removed. All of this happens before the data moves anywhere downstream.

If `validate_primary_key` is `false`, none of this runs and the data passes through as-is.

#### Column Validation

If `validate_columns` is set to `true`, the validator compares your DataFrame's schema against what is defined in your metadata registry. There are three outcomes depending on what it finds:

- **Missing required columns** — raises a `ValueError` and stops the pipeline immediately
- **Missing optional columns** — logs a warning and lets the pipeline continue
- **Unexpected extra columns** — flagged as a warning so you are aware of what came in, but nothing is blocked

If `validate_columns` is `false`, no schema comparison is done.

#### Null Validation in non key column

If `validate_nulls` is set to `true`, you get the most flexibility of any check. You can either let the validator check nulls against the required non-key columns defined in your metadata, or pass your own custom column list directly. Either way, for each column being checked you set an `action` that controls what happens when a null is found:

- **`drop`** — silently removes the offending rows and continues
- **`warn`** — logs the issue but lets the data through unchanged
- **`fail`** — raises a `ValueError` and stops the pipeline immediately

This means you can run `fail` or `warn` according to the business requirement and the logic stays the same, only the config changes.

Example usage:
```python
# Metadata-driven null check
drop_nulls={
    "check_config": "required_non_key",
    "action": "fail"
}

# Custom column null check
drop_nulls={
    "check_config": "custom_columns",
    "columns": ["name", "status"],
    "action": "warn"
}
```
#### Running Tests
The entire framework is covered by 25 unit tests that run directly in Databricks. To run them, navigate to the data_quality root and run the following command. All 25 pass.

```python
cd data_ops/data_quality
python -m pytest test/test_data_guard.py -v
```
<img width="1381" height="627" alt="Screenshot 2026-03-16 at 17 23 00" src="https://github.com/user-attachments/assets/4ba818a3-a305-469a-a507-f5160f27caef" />

## Delta Table Writer

The Delta Table Writer gives you a single function that handles every write pattern you will need in a Databricks pipeline. Whether you are appending new records, overwriting a table completely, or merging updates into an existing dataset, the interface stays the same. You can also switch on audit columns — which automatically stamps every row with `DWHCreatedDate`, `DWHModifiedDate`, `DWHCreatedBy`, and `DWHModifiedBy` — and surrogate key generation, which adds a unique key to every row without you writing a single line of key logic.

The write_mode parameter accepts append, overwrite, or upsert. For upsert, pass merge_on_key as a list of columns to merge on — the writer handles deduplication of incoming source rows automatically. Setting enable_audit=True adds the DWH audit columns on every write. Setting enable_surrogate_key=True generates a unique surrogate key column on the DataFrame before writing.

**Append method**
```python
from data_ops.delta_table_writer import write_table

# Simple append
write_table(
    df=df,
    target_table="catalog.schema.my_table",
    write_mode="append",
    enable_audit=True,
    enable_surrogate_key=True
)
```

**Upsert method**
```python
# Upsert with merge key
write_table(
    df=df,
    target_table="catalog.schema.my_table",
    write_mode="upsert",
    merge_on_key=["id"],
    enable_audit=True,
    enable_surrogate_key=True
)
```

**Overwrite method**
```python
from data_ops.delta_table_writer import write_table

# Simple append
write_table(
    df=df,
    target_table="catalog.schema.my_table",
    write_mode="overwrite",
    enable_audit=True,
    enable_surrogate_key=True
)
```

#### Running Tests
The entire framework is covered by 9 unit tests that run directly in Databricks. To run them, navigate to the data_quality root and run the following command. All 25 pass.

```python
cd data_ops/delta_table_writer
python -m pytest test/test_delta_table_write.py -v
```

<img width="1381" height="628" alt="Screenshot 2026-03-16 at 17 27 41" src="https://github.com/user-attachments/assets/7b788af4-7335-4dcb-86f6-2355e0785659" />
