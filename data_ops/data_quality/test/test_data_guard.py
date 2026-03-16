# test_data_guard.py

import pytest
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from checks.primarykey import validate_pk
from checks.columnValidation import validate_column
from checks.nullvalidation import clean_non_key_null


# ─────────────────────────────────────────────────────
# Shared SparkSession
# ─────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    return SparkSession.getActiveSession()


# ─────────────────────────────────────────────────────
# Test metadata constants
# ─────────────────────────────────────────────────────

TEST_TABLE_NAME = "test_orders_dqtest"
TEST_TABLE_ID = 2


# ════════════════════════════════════════════════════════
# 1. PRIMARY KEY TESTS
# ════════════════════════════════════════════════════════

def test_pk_missing_column_raises(spark):
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    with pytest.raises(ValueError, match="missing in source Dataframe"):
        validate_pk(df, "wrong_col")


def test_pk_null_rows_dropped(spark):
    df = spark.createDataFrame(
        [(1, "Alice"), (None, "Bob"), (3, "Charlie")],
        ["id", "name"]
    )
    result = validate_pk(df, "id")
    assert result.filter(F.col("id").isNull()).count() == 0


def test_pk_duplicates_dropped(spark):
    df = spark.createDataFrame(
        [(1, "Alice"), (1, "Dup"), (2, "Bob")],
        ["id", "name"]
    )
    result = validate_pk(df, "id")
    assert result.count() == 2


def test_pk_clean_df_unchanged(spark):
    df = spark.createDataFrame(
        [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
        ["id", "name"]
    )
    result = validate_pk(df, "id")
    assert result.count() == 3


def test_pk_null_and_duplicate_combined(spark):
    df = spark.createDataFrame(
        [(1, "Alice"), (1, "Dup"), (None, "Bob"), (3, "Charlie")],
        ["id", "name"]
    )
    result = validate_pk(df, "id")
    assert result.count() == 2
    assert result.filter(F.col("id").isNull()).count() == 0


# ════════════════════════════════════════════════════════
# 2. COLUMN VALIDATION TESTS
# ════════════════════════════════════════════════════════

def test_col_table_not_in_metadata_warns(spark):
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    result = validate_column(df, "non_existent_table_xyz")
    assert result is None


def test_col_missing_required_columns_raises(spark):
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])  # missing 'status'
    with pytest.raises(ValueError, match="missing required mandatory columns"):
        validate_column(df, TEST_TABLE_NAME)


def test_col_all_required_columns_present(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "Active")],
        ["id", "name", "status"]
    )
    result = validate_column(df, TEST_TABLE_NAME)
    assert result is None


def test_col_missing_optional_columns_no_raise(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "Active")],
        ["id", "name", "status"]  # missing optional 'description'
    )
    try:
        validate_column(df, TEST_TABLE_NAME)
    except ValueError:
        pytest.fail("Should not raise for missing optional columns")


def test_col_unexpected_columns_no_raise(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "Active", "extra")],
        ["id", "name", "status", "extra_col"]
    )
    try:
        validate_column(df, TEST_TABLE_NAME)
    except Exception as e:
        pytest.fail(f"Should not raise for extra columns: {e}")


# ════════════════════════════════════════════════════════
# 3. NULL VALIDATION TESTS
# ════════════════════════════════════════════════════════

# ── config None ─────────────────────────────────────────

def test_null_config_none_returns_df(spark):
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    result = clean_non_key_null(df, TEST_TABLE_NAME, config=None)
    assert result.count() == df.count()


# ── required_non_key: drop ──────────────────────────────

def test_null_required_non_key_drop(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "Active"), (2, None, "Active"), (3, "Charlie", None)],
        ["id", "name", "status"]
    )
    config = {"check_config": "required_non_key", "action": "drop"}
    result = clean_non_key_null(df, TEST_TABLE_NAME, config)
    assert result.count() == 1


# ── required_non_key: warn ──────────────────────────────

def test_null_required_non_key_warn_with_nulls(spark, caplog):
    df = spark.createDataFrame(
        [(1, "Alice", "Active"), (2, None, "Active")],
        ["id", "name", "status"]
    )
    config = {"check_config": "required_non_key", "action": "warn"}
    with caplog.at_level(logging.WARNING):
        result = clean_non_key_null(df, TEST_TABLE_NAME, config)
    assert result.count() == 2
    assert "WARN" in caplog.text


def test_null_required_non_key_warn_no_nulls(spark, caplog):
    df = spark.createDataFrame(
        [(1, "Alice", "Active"), (2, "Bob", "Inactive")],
        ["id", "name", "status"]
    )
    config = {"check_config": "required_non_key", "action": "warn"}
    with caplog.at_level(logging.WARNING):
        clean_non_key_null(df, TEST_TABLE_NAME, config)
    assert "WARN" not in caplog.text


# ── required_non_key: fail ──────────────────────────────

def test_null_required_non_key_fail_with_nulls(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "Active"), (2, None, "Active")],
        ["id", "name", "status"]
    )
    config = {"check_config": "required_non_key", "action": "fail"}
    with pytest.raises(ValueError, match="FAIL"):
        clean_non_key_null(df, TEST_TABLE_NAME, config)


def test_null_required_non_key_fail_no_nulls_returns(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "Active"), (2, "Bob", "Inactive")],
        ["id", "name", "status"]
    )
    config = {"check_config": "required_non_key", "action": "fail"}
    result = clean_non_key_null(df, TEST_TABLE_NAME, config)
    assert result.count() == 2


# ── edge cases ──────────────────────────────────────────

def test_null_table_not_in_metadata_raises(spark):
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    config = {"check_config": "required_non_key", "action": "drop"}
    with pytest.raises(ValueError, match="No metadata found"):
        clean_non_key_null(df, "non_existent_table_xyz", config)


def test_null_invalid_action_raises(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "Active")],
        ["id", "name", "status"]
    )
    config = {"check_config": "required_non_key", "action": "invalid_action"}
    with pytest.raises(ValueError, match="Invalid action"):
        clean_non_key_null(df, TEST_TABLE_NAME, config)


def test_null_invalid_check_config_raises(spark):
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    config = {"check_config": "unknown", "action": "drop"}
    with pytest.raises(ValueError, match="Invalid check_config"):
        clean_non_key_null(df, TEST_TABLE_NAME, config)


# ── custom_columns ──────────────────────────────────────

def test_null_custom_columns_drop(spark):
    df = spark.createDataFrame(
        [(1, "Alice", "Active"), (2, None, "Active"), (3, "Charlie", None)],
        ["id", "name", "status"]
    )
    config = {
        "check_config": "custom_columns",
        "columns": ["name", "status"],
        "action": "drop"
    }
    result = clean_non_key_null(df, TEST_TABLE_NAME, config)
    assert result.count() == 1


def test_null_custom_columns_warn_with_nulls(spark, caplog):
    df = spark.createDataFrame([(1, "Alice"), (2, None)], ["id", "name"])
    config = {
        "check_config": "custom_columns",
        "columns": ["name"],
        "action": "warn"
    }
    with caplog.at_level(logging.WARNING):
        result = clean_non_key_null(df, TEST_TABLE_NAME, config)
    assert result.count() == 2
    assert "WARN" in caplog.text


def test_null_custom_columns_fail_with_nulls(spark):
    df = spark.createDataFrame([(1, "Alice"), (2, None)], ["id", "name"])
    config = {
        "check_config": "custom_columns",
        "columns": ["name"],
        "action": "fail"
    }
    with pytest.raises(ValueError, match="FAIL"):
        clean_non_key_null(df, TEST_TABLE_NAME, config)


def test_null_custom_columns_no_nulls_no_raise(spark):
    df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
    config = {
        "check_config": "custom_columns",
        "columns": ["name"],
        "action": "fail"
    }
    result = clean_non_key_null(df, TEST_TABLE_NAME, config)
    assert result.count() == 2


def test_null_custom_columns_none_raises(spark):
    df = spark.createDataFrame([(1, "Alice")], ["id", "name"])
    config = {"check_config": "custom_columns", "action": "drop"}
    with pytest.raises(ValueError, match="No columns provided"):
        clean_non_key_null(df, TEST_TABLE_NAME, config)
