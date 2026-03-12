import uuid
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from data_ops.delta_table_writer.api import write_table



@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.getOrCreate()


def _table_name(prefix="ut_delta_writer"):
    return f"default.{prefix}_{uuid.uuid4().hex[:12]}"


def _drop_table_if_exists(spark, table_name: str):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")


def _sorted_rows(df, order_cols):
    return [tuple(r) for r in df.orderBy(*order_cols).collect()]


def _assert_table_data(spark, table_name, expected_rows, order_cols):
    actual_rows = _sorted_rows(spark.table(table_name), order_cols)
    assert actual_rows == expected_rows


def test_append_write(spark):
    table_name = _table_name("append")
    _drop_table_if_exists(spark, table_name)

    try:
        df1 = spark.createDataFrame(
            [(1, "A"), (2, "B")],
            ["id", "value"]
        )

        write_table(
            df=df1,
            target_table=table_name,
            write_mode="append",
            enable_audit=False,
            enable_surrogate_key=False
        )

        df2 = spark.createDataFrame(
            [(3, "C")],
            ["id", "value"]
        )

        write_table(
            df=df2,
            target_table=table_name,
            write_mode="append",
            enable_audit=False,
            enable_surrogate_key=False
        )

        _assert_table_data(
            spark,
            table_name,
            [(1, "A"), (2, "B"), (3, "C")],
            ["id"]
        )
    finally:
        _drop_table_if_exists(spark, table_name)


def test_overwrite_write(spark):
    table_name = _table_name("overwrite")
    _drop_table_if_exists(spark, table_name)

    try:
        df1 = spark.createDataFrame(
            [(1, "A"), (2, "B")],
            ["id", "value"]
        )

        write_table(
            df=df1,
            target_table=table_name,
            write_mode="append",
            enable_audit=False,
            enable_surrogate_key=False
        )

        df2 = spark.createDataFrame(
            [(10, "X")],
            ["id", "value"]
        )

        write_table(
            df=df2,
            target_table=table_name,
            write_mode="overwrite",
            enable_audit=False,
            enable_surrogate_key=False
        )

        _assert_table_data(
            spark,
            table_name,
            [(10, "X")],
            ["id"]
        )
    finally:
        _drop_table_if_exists(spark, table_name)


def test_append_with_audit_columns(spark):
    table_name = _table_name("audit")
    _drop_table_if_exists(spark, table_name)

    try:
        df = spark.createDataFrame(
            [(1, "A")],
            ["id", "value"]
        )

        write_table(
            df=df,
            target_table=table_name,
            write_mode="append",
            enable_audit=True,
            enable_surrogate_key=False
        )

        result = spark.table(table_name)

        expected_columns = {
            "id",
            "value",
            "DWHCreatedDate",
            "DWHModifiedDate",
            "DWHCreatedBy",
            "DWHModifiedBy"
        }

        assert expected_columns.issubset(set(result.columns))

        row = result.first()
        assert row["DWHCreatedBy"] == "system"
        assert row["DWHModifiedBy"] == "system"
        assert row["DWHCreatedDate"] is not None
        assert row["DWHModifiedDate"] is not None
    finally:
        _drop_table_if_exists(spark, table_name)


def test_upsert_write(spark):
    table_name = _table_name("upsert")
    _drop_table_if_exists(spark, table_name)

    try:
        initial_df = spark.createDataFrame(
            [(1, "A"), (2, "B")],
            ["id", "value"]
        )

        write_table(
            df=initial_df,
            target_table=table_name,
            write_mode="append",
            enable_audit=False,
            enable_surrogate_key=False
        )

        upsert_df = spark.createDataFrame(
            [(2, "B_UPDATED"), (3, "C")],
            ["id", "value"]
        )

        write_table(
            df=upsert_df,
            target_table=table_name,
            merge_on_key=["id"],
            write_mode="upsert",
            enable_audit=False,
            enable_surrogate_key=False
        )

        _assert_table_data(
            spark,
            table_name,
            [(1, "A"), (2, "B_UPDATED"), (3, "C")],
            ["id"]
        )
    finally:
        _drop_table_if_exists(spark, table_name)


def test_upsert_without_merge_key_raises(spark):
    table_name = _table_name("upsert_no_key")
    _drop_table_if_exists(spark, table_name)

    try:
        df = spark.createDataFrame(
            [(1, "A")],
            ["id", "value"]
        )

        with pytest.raises(ValueError, match="merge_on_key"):
            write_table(
                df=df,
                target_table=table_name,
                write_mode="upsert",
                merge_on_key=None,
                enable_audit=False,
                enable_surrogate_key=False
            )
    finally:
        _drop_table_if_exists(spark, table_name)


def test_upsert_with_empty_merge_key_list_raises(spark):
    table_name = _table_name("upsert_empty_key")
    _drop_table_if_exists(spark, table_name)

    try:
        df = spark.createDataFrame(
            [(1, "A")],
            ["id", "value"]
        )

        with pytest.raises(ValueError, match="merge_on_key"):
            write_table(
                df=df,
                target_table=table_name,
                write_mode="upsert",
                merge_on_key=[],
                enable_audit=False,
                enable_surrogate_key=False
            )
    finally:
        _drop_table_if_exists(spark, table_name)


def test_upsert_with_invalid_merge_key_column_raises(spark):
    table_name = _table_name("upsert_bad_key")
    _drop_table_if_exists(spark, table_name)

    try:
        df = spark.createDataFrame(
            [(1, "A")],
            ["id", "value"]
        )

        with pytest.raises(ValueError):
            write_table(
                df=df,
                target_table=table_name,
                write_mode="upsert",
                merge_on_key=["business_id"],
                enable_audit=False,
                enable_surrogate_key=False
            )
    finally:
        _drop_table_if_exists(spark, table_name)


def test_invalid_write_mode_raises(spark):
    table_name = _table_name("bad_mode")
    _drop_table_if_exists(spark, table_name)

    try:
        df = spark.createDataFrame(
            [(1, "A")],
            ["id", "value"]
        )

        with pytest.raises(ValueError, match="Invalid write mode"):
            write_table(
                df=df,
                target_table=table_name,
                write_mode="bad_mode",
                enable_audit=False,
                enable_surrogate_key=False
            )
    finally:
        _drop_table_if_exists(spark, table_name)


def test_upsert_deduplicates_incoming_source_when_duplicate_keys_exist(spark):
    table_name = _table_name("upsert_dedup")
    _drop_table_if_exists(spark, table_name)

    try:
        initial_df = spark.createDataFrame(
            [(1, "A")],
            ["id", "value"]
        )

        write_table(
            df=initial_df,
            target_table=table_name,
            write_mode="append",
            enable_audit=False,
            enable_surrogate_key=False
        )

        source_df = spark.createDataFrame(
            [(1, "A_NEW"), (1, "A_NEW"), (2, "B")],
            ["id", "value"]
        )

        write_table(
            df=source_df,
            target_table=table_name,
            merge_on_key=["id"],
            write_mode="upsert",
            enable_audit=False,
            enable_surrogate_key=False
        )

        result = spark.table(table_name).select("id", "value")
        rows = _sorted_rows(result, ["id"])

        assert len(rows) == 2
        assert rows[0][0] == 1
        assert rows[1][0] == 2
    finally:
        _drop_table_if_exists(spark, table_name)
