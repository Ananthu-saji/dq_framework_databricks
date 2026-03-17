import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

# ---------- Spark fixture ----------

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    # In Databricks / Databricks Connect, SparkSession is already configured.
    # Just grab the existing one.
    spark = SparkSession.builder.getOrCreate()
    # Optionally set test DB if you want
    spark.sql("CREATE DATABASE IF NOT EXISTS test_db")
    spark.catalog.setCurrentDatabase("test_db")
    return spark

# ---------- Functions under test (import from your module in real code) ----------

from pyspark.sql import DataFrame

def append_data(df: DataFrame, target_table: str) -> None:
    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

def overwrite_data(df: DataFrame, target_table: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

def merge_data(spark: SparkSession, df: DataFrame, target_table: str, merge_on_keys: list[str]) -> None:
    delta_table = DeltaTable.forName(spark, target_table)
    merge_condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_on_keys])
    (
        delta_table.alias("t")
        .merge(df.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

def create_audit_columns(df: DataFrame) -> DataFrame:
    now = F.current_timestamp()
    return (
        df
        .withColumn("DWHCreatedDate", now)
        .withColumn("DWHModifiedDate", now)
        .withColumn("DWHCreatedBy", F.lit("system"))
        .withColumn("DWHModifiedBy", F.lit("system"))
        .withColumn("DWHDeletedDate", F.lit(None).cast("timestamp"))
        .withColumn("DWH_ISDeleted", F.lit(0))
    )

def scd2(spark: SparkSession, df: DataFrame, target_table: str, merge_on_keys: list[str]) -> None:
    target_df = (
        spark.table(target_table)
        .filter("DWH_ISDeleted = 0")
        .drop(
            "DWH_ISDeleted",
            "DWHDeletedDate",
            "DWHModifiedDate",
            "DWHModifiedBy",
            "DWHCreatedDate",
            "DWHCreatedBy",
        )
        .select(df.columns)
    )

    change_df = df.exceptAll(target_df)
    change_count = change_df.count()
    if change_count == 0:
        return

    change_df = create_audit_columns(change_df)

    delta_table = DeltaTable.forName(spark, target_table)

    key_condition = " AND ".join([f"t.{k} = s.{k}" for k in merge_on_keys])
    merge_condition = f"{key_condition} AND t.DWH_ISDeleted = 0"

    (
        delta_table.alias("t")
        .merge(change_df.alias("s"), merge_condition)
        .whenMatchedUpdate(
            set={
                "DWH_ISDeleted": F.lit(1),
                "DWHDeletedDate": F.current_timestamp(),
                "DWHModifiedDate": F.current_timestamp(),
                "DWHModifiedBy": F.lit("system"),
            }
        )
        .execute()
    )

    (
        change_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

# ---------- Tests ----------

def test_append_data_creates_rows(spark: SparkSession):
    table = "test_db.append_table"
    df_init = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    overwrite_data(df_init, table)

    df_new = spark.createDataFrame([(3, "c")], ["id", "val"])
    append_data(df_new, table)

    result = spark.table(table).orderBy("id").collect()
    assert [(r.id, r.val) for r in result] == [(1, "a"), (2, "b"), (3, "c")]

def test_overwrite_data_replaces_all_rows(spark: SparkSession):
    table = "test_db.overwrite_table"
    df_init = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    overwrite_data(df_init, table)

    df_new = spark.createDataFrame([(10, "x")], ["id", "val"])
    overwrite_data(df_new, table)

    result = spark.table(table).collect()
    assert [(r.id, r.val) for r in result] == [(10, "x")]

def test_merge_data_upsert_behavior(spark: SparkSession):
    table = "test_db.merge_table"
    df_init = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    overwrite_data(df_init, table)

    df_upsert = spark.createDataFrame([(2, "b2"), (3, "c")], ["id", "val"])
    merge_data(spark, df_upsert, table, merge_on_keys=["id"])

    result = spark.table(table).orderBy("id").collect()
    assert [(r.id, r.val) for r in result] == [(1, "a"), (2, "b2"), (3, "c")]

def test_scd2_inserts_new_and_versions_changed(spark: SparkSession):
    table = "test_db.scd2_table"

    # Initial snapshot
    df_init = spark.createDataFrame(
        [
            (1, "Alice", "Berlin"),
            (2, "Bob", "Munich"),
        ],
        ["id", "name", "city"],
    )
    df_init_audit = create_audit_columns(df_init)
    overwrite_data(df_init_audit, table)

    # New snapshot: Alice moves city; Charlie is new; Bob unchanged
    df_snap2 = spark.createDataFrame(
        [
            (1, "Alice", "Hamburg"),  # changed
            (2, "Bob", "Munich"),     # unchanged
            (3, "Charlie", "Berlin"), # new
        ],
        ["id", "name", "city"],
    )

    scd2(spark, df_snap2, table, merge_on_keys=["id"])

    df_res = spark.table(table).orderBy("id", "DWHCreatedDate").collect()

    # Group by id for easier assertions
    by_id = {}
    for r in df_res:
        by_id.setdefault(r.id, []).append(r)

    # id=1: one old deleted, one current
    assert len(by_id[1]) == 2
    old_v, new_v = by_id[1]
    assert old_v.city == "Berlin" and old_v.DWH_ISDeleted == 1
    assert new_v.city == "Hamburg" and new_v.DWH_ISDeleted == 0

    # id=2: still one current version only (unchanged)
    assert len(by_id[2]) == 1
    assert by_id[2][0].city == "Munich" and by_id[2][0].DWH_ISDeleted == 0

    # id=3: one current version (new)
    assert len(by_id[3]) == 1
    assert by_id[3][0].city == "Berlin" and by_id[3][0].DWH_ISDeleted == 0

def test_scd2_no_change_does_not_create_new_versions(spark: SparkSession):
    table = "test_db.scd2_nochange_table"

    df_init = spark.createDataFrame(
        [
            (1, "Alice", "Berlin"),
        ],
        ["id", "name", "city"],
    )
    df_init_audit = create_audit_columns(df_init)
    overwrite_data(df_init_audit, table)

    # Snapshot is identical
    df_same = spark.createDataFrame(
        [
            (1, "Alice", "Berlin"),
        ],
        ["id", "name", "city"],
    )

    scd2(spark, df_same, table, merge_on_keys=["id"])

    df_res = spark.table(table).collect()
    # Should still be exactly one current row
    assert len(df_res) == 1
    r = df_res[0]
    assert r.id == 1 and r.city == "Berlin" and r.DWH_ISDeleted == 0
