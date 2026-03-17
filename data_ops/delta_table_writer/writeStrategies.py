# Import libraries
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

#Import custom libraries
from .audit import create_audit_columns

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def append_data(df: DataFrame, target_table: str) -> None:
  """
  Function to perform append operation
  Args:
    df: Dataframe to be appended
    target_table: Target Delta Table
  Returns:
    None
  """
  (
      df.write
      .format("delta")
      .mode("append")
      .saveAsTable(target_table)
  )

def overwrite_data(df: DataFrame, target_table: str) -> None:
    """
    Function to perform overwrite operation
    Args:
        df: Dataframe to be overwite
        target_table: Target Delta Table
    Returns:
        None
    """
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

def merge_data(spark, df: DataFrame, target_table: str, merge_on_keys: list[str]) -> None:
    """
    Function to perform merge operation
    Args:
        df: Dataframe to be upsert
        target_table: Target Delta Table
        merge_on_keys: List of keys to be used for merge
    Returns:
        None
    """
    # Get Delta Table
    delta_table = DeltaTable.forName(spark, target_table)

    # Create merge condition
    merge_condition = " AND ".join([f"t.{key} = s.{key}" for key in merge_on_keys])

    (
        delta_table.alias("t")
        .merge(df.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

def scd1(spark, df: DataFrame, target_table: str, merge_on_keys: list[str]) -> None:
    """
    Function to perform SCD Type 1 operation
    Args:
        df: Dataframe to perfrom scd1
        target_table: Target Delta Table
        merge_on_keys: List of keys to be used for merge
    Returns:
        None
    """
    merge_data(spark, df, target_table, merge_on_keys)

def scd2(spark, df: DataFrame, target_table: str, merge_on_keys: list[str]) -> None:
    """
    Function to perform SCD Type 2 operation
    Args:
        df: Dataframe to perform scd2
        target_table: Target Delta Table
        merge_on_keys: List of keys to be used for merge
    Returns:
        None
    """
    target_df = (spark.table(target_table)
                 .filter("DWH_ISDeleted = 0")
                 .drop("DWH_ISDeleted", "DWHDeletedDate", "DWHModifiedDate", "DWHModifiedBy", "DWHCreatedDate", "DWHCreatedBy")
                 .select(df.columns)
                )

    # filter updated and new records
    change_df = df.exceptAll(target_df)
    change_count = change_df.count()

    if change_count == 0:
        logger.info("[INFO] No changes found in source dataframe")
        return


    # add audit columns
    change_df = create_audit_columns(change_df)

    # create merge condition
    merge_condition = " AND ".join([f"t.{key} = s.{key}" for key in merge_on_keys])
    merge_conidtion = f"{merge_condition} AND t.DWH_ISDeleted = 0"

    delta_table = DeltaTable.forName(spark, target_table)

    (
        delta_table.alias("t")
        .merge(change_df.alias("s"), merge_conidtion)
        .whenMatchedUpdate(
            set = {
                "DWH_ISDeleted": F.lit(1),
                "DWHDeletedDate": F.current_timestamp(),
                "DWHModifiedDate": F.current_timestamp(),
                "DWHModifiedBy": F.lit("system")
            }
        )
        .execute()
    )

    (
        change_df
        .write
        .format("delta")
        .mode("append")
        .saveAsTable(target_table)
    )

    logger.info("[INFO] SCD2 operation completed")
    logger.info(f"[INFO] Number of records updated/Inserted: {change_count}")
