# Import libraries
from pyspark.sql import DataFrame
from delta.tables import DeltaTable

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
        df: Dataframe to be appended
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
        df: Dataframe to be appended
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
