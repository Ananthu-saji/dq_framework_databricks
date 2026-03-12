'''
Main API for delta table write module.

This module provide the primary interface for writing daraframes into delta tables, with various options for controlling the write behavior. Various write strategies include: append, overwrite and upsert. The write_table functions serves as the unified entry point for all table writing operations.
'''
from typing import List, Optional
from pyspark.sql import DataFrame
from .writer import DeltaWrite

def write_table(
        df: DataFrame,
        target_table: str,
        merge_on_key: Optional[List[str]] = None,
        write_mode: str = "append",
        enable_audit: bool = False,
        enable_surrogate_key: bool = False,
        surrogate_key_col: str = "surrogate_key"
    ) -> None:

    """
    Function to be invoked in fact and dimension notebooks to write data into target table
    
    Agrs:
        df: Spark Dataframe to be written into target table
        target_table: Name of the target table
        merge_on_key: List of columns to be used for merge operation
        write_mode: Write mode to be used for writing data into target table. Valid values are append, overwrite and upsert
        enable_audit: Boolean flag to enable audit columns. Valid values are True and False
        enable_surrogate_key: Boolean flag to enable surrogate key. Valid values are True and False
        surrogate_key_col: Name of the surrogate key column.
        Returns:
            None
    """

    manager = DeltaWrite()
    manager.execute_write(
        df=df,
        target_table=target_table,
        merge_on_key=merge_on_key,
        write_mode=write_mode,
        enable_audit=enable_audit,
        enable_surrogate_key=enable_surrogate_key,
        surrogate_key_col=surrogate_key_col
    )