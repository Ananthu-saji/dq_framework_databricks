'''
Main APIs for data quality check.

This module provide the primary interface for .
'''
from typing import List, Optional
from pyspark.sql import DataFrame
from .validator import SourceDataValidator

def validate_source_data(
        df: DataFrame,
        target_table: str,
        primary_key: str = Optional[List[str]] = None,
        validate_primary_key: bool = False,
        validate_column: bool = False,
        drop_nulls: Optional[dict] = None
    ) -> None:

    manager = DeltaWrite()
    manager.execute_validation(
        df=df,
        target_table=target_table,
        primary_key=primary_key,
        validate_primary_key=validate_primary_key,
        validate_column=validate_column,
        drop_nulls=drop_nulls
    )