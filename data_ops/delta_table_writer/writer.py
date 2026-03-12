# Library imports
from typing import List, Union, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# Import custom helper functions
from .surrogate import create_surrogate_key
from .audit import create_audit_columns
from .writeStrategies import append_data, overwrite_data, merge_data

class DeltaWrite:
    def __init__(self):
        self.spark = SparkSession.getActiveSession()
        if self.spark is None:
            raise ValueError("No active SparkSession found")

    
    def execute_write(
        self, 
        df: DataFrame, 
        target_table: str, 
        merge_on_key: Optional[List[str]] = None, 
        write_mode: str = "append",
        enable_audit: bool = False,
        enable_surrogate_key: bool = False,
        surrogate_key_col: str = "surrogate_key"
    ) -> None:
        
        self._df = df
        self._target_table = target_table
        self._merge_on_key = merge_on_key
        self._write_mode = write_mode
        self._enable_audit = enable_audit
        self._enable_surrogate_key = enable_surrogate_key
        self._surrogate_key_col = surrogate_key_col
        


        # Convert write mode to lower case to prevent case sensitivity issues
        self._write_mode = self._write_mode.lower().strip()

        # validate write mode
        if self._write_mode not in ["append", "overwrite", "upsert"]:
            raise ValueError("Invalid write mode. Valid values are append, overwrite and upsert")

        # validate 
        # First validate merge_on_key is present if write mode is upsert
        if self._write_mode == "upsert":
            if not self._merge_on_key:
                raise ValueError("merge_on_key must be provided when write_mode is 'upsert'")

            if not isinstance(self._merge_on_key, list):
                raise ValueError("merge_on_key must be a list of strings(Column names)")

            if any((not isinstance(col, str)) or (not col.strip()) for col in self._merge_on_key):
                raise ValueError("merge_on_key must contain only non-empty column names")

            missing_keys = [key for key in self._merge_on_key if key not in self._df.columns]
            if missing_keys:
                raise ValueError(
                    f"Dataframe does not contain all merge_on_key columns. Missing columns are {missing_keys}"
                )

            self._df = self._df.dropDuplicates(self._merge_on_key)



        # create audit column if enabled
        if self._enable_audit:
            self._df = create_audit_columns(self._df)

        # create surrogate key if enabled
        if self._enable_surrogate_key:
            self._df = create_surrogate_key(self.spark, self._df, self._target_table, self._surrogate_key_col)

         
        # write data to target table according to write_mode
        if self._write_mode == "append":
            append_data(self._df, self._target_table)
        elif self._write_mode == "overwrite":
            overwrite_data(self._df, self._target_table)
        elif self._write_mode == "upsert":
            merge_data(self.spark, self._df, self._target_table, self._merge_on_key)



