# Import libraries
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

# Import custom helper functions
from checks.primarykey import validate_pk
from checks.columnValidation import validate_column
from checks.nullvalidation import clean_non_key_null


class SourceDataValidator:
    def __init__(self):
        self.spark = SparkSession.getActiveSession()
        if self.spark is None:
            raise ValueError("No active SparkSession found")

    def execute_validation(
        self,
        df: DataFrame,
        target_table: str,
        primary_key: Optional[List[str]] = None,
        validate_primary_key: bool = False,
        validate_column: bool = False,
        drop_nulls: Optional[dict] = None
    ):
        self._df = df
        self._target_table = target_table
        self._primary_key = primary_key
        self._validate_primary_key = validate_primary_key
        self._validate_column = validate_column
        self._drop_nulls = drop_nulls

        # Validate and clean primary key
        if self._validate_primary_key:
            self._df = validate_pk(self._df, self._primary_key)

        # Validate column
        if self._validate_column:
            self.validate_column(self._df, self._target_table)

        # Non-key Null check
        if self._dropnulls and self._drop_nulls.get("enabled"):
            self._df = self.clean_non_key_null(self._df, self._target_table, self._drop_nulls)

