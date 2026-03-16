import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# meta data tables
Tables_Meta = "data_ops.meta.tables"
Datapoints_Meta = "data_ops.meta.datapoints"


def clean_non_key_null(df: DataFrame, Tablename: str, config: Optional[dict]=None) -> DataFrame:
    """
    The aim of the function is to clean non-key columns with null values.
    args:
      df: pyspark dataframe
      config: dictionary with the following keys:
        - enabled (boolean): whether to run the function or not: True or False
        - check_config (string) : 
            "required_non_key" - check null values for non key columns based on meta data table
            "custom_columns" - check null values for custom columns pass as list
        - columns (list): list of columns to check for null values
        - action (string): action to take if null values are found
            "drop" - drop rows with null values
            "warn" - warn if null values are found
            "fail" - fail if null values are found
    """
    if config is None:
        return df
    
    if config.get("check_config") == "required_non_key":
        logger.info("[INFO]: Started required non-key validation")

        df_table = spark.table(Tables_Meta).filter(
            F.lower(F.col("table_name")) == Tablename.lower()
        )

        if df_table.count() == 0:
            raise ValueError(f"No metadata found for table: {Tablename}")

        table_id = df_table.select("table_id").collect()[0]["table_id"]

        df_datapoints = (
            spark.table(Datapoints_Meta)
            .filter(
                (F.col("table_id") == table_id) &
                (F.col("is_enabled") == True) &
                (F.col("is_required") == True) &
                (F.col("is_primary_key") == False)
            )
            .select("datapoint_name")
        )

        required_col_ls = [row["datapoint_name"] for row in df_datapoints.collect()]

        if not required_col_ls:
            logger.info("[INFO]: No required non-key columns found. Skipping.")
            return df


        if config.get("action") == "drop":
            df = df.dropna(subset=required_col_ls)
            logger.info(f"[INFO]: Dropped rows with null values in non-key columns: {required_col_ls}")
            logger.info("[INFO]: Completed required non-key validation")
            return df
        
        null_cols_ls = [c for c in required_col_ls if df.filter(F.col(c).isNull()).count() > 0]
        
        if config.get("action") == "warn":
            if null_cols_ls:
                logger.warning(f"[WARN]: Found null values in non-key columns: {null_cols_ls}")
                logger.info("[INFO]: Completed required non-key validation")
                return df
            else:
                logger.info("[INFO]: No null values found in non-key columns.")
                return df
        
        elif config.get("action") == "fail":
            if null_cols_ls:
                raise ValueError(f"[FAIL]: Found null values in non-key columns: {null_cols_ls}")
            else:
                logger.info("[INFO]: No null values found in non-key columns.")
                return df
        else:
            raise ValueError(f"Invalid action: {config.get('action')}")

        

    elif config.get("check_config") == "custom_columns":
        logger.info("[INFO]: Started custom columns validation")

        if config.get("columns") is None:
            raise ValueError("No columns provided for custom columns validation")

        if config.get("action") == "drop":
            df = df.dropna(subset=config.get("columns"))
            logger.info(f"[INFO]: Dropped rows with null values in custom columns: {config.get('columns')}")
            logger.info("[INFO]: Completed custom columns validation")
            return df
        
        null_cols_ls = [c for c in config.get("columns") if df.filter(F.col(c).isNull()).count() > 0]
        
        if config.get("action") == "warn":
            if null_cols_ls: 
                logger.warning(f"[WARN]: Found null values in custom columns: {null_cols_ls}")
                logger.info("[INFO]: Completed custom columns validation")
                return df
            else:
                logger.info("[INFO]: No null values found in custom columns.")
                return df
        
        elif config.get("action") == "fail":
            if null_cols_ls:
                raise ValueError(f"[FAIL]: Found null values in custom columns: {null_cols_ls}")
            else:
                logger.info("[INFO]: No null values found in custom columns.")
                return df
        else:
            raise ValueError(f"Invalid action: {config.get('action')}")
    else:
        raise ValueError(f"Invalid check_config: {config.get('check_config')}")


