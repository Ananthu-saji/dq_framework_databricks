import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# meta data tables
Tables_Meta = "data_ops.meta.tables"
Datapoints_Meta = "data_ops.meta.datapoints"

def validate_column(df: DataFrame, Tablename : str) -> None:
    """
    The aim of the function is to validates the columns of the passed DataFrame.
    Args:
        df (DataFrame): The DataFrame to be validated.
        Tablename (str): The name of the table to be validated.
    Returns:
        DataFrame: The validated DataFrame.
    """
    logger.info(f"[INFO]: Validating columns of dataframe")

    # Get meta data from meta.datapoints
    df_table = spark.table(Tables_Meta).filter(
        F.lower(F.col("table_name")) == Tablename.lower()
    )

    if df_table.count() == 0:
        logger.warning(f"[WARNING]: No metadata found for table: {Tablename}, skipping column validation.")
        return 

    # Get table id of the passed Tablename
    table_id = df_table.select("table_id").collect()[0]["table_id"]

    # Get datapoint name
    df_datapoints = (
    spark.table(Datapoints_Meta)
        .filter((F.col("table_id") == table_id) & (F.col("is_enabled") == True))
        .select("datapoint_name", "is_required")
    )

    #check mandatory columns missing
    required_col_ls = [row["datapoint_name"] for row in df_datapoints.filter(F.col("is_required") == True).collect()]
    missing_col_ls = [col for col in required_col_ls if col not in df.columns]
    
    if missing_col_ls:
        raise ValueError(f"[ERROR]: Source Dataframe is missing required mandatory columns: {missing_col_ls}")
    else:
        logger.info(f"[INFO]: All required columns are present in source dataframe")

    # Chck optional columns missing
    optional_col_ls = [row["datapoint_name"] for row in df_datapoints.filter(F.col("is_required") == False).collect()]
    missing_col_ls = [col for col in optional_col_ls if col not in df.columns]

    if missing_col_ls:
        logger.warning(f"[WARNING]: Source Dataframe is missing optional columns: {missing_col_ls}")
    else:
        logger.info(f"[INFO]: All optional columns are present in source dataframe")

    # Check for additional columns
    unexpected_col_ls = [col for col in df.columns if col not in (required_col_ls + optional_col_ls)]
    if unexpected_col_ls:
        logger.warning(f"[WARNING]: Source Dataframe has additional columns extracted to landing zone: {unexpected_col_ls}")
    logger.info(f"[INFO]: Column validation completed for {Tablename}")
    return