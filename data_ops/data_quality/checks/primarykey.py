import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

def validate_pk(df: DataFrame, pk: str) -> DataFrame:
    """
    Validates and clean the Primary key in the passed dataframe.
    args:
        df: Spark Dataframe
        pk: Name of Primary key
    returns:
        df: Cleaned Dataframe
    Validations perfomed:
        - Check if Primary key is present in the dataframe
        - Drop duplicate primary key
        - Check if Primary key is not null, drop rows with null primary key
    """
    logger.info(f"[Info] Started validating and cleaning primary Key {pk}")
    # Check if Primary key is present in the dataframe
    if pk not in df.columns:
        raise ValueError(f"Primary key {pk} is missing in source Dataframe")
    
    logger.info(f"[Info] Source Dataframe count is {df.count()}")

    # Check for null values in Primary key column
    null_count = df.filter(F.col(pk).isNull()).count()
    if null_count:
        logger.info(f"[Info] Found {null_count} null values in Primary key {pk}.")
    df = df.dropna(subset=[pk])
    
    # Drop duplicate primary key
    df = df.dropDuplicates([pk])
    logger.info(f"[Info] Primary key {pk}: cleaned and validated")

    return df


  