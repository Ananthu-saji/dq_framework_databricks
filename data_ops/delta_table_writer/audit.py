from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def create_audit_columns(df: DataFrame) -> DataFrame:
    """
    Function to create audit columns in the dataframe
    Audit columns:
        - DWHCreatedDate
        - DWHModifiedDate
        - DWHCreatedBy
        - DWHModifiedBy
    Args:
        df: Input Dataframe
    Returns:
        df: Spark Dataframe with audit columns
    """
    return (df.withColumn("DWHCreatedDate", F.current_timestamp())
                .withColumn("DWHModifiedDate", F.current_timestamp())
                .withColumn("DWHCreatedBy", F.lit("system"))
                .withColumn("DWHModifiedBy", F.lit("system"))
                .withColumn("DWHDeletedDate", F.lit(None).cast("timestamp"))
                .withColumn("DWH_ISDeleted", F.lit(0))
    )