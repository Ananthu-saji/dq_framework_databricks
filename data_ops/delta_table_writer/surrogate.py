from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as F

def create_surrogate_key(spark, df: DataFrame, target_table: str, surrogate_key_col: str) -> DataFrame:
        """
        Function to create surrogate key in the dataframe
        Args:
            df: Spark Dataframe
            surrogate_key_col: Name of the surrogate key column
            target_table: Name of the target table
        Returns:
            df: Spark Dataframe with surrogate key column

        [IMPORTANT] if you are using Delta Lake, the best approach would be to let Delta Lake generate the surrogate Key. In that case please skip this step and use the following SQL statement to create the table:
        Example:
            CREATE TABLE <TABLENAME> (
                <surrogate_key> BIGINT GENERATED ALWAYS AS IDENTITY, -- Use GENERATED ALWAYS AS IDENTITY to generate a surrogate key
                COL_1 DATATYPE,
                COL_2 DATATYPE,
                .
                .
                COL_N DATATYPE
            )

        then simply write the dataframe to the table:
            df.write.format("delta").mode("append").saveAsTable(<TABLENAME>)       
        """
        if spark.catalog.tableExists(target_table):

            #get maximum surrogate key from target table
            max_sk = (
                spark.table(target_table).select(F.max(surrogate_key_col)).collect()[0][0]
                )
            if max_sk is None:
                max_sk = 0
        else:
            max_sk = 0

        window = Window.orderBy(F.monotonically_increasing_id())
        
        df = (
            df.withColumn(surrogate_key_col, F.row_number().over(window) + F.lit(max_sk))
        )

        return df