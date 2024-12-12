from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


def add_status_column(df: DataFrame) -> DataFrame:
    """
    Agrega una columna 'status' basada en la columna 'value':
    - 'high' si value > 50
    - 'low' si value <= 50
    """
    return df.withColumn(
        "status",
        when(col("value") > 50, "high").otherwise("low")
    )
