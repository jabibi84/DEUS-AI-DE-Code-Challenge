from src.utils import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = get_logger(__name__)


def check_missing_values(df: DataFrame, column: str) -> int:
    """
    Checks for missing or null values in a specific column of the DataFrame.

    Args:
        df (DataFrame): The Spark DataFrame.
        column (str): The name of the column to check.

    Returns:
        int: The count of missing or null values in the column.
    """
    try:
        missing_count = df.filter(col(column).isNull()).count()
        logger.info(f"Column '{column}' has {missing_count} missing or null values.")
        return missing_count

    except Exception as e:
        logger.error(f"Application encountered an error: {e}")

