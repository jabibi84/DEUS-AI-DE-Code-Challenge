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


def check_data_format_inconsistencies(df: DataFrame, column: str, expected_type: str) -> int:
    """
    Checks for inconsistencies in the data format of a column.

    Args:
        df (DataFrame): The Spark DataFrame.
        column (str): The name of the column to check.
        expected_type (str): The expected Spark data type (e.g., "int", "string").

    Returns:
        int: The count of inconsistent rows.
    """
    inconsistent_count = df.filter(~col(column).cast(expected_type).isNotNull()).count()
    logger.info(f"Column '{column}' has {inconsistent_count} inconsistent rows (not {expected_type}).")
    return inconsistent_count
