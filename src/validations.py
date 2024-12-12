from src.utils import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

logger = get_logger(__name__)


def check_missing_values(df_name: str, df: DataFrame, column: str) -> int:
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
        if missing_count > 0:
            logger.info(
                f"Dataframe: {df_name} Column: '{column}' has {missing_count} missing/null values."
            )
        return missing_count

    except Exception as e:
        logger.error(f"Application encountered an error: {e}")


def check_data_format(
    df_name: str, df: DataFrame, column: str, expected_type: str
) -> int:
    """
    Checks for inconsistencies in the data format of a column.

    Args:
        df (DataFrame): The Spark DataFrame.
        column (str): The name of the column to check.
        expected_type (str): The expected Spark data type (e.g., "int", "string").

    Returns:
        int: The count of inconsistent rows.
    """
    try:
        inconsistent_count = df.filter(
            ~col(column).cast(expected_type).isNotNull()
        ).count()
        if inconsistent_count > 0:
            logger.info(
                f"DataFrame: {df_name} Column '{column}' has {inconsistent_count} inconsistent rows (not {expected_type})."
            )
        return inconsistent_count
    except Exception as e:
        logger.error(f"Application encountered an error: {e}")


def check_duplicates_by_column(df_name: str, df: DataFrame, column: str) -> int:
    """
    Identifies duplicates in the DataFrame based on a specific column.

    Args:
        df (DataFrame): Input DataFrame.
        column (str): The column to check for duplicates.

    Returns:
        int: Count of duplicate rows based on the specified column.
    """
    logger.info(f"Checking Duplicate Values on {df_name} - {column}")
    try:
        duplicate_count = df.count() - df.select(column).distinct().count()
        if duplicate_count > 0:
            logger.info(
                f"DataFrame: {df_name} Column '{column}' has {duplicate_count} duplicate rows."
            )
        return duplicate_count

    except Exception as e:
        logger.error(f"Application encountered an error: {e}")
