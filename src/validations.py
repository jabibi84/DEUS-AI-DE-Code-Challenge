from src.utils import get_logger
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from src.utils import get_config, get_logger


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
    logger.info(f" on {df_name} - {column}")
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
    logger.info(f" on {df_name} - {column}")
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


def check_duplicates(df_name: str, df: DataFrame, column: str = None) -> int:
    """
    Identifies duplicates in the DataFrame. If a column is specified, it checks for duplicates
    based on that column. Otherwise, it checks for duplicates across the entire DataFrame.

    Args:
        df_name (str): Name of the DataFrame.
        df (DataFrame): Input DataFrame.
        column (str, optional): The column to check for duplicates. Defaults to None.

    Returns:
        int: Count of duplicate rows.
    """
    try:
        if column:
            duplicate_count = df.count() - df.select(column).distinct().count()
            if duplicate_count > 0:
                logger.info(
                    f"DataFrame: {df_name} Column '{column}' has {duplicate_count} duplicate rows."
                )
        else:
            logger.info(f" Values on {df_name} - Entire DataFrame")
            duplicate_count = df.count() - df.distinct().count()
            if duplicate_count > 0:
                logger.info(
                    f"DataFrame: {df_name} has {duplicate_count} duplicate rows across all columns."
                )

        return duplicate_count

    except Exception as e:
        logger.error(f"Application encountered an error: {e}")
