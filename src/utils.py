import logging
import json
from pyspark.sql import DataFrame


def get_logger(name: str) -> logging.Logger:
    """
    Configures and returns a logger to write messages during
    execution tim.

    Args:
        name (str): The name of the logger.

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    if not logger.handlers:
        logger.addHandler(console_handler)

    return logger


def get_dtype(df: DataFrame, colname: str) -> str:
    """
    Get the data type of a specific column from a Spark DataFrame.

    Args:
        df (DataFrame): The Spark DataFrame.
        colname (str): Name of the column in the DataFrame.

    Returns:
        str: The data type of the specified column.

    Raises:
        TypeError: If the input DataFrame is not a Spark DataFrame or colname is not a string.
        ValueError: If the specified column name does not exist in the DataFrame.
    """
    # Validate input types
    if not isinstance(df, DataFrame):
        raise TypeError("The 'df' parameter must be a Spark DataFrame.")
    if not isinstance(colname, str):
        raise TypeError("The 'colname' parameter must be a string.")

    # Check if column exists
    if colname not in df.columns:
        raise ValueError(f"Column '{colname}' does not exist in the DataFrame. Available columns: {df.columns}")

    # Get the data type of the column
    try:
        return [dtype for name, dtype in df.dtypes if name == colname][0]
    except IndexError:
        # This shouldn't happen due to the prior column existence check, but included for safety.
        raise ValueError(f"Unexpected error: Column '{colname}' could not be found in the DataFrame.")


logger = get_logger(__name__)


def get_config(file_path: str) -> dict:
    """
    Reads a configuration file in JSON format and parses it into a dictionary.

    Args:
        file_path (str): Path to the JSON configuration file.

    Returns:
        dict: Parsed configuration as a dictionary.
    """
    logger.info("Getting Dataset configuration.")
    try:
        with open(file_path, "r") as file:
            config = json.load(file)
        logger.info("Configuration File loaded successfully.")
        return config

    except FileNotFoundError:
        logger.error(f"Error: Configuration file not found at {file_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error: Failed to decode JSON. {e}")
        raise
    except Exception as e:
        logger.error(f"Error: Failed to decode JSON. {e}")


def write_dataframe(
    df, output_path, format="parquet", partition_by=None, mode="overwrite", header=True
):
    """
    Function to write a PySpark DataFrame in Parquet or CSV format with optional partitioning.

    Args:
        df (DataFrame): The PySpark DataFrame to be written.
        output_path (str): The path where the file will be saved.
        format (str): Output format. Options: "parquet", "csv". Default: "parquet".
        partition_by (list): List of columns to partition the data by. Default: None.
        mode (str): Write mode. Options: "overwrite", "append", "ignore", "error". Default: "overwrite".
        header (bool): If True, includes a header in CSV files. Default: True (only applies to CSV).
    """
    if format not in {"parquet", "csv"}:
        logger.error(f"Unsupported format '{format}'. Use 'parquet' or 'csv'.")
        raise ValueError(f"Unsupported format '{format}'. Use 'parquet' or 'csv'.")

    writer = df.write.mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    try:
        if format == "csv":
            writer.option("header", header).csv(output_path)
        elif format == "parquet":
            writer.parquet(output_path)

        logger.info(f"DataFrame written in {format} format to: {output_path}")
    except Exception as e:
        logger.error(f"Failed to write DataFrame in {format} format. Error: {e}")
        raise
