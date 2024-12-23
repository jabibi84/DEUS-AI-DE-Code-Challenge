import json
import logging

from pyspark.sql import DataFrame


def get_logger(name: str, level: str = "INFO") -> logging.Logger:
    """
    Configures and returns a logger to write messages during
    execution time with a specified log level.

    Args:
        name (str): The name of the logger.
        level (str): The logging level. Options are "DEBUG", "INFO",
                     "WARNING", "ERROR", or "CRITICAL". Default is "INFO".

    Returns:
        logging.Logger: Configured logger.

    Raises:
        ValueError: If the provided logging level is invalid.
    """
    # Map string level to logging level
    valid_levels = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    if level not in valid_levels:
        raise ValueError(
            f"Invalid log level '{level}'. Choose from {list(valid_levels.keys())}."
        )

    logger = logging.getLogger(name)
    logger.setLevel(valid_levels[level])

    console_handler = logging.StreamHandler()
    console_handler.setLevel(valid_levels[level])

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
    )
    console_handler.setFormatter(formatter)

    # Avoid adding multiple handlers to the logger
    if not logger.handlers:
        logger.addHandler(console_handler)

    return logger


def get_config(file_path: str) -> dict:
    """
    Reads a configuration file in JSON format and parses it into a dictionary.

    Args:
        file_path (str): Path to the JSON configuration file.

    Returns:
        dict: Parsed configuration as a dictionary.

    Raises:
        FileNotFoundError: If the file is not found at the specified path.
        json.JSONDecodeError: If the file contains invalid JSON.
        Exception: For any other unexpected errors.
    """
    try:
        with open(file_path, "r") as file:
            config = json.load(file)
        return config

    except FileNotFoundError:
        raise FileNotFoundError(f"Error: Configuration file not found at {file_path}")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Error: Failed to decode JSON. {e}", file_path, 0)
    except Exception as e:
        raise Exception(f"Unexpected error: {e}")


logger = get_logger(__name__)


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
    if not isinstance(df, DataFrame):
        logger.error("The 'df' parameter must be a Spark DataFrame.")
        raise TypeError("The 'df' parameter must be a Spark DataFrame.")
    if not isinstance(colname, str):
        logger.error("The 'colname' parameter must be a string.")
        raise TypeError("The 'colname' parameter must be a string.")

    if colname not in df.columns:
        logger.error(f"Column '{colname}' does not exist in the DataFrame.")
        raise ValueError(f"Column '{colname}' does not exist in the DataFrame.")

    try:
        dtype = [dtype for name, dtype in df.dtypes if name == colname][0]
        logger.info(f"Data type of column '{colname}' is {dtype}.")
        return dtype
    except IndexError:
        logger.error(f"Unexpected error: Column '{colname}' could not be found.")
        raise ValueError(f"Unexpected error: Column '{colname}' could not be found.")


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

        logger.info(f"DF written in {format} format to: {output_path}")
    except Exception as e:
        logger.error(f"Failed to write DataFrame in {format} format. Error: {e}")
        raise
