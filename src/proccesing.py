import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.utils import AnalysisException

from src.utils import get_logger

logger = get_logger(__name__)


def load_csv(spark: SparkSession, path: str, schema: StructType = None) -> DataFrame:
    """
    Loads a CSV file from a specified path with an optional schema.

    Args:
        spark (SparkSession): The active Spark session.
        path (str): Path to the CSV file.
        schema (StructType, optional): Schema to enforce on the CSV file. If not provided, infers the schema.

    Returns:
        DataFrame: Spark DataFrame containing the loaded data.

    Raises:
        TypeError: If the Spark session, path, or schema is not of the correct type.
        ValueError: If the path does not exist or the file is not a CSV.
        AnalysisException: If there is an issue loading the file.
    """
    # Validate input types
    if not isinstance(spark, SparkSession):
        logger.error("The 'spark' parameter must be a valid SparkSession.")
        raise TypeError("The 'spark' parameter must be a valid SparkSession.")
    if not isinstance(path, str):
        logger.error("The 'path' parameter must be a string.")
        raise TypeError("The 'path' parameter must be a string.")
    if schema is not None and not isinstance(schema, StructType):
        logger.error("The 'schema' parameter must be of type StructType or None.")
        raise TypeError("The 'schema' parameter must be of type StructType or None.")

    # Check if the file path exists
    if not os.path.exists(path):
        logger.error(f"The specified path '{path}' does not exist.")
        raise ValueError(f"The specified path '{path}' does not exist.")

    # Check if the file has a .csv extension
    if not path.lower().endswith(".csv"):
        logger.error(f"The specified file '{path}' is not a CSV file.")
        raise ValueError(f"The specified file '{path}' is not a CSV file.")

    try:
        # Load the CSV file with or without schema
        if schema:
            df = (
                spark.read.format("csv")
                .option("header", "true")
                .schema(schema)
                .load(path)
            )
        else:
            df = spark.read.format("csv").option("header", "true").load(path)

        logger.info(f"Successfully loaded CSV file from path: {path}")
        return df
    except AnalysisException as e:
        logger.error(f"Failed to load CSV file from path '{path}'. Error: {e}")
        raise ValueError(f"Failed to load CSV file from path '{path}'. Error: {e}")
