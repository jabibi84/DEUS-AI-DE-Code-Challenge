from pyspark.sql import DataFrame, SparkSession


def load_data(spark: SparkSession, path: str, file_format: str = "csv") -> DataFrame:
    """
    Loads any kind of file from a specific file path.

    Args:
        path (str): Path to the file.
        file_format (str): Format of the file (default is 'csv').

    Returns:
        DataFrame: Spark DataFrame containing the loaded data.
    """
    return spark.read.format(file_format).option("header", "true").load(path)
