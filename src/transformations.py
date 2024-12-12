from pyspark.sql import DataFrame
from pyspark.sql.functions import to_date, coalesce, col, lit, when


# Define the categorization function
def categorize_price(price):
    if price < 20:
        return "Low"
    elif 20 <= price <= 100:
        return "Medium"
    elif price > 100:
        return "High"
    else:
        return None


def standardize_date_format(
    df: DataFrame, date_column: str, output_column: str = "standardized_date"
) -> DataFrame:
    """
    Converts a column with multiple date formats to the standard format YYYY-MM-DD.

    Args:
        df (DataFrame): Input Spark DataFrame.
        date_column (str): Name of the column containing dates in multiple formats.
        output_column (str): Name of the output column with standardized dates. Default is "standardized_date".

    Returns:
        DataFrame: A new DataFrame with an additional column for the standardized date.
    """
    # Define possible date formats
    date_formats = [
        "yyyy-MM-dd",  # 2023-12-01
        "MM/dd/yyyy",  # 12/01/2023
        "dd-MM-yyyy",  # 01-12-2023
        "dd/MM/yyyy",  # 01/12/2023
        "yyyyMMdd",  # 20231201
        "dd MMM yyyy",  # 01 Dec 2023
        "MMMM dd, yyyy",  # February 25, 2024
        "yyyy/MM/dd",  # 2024/11/04
    ]

    # Attempt to parse the date column using each format
    standardized_date = None
    for fmt in date_formats:
        parsed_date = to_date(col(date_column), fmt)
        standardized_date = (
            parsed_date
            if standardized_date is None
            else coalesce(standardized_date, parsed_date)
        )

    # Add the standardized column to the DataFrame
    return df.withColumn(output_column, standardized_date)
