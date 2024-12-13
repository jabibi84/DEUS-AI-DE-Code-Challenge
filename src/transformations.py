from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum, year, month


def calculate_total_revenue(sales_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Calculate the total revenue for each store and product category.

    Args:
        sales_df (DataFrame): Sales transactions dataset.
        products_df (DataFrame): Products dataset.

    Returns:
        DataFrame: Aggregated DataFrame with store_id, category, and total_revenue.
    """
    # Join sales and products datasets to include category
    sales_with_category = sales_df.join(products_df, on="product_id")

    # Calculate total revenue per store and category
    total_revenue_df = sales_with_category.groupBy("store_id", "category").agg(
        sum(col("quantity") * col("price")).alias("total_revenue")
    )

    return total_revenue_df


def calculate_monthly_sales(sales_df: DataFrame, products_df: DataFrame) -> DataFrame:
    """
    Calculate the total quantity sold for each product category, grouped by month.

    Args:
        sales_df (DataFrame): Sales transactions dataset.
        products_df (DataFrame): Products dataset.

    Returns:
        DataFrame: Aggregated DataFrame with year, month, category, and total_quantity_sold.
    """
    # Extract year and month from transaction_date
    sales_with_date = sales_df.withColumn(
        "year", year(col("transaction_date"))
    ).withColumn("month", month(col("transaction_date")))

    # Join with products dataset to include category
    sales_with_category = sales_with_date.join(products_df, on="product_id")

    # Calculate total quantity sold grouped by year, month, and category
    monthly_sales_df = sales_with_category.groupBy("year", "month", "category").agg(
        sum("quantity").alias("total_quantity_sold")
    )

    return monthly_sales_df


def enrich_data(
    sales_df: DataFrame, products_df: DataFrame, stores_df: DataFrame
) -> DataFrame:
    """
    Combine sales, products, and stores datasets into a single enriched dataset.

    Args:
        sales_df (DataFrame): Sales transactions dataset.
        products_df (DataFrame): Products dataset.
        stores_df (DataFrame): Stores dataset.

    Returns:
        DataFrame: Enriched DataFrame with transaction_id, store_name, location,
                   product_name, category, quantity, transaction_date, and price.
    """
    # Join sales with products
    sales_with_products = sales_df.join(products_df, on="product_id")

    # Join the result with stores
    enriched_data = sales_with_products.join(stores_df, on="store_id")

    # Select and reorder the desired columns
    enriched_data = enriched_data.select(
        "transaction_id",
        "store_name",
        "location",
        "product_name",
        "category",
        "quantity",
        "transaction_date",
        "price",
    )

    return enriched_data


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


def write_dataframe(
    df, output_path, format="parquet", partition_by=None, mode="overwrite", header=True
):
    """
    Function to write a PySpark DataFrame in a specified format with optional partitioning.

    Args:
        df (DataFrame): The PySpark DataFrame to be written.
        output_path (str): The path where the file will be saved.
        format (str): Output format. Options: "parquet", "csv", "json", "avro". Default: "parquet".
        partition_by (list): List of columns to partition the data by. Default: None.
        mode (str): Write mode. Options: "overwrite", "append", "ignore", "error". Default: "overwrite".
        header (bool): If True, includes a header in CSV files. Default: True (only applies to CSV).
    """
    writer = df.write.mode(mode)

    if partition_by:
        writer = writer.partitionBy(*partition_by)

    if format == "csv":
        writer.option("header", header).csv(output_path)
    elif format == "parquet":
        writer.parquet(output_path)
    elif format == "json":
        writer.json(output_path)
    elif format == "avro":
        try:
            writer.format("avro").save(output_path)
        except Exception as e:
            raise ValueError(
                f"Failed to write in Avro format. Make sure Avro dependencies are available. Error: {e}"
            )
    else:
        raise ValueError(
            f"Unsupported format '{format}'. Use 'parquet', 'csv', 'json', or 'avro'."
        )

    print(f"DataFrame written in {format} format to: {output_path}")
