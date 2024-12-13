
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
