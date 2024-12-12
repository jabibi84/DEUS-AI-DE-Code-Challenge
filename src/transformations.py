from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


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
