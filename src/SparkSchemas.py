from pyspark.sql.types import StructType, StructField, StringType, FloatType, DateType


class SchemaManager:
    """
    Clase para gestionar y devolver esquemas basados en el nombre del dataset.
    """

    @staticmethod
    def get_schema(schema_name):
        """
        Devuelve el esquema correspondiente al nombre del dataset.

        Args:
            schema_name (str): Nombre del esquema requerido.

        Returns:
            StructType: Esquema de PySpark correspondiente.

        Raises:
            ValueError: Si el nombre del esquema no es válido.
        """
        schemas = {
            "SalesTransactions": StructType(
                [
                    StructField("transaction_id", StringType(), True),
                    StructField("store_id", StringType(), True),
                    StructField("product_id", StringType(), True),
                    StructField("quantity", FloatType(), True),
                    StructField("transaction_date", DateType(), True),
                    StructField("price", FloatType(), True),
                ]
            ),
            "Products": StructType(
                [
                    StructField("product_id", StringType(), True),
                    StructField("product_name", StringType(), True),
                    StructField("category", StringType(), True),
                ]
            ),
            "Stores": StructType(
                [
                    StructField("store_id", StringType(), True),
                    StructField("store_name", StringType(), True),
                    StructField("location", StringType(), True),
                ]
            ),
        }

        if schema_name not in schemas:
            raise ValueError(
                f"El esquema '{schema_name}' no está definido. Esquemas disponibles: {list(schemas.keys())}"
            )

        return schemas[schema_name]
