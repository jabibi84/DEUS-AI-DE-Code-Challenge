from SparkSchemas import SchemaManager


def test_create_schema():
    schema_definition = {
        "fields": [
            {"name": "name", "type": "string", "nullable": True},
            {"name": "age", "type": "integer", "nullable": True},
        ]
    }
    schema = SchemaManager.create_schema(schema_definition)
    assert schema.fieldNames() == ["name", "age"]
