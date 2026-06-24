from pyspark.sql.functions import col


def validate_and_cast_schema(df, expected_schema):
    """
    Validates DataFrame against StructType schema.
    Casts columns when datatype mismatches.
    Raises exception if columns are missing.
    """
    expected_fields = {
        field.name: field.dataType
        for field in expected_schema.fields
    }

    actual_fields = {
        field.name: field.dataType
        for field in df.schema.fields
    }

    # Check missing columns
    missing_columns = set(expected_fields.keys()) - set(actual_fields.keys())

    if missing_columns:
        raise Exception(
            f"Schema validation failed. Missing columns: {missing_columns}"
        )

    # Cast mismatched datatypes
    for column_name, expected_type in expected_fields.items():

        actual_type = actual_fields[column_name]

        if actual_type != expected_type:
            try:
                df = df.withColumn(
                    column_name,
                    col(column_name).cast(expected_type)
                )
            except Exception as e:
                raise Exception(
                    f"Failed casting column '{column_name}' "
                    f"from {actual_type} to {expected_type}. Error: {str(e)}"
                )

    return df