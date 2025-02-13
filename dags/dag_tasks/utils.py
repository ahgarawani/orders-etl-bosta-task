import pandas as pd

def escape_value(value: any) -> str:
    """
    Escape values for SQL insertion, handling NULLs and special characters.

    Args:
        value (any): The value to be escaped.

    Returns:
        str: The escaped value as a string suitable for SQL insertion.
    """
    if pd.isnull(value):
        return 'NULL'
    elif isinstance(value, str):
        return f"""'{value.replace("'", "''")}'"""
    else:
        return repr(value)