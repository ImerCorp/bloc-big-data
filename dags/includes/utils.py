import pandas as pd
from datetime import datetime, timedelta, time, date

def serialize_value(value):
    """Convert non-JSON-serializable types to strings"""
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    elif isinstance(value, time):
        return value.isoformat()
    elif isinstance(value, timedelta):
        return str(value)
    elif pd.isna(value):
        return None
    return value

def serialize_results(results):
    """Serialize query results to make them JSON-compatible"""
    if not results:
        return []
    
    serialized = []
    for row in results:
        if isinstance(row, dict):
            serialized_row = {key: serialize_value(value) for key, value in row.items()}
            serialized.append(serialized_row)
        else:
            # If row is a tuple or list
            serialized.append([serialize_value(val) for val in row])
    
    return serialized
