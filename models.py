from pydantic import BaseModel
from typing import Dict, Any

class GenericEntity(BaseModel):
    """
    A generic entity for Azure Table Storage.
    Requires PartitionKey and RowKey.
    The 'data' field can hold any JSON-serializable data model.
    """
    PartitionKey: str
    RowKey: str
    data: Dict[str, Any]
