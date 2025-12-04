import os
import json
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
from models import GenericEntity

class TableStorageClient:
    def __init__(self):
        connection_string = os.environ.get("AZURE_TABLE_STORAGE_CONNECTION_STRING")
        self.table_name = os.environ.get("AZURE_TABLE_STORAGE_TABLE_NAME")

        if not connection_string or not self.table_name:
            raise ValueError("AZURE_TABLE_STORAGE_CONNECTION_STRING and AZURE_TABLE_STORAGE_TABLE_NAME must be set.")

        self.table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string)
        self.table_client = self.table_service_client.get_table_client(table_name=self.table_name)
        self._create_table_if_not_exists()

    def _create_table_if_not_exists(self):
        try:
            self.table_client.create_table()
            print(f"Table '{self.table_name}' created.")
        except HttpResponseError:
            print(f"Table '{self.table_name}' already exists.")

    def create_entity(self, entity: GenericEntity):
        table_entity = {
            "PartitionKey": entity.PartitionKey,
            "RowKey": entity.RowKey,
            "data": json.dumps(entity.data)
        }
        return self.table_client.create_entity(entity=table_entity)

    def get_entity(self, partition_key: str, row_key: str):
        entity = self.table_client.get_entity(partition_key=partition_key, row_key=row_key)
        entity['data'] = json.loads(entity['data'])
        return entity

    def query_entities(self, filter_query: str):
        entities = self.table_client.query_entities(query_filter=filter_query if filter_query else "")
        results = []
        for entity in entities:
            entity['data'] = json.loads(entity['data'])
            results.append(entity)
        return results

    def update_entity(self, entity: GenericEntity):
        table_entity = {
            "PartitionKey": entity.PartitionKey,
            "RowKey": entity.RowKey,
            "data": json.dumps(entity.data)
        }
        self.table_client.update_entity(table_entity, mode=UpdateMode.REPLACE)

    def delete_entity(self, partition_key: str, row_key: str):
        self.table_client.delete_entity(partition_key=partition_key, row_key=row_key)
