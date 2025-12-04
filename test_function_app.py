import unittest
from unittest.mock import MagicMock, patch, mock_open
import json
import os
import azure.functions as func

# Set environment variables before importing the function app
os.environ["AZURE_TABLE_STORAGE_CONNECTION_STRING"] = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
os.environ["AZURE_TABLE_STORAGE_TABLE_NAME"] = "testtable"

from function_app import crud_api

class TestCrudApi(unittest.TestCase):

    @patch('function_app.table_client')
    def test_create_entity(self, mock_table_client):
        """
        Test creating a new entity with PartitionKey and RowKey.
        """
        # Arrange
        req_body = {
            "PartitionKey": "testpartition",
            "RowKey": "testrow",
            "data": {"message": "Hello, World!"}
        }
        req = func.HttpRequest(
            method='POST',
            body=json.dumps(req_body).encode('utf-8'),
            url='/api/data'
        )
        
        # Act
        resp = crud_api(req)

        # Assert
        self.assertEqual(resp.status_code, 201)
        mock_table_client.create_entity.assert_called_once()
        
        # Check the response body
        resp_body = json.loads(resp.get_body())
        self.assertEqual(resp_body['PartitionKey'], 'testpartition')
        self.assertEqual(resp_body['RowKey'], 'testrow')
        self.assertEqual(resp_body['data'], {'message': 'Hello, World!'})

    @patch('function_app.table_client')
    def test_create_entity_missing_keys(self, mock_table_client):
        """
        Test creating an entity with missing PartitionKey or RowKey.
        """
        # Arrange
        req_body = {"data": {"message": "This should fail"}}
        req = func.HttpRequest(
            method='POST',
            body=json.dumps(req_body).encode('utf-8'),
            url='/api/data'
        )
        
        # Act
        resp = crud_api(req)

        # Assert
        self.assertEqual(resp.status_code, 400)
        mock_table_client.create_entity.assert_not_called()

    @patch('function_app.table_client')
    def test_get_entity(self, mock_table_client):
        """
        Test retrieving an existing entity.
        """
        # Arrange
        partition_key = "testpartition"
        row_key = "testrow"
        entity = {
            "PartitionKey": partition_key,
            "RowKey": row_key,
            "data": {"message": "Hello!"}
        }
        
        req = func.HttpRequest(
            method='GET',
            url=f'/api/data/{partition_key}/{row_key}',
            route_params={'partitionKey': partition_key, 'rowKey': row_key}
        )
        
        mock_table_client.get_entity.return_value = entity

        # Act
        resp = crud_api(req)

        # Assert
        self.assertEqual(resp.status_code, 200)
        mock_table_client.get_entity.assert_called_with(partition_key, row_key)
        
        # Check the response body
        resp_body = json.loads(resp.get_body())
        self.assertEqual(resp_body, entity)

    @patch('function_app.table_client')
    def test_update_entity(self, mock_table_client):
        """
        Test updating an existing entity.
        """
        # Arrange
        partition_key = "testpartition"
        row_key = "testrow"
        req_body = {'data': {'message': 'Updated message'}}
        
        req = func.HttpRequest(
            method='PUT',
            body=json.dumps(req_body).encode('utf-8'),
            url=f'/api/data/{partition_key}/{row_key}',
            route_params={'partitionKey': partition_key, 'rowKey': row_key}
        )
        
        # Act
        resp = crud_api(req)

        # Assert
        self.assertEqual(resp.status_code, 204)
        mock_table_client.update_entity.assert_called_once()
        
    @patch('function_app.table_client')
    def test_delete_entity(self, mock_table_client):
        """
        Test deleting an entity.
        """
        # Arrange
        partition_key = "testpartition"
        row_key = "testrow"
        
        req = func.HttpRequest(
            method='DELETE',
            url=f'/api/data/{partition_key}/{row_key}',
            route_params={'partitionKey': partition_key, 'rowKey': row_key}
        )
        
        # Act
        resp = crud_api(req)

        # Assert
        self.assertEqual(resp.status_code, 204)
        mock_table_client.delete_entity.assert_called_with(partition_key, row_key)

if __name__ == '__main__':
    unittest.main()
