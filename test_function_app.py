import unittest
from unittest.mock import MagicMock, patch
import json
import os
import azure.functions as func
from function_app import crud_api, table_client

class TestCrudApi(unittest.TestCase):

    def setUp(self):
        # Set up environment variables for tests
        os.environ["AZURE_TABLE_STORAGE_CONNECTION_STRING"] = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;"
        os.environ["AZURE_TABLE_STORAGE_TABLE_NAME"] = "testtable"

    @patch('azure.data.tables.TableClient')
    def test_create_entity(self, mock_table_client):
        """
        Test creating a new entity.
        """
        # Arrange
        req = func.HttpRequest(
            method='POST',
            body=json.dumps({'data': {'message': 'Hello, World!'}, 'PartitionKey': 'testpartition'}).encode('utf-8'),
            url='/api/data'
        )
        
        # Mock the TableClient's create_entity method
        mock_table_client.create_entity = MagicMock()

        # Act
        resp = crud_api(req)

        # Assert
        self.assertEqual(resp.status_code, 201)
        mock_table_client.create_entity.assert_called_once()
        
        # Check the response body
        resp_body = json.loads(resp.get_body())
        self.assertEqual(resp_body['PartitionKey'], 'testpartition')
        self.assertIn('RowKey', resp_body)
        self.assertEqual(json.loads(resp_body['data']), {'message': 'Hello, World!'})

    @patch('azure.data.tables.TableClient')
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
            "data": json.dumps({"message": "Hello!"})
        }
        
        req = func.HttpRequest(
            method='GET',
            url=f'/api/data/{partition_key}/{row_key}',
            route_params={'partitionKey': partition_key, 'rowKey': row_key}
        )
        
        # Mock the TableClient's get_entity method
        mock_table_client.get_entity = MagicMock(return_value=entity)

        # Act
        resp = crud_api(req)

        # Assert
        self.assertEqual(resp.status_code, 200)
        mock_table_client.get_entity.assert_called_with(partition_key=partition_key, row_key=row_key)
        
        # Check the response body
        resp_body = json.loads(resp.get_body())
        self.assertEqual(resp_body, {"PartitionKey": partition_key, "RowKey": row_key, "data": {"message": "Hello!"}})

    @patch('azure.data.tables.TableClient')
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
        
        # Mock the TableClient's update_entity method
        mock_table_client.update_entity = MagicMock()

        # Act
        resp = crud_api(req)

        # Assert
        self.assertEqual(resp.status_code, 204)
        mock_table_client.update_entity.assert_called_once()
        
    @patch('azure.data.tables.TableClient')
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
        
        # Mock the TableClient's delete_entity method
        mock_table_client.delete_entity = MagicMock()

        # Act
        resp = crud_api(req)

        # Assert
        self.assertEqual(resp.status_code, 204)
        mock_table_client.delete_entity.assert_called_with(partition_key=partition_key, row_key=row_key)

if __name__ == '__main__':
    unittest.main()
