import azure.functions as func
import logging
import os
from azure.data.tables import TableServiceClient, UpdateMode
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError
import json
import uuid

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Get connection string and table name from environment variables
connection_string = os.environ.get("AZURE_TABLE_STORAGE_CONNECTION_STRING")
table_name = os.environ.get("AZURE_TABLE_STORAGE_TABLE_NAME")

# Check if environment variables are set
if not connection_string or not table_name:
    logging.error("AZURE_TABLE_STORAGE_CONNECTION_STRING and AZURE_TABLE_STORAGE_TABLE_NAME must be set as environment variables.")
    # You might want to raise an exception or handle this differently
    # For now, we'll let it fail when trying to create the clients

# Create TableServiceClient and TableClient
table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string)
table_client = table_service_client.get_table_client(table_name=table_name)

try:
    table_client.create_table()
    logging.info(f"Table '{table_name}' created.")
except HttpResponseError:
    logging.info(f"Table '{table_name}' already exists.")


@app.route(route="data/{partitionKey?}/{rowKey?}")
def crud_api(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    partition_key = req.route_params.get('partitionKey')
    row_key = req.route_params.get('rowKey')
    
    if req.method == "POST":
        return create_entity(req)
    elif req.method == "GET":
        if partition_key and row_key:
            return get_entity(partition_key, row_key)
        else:
            return query_entities(req)
    elif req.method == "PUT":
        if partition_key and row_key:
            return update_entity(req, partition_key, row_key)
        else:
            return func.HttpResponse(
                "PartitionKey and RowKey are required for PUT operations.",
                status_code=400
            )
    elif req.method == "DELETE":
        if partition_key and row_key:
            return delete_entity(partition_key, row_key)
        else:
            return func.HttpResponse(
                "PartitionKey and RowKey are required for DELETE operations.",
                status_code=400
            )
    else:
        return func.HttpResponse(
            "Method not supported.",
            status_code=405
        )

def create_entity(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON format.", status_code=400)

    # Use 'data' as the PartitionKey if not provided
    partition_key = req_body.get("PartitionKey", "data")
    
    entity = {
        "PartitionKey": partition_key,
        "RowKey": str(uuid.uuid4()),
        "data": json.dumps(req_body.get('data')) # Store the data model as a JSON string
    }

    try:
        table_client.create_entity(entity=entity)
        return func.HttpResponse(json.dumps(entity), status_code=201, mimetype="application/json")
    except HttpResponseError as e:
        return func.HttpResponse(f"Error creating entity: {e}", status_code=500)

def get_entity(partition_key: str, row_key: str) -> func.HttpResponse:
    try:
        entity = table_client.get_entity(partition_key=partition_key, row_key=row_key)
        entity['data'] = json.loads(entity['data']) # Deserialize the data string
        return func.HttpResponse(json.dumps(entity), status_code=200, mimetype="application/json")
    except ResourceNotFoundError:
        return func.HttpResponse("Entity not found.", status_code=404)
    except Exception as e:
        return func.HttpResponse(f"Error getting entity: {e}", status_code=500)

def query_entities(req: func.HttpRequest) -> func.HttpResponse:
    try:
        filter_query = req.params.get('$filter')
        entities = table_client.query_entities(query_filter=filter_query if filter_query else "")
        
        results = []
        for entity in entities:
            entity['data'] = json.loads(entity['data'])
            results.append(entity)

        return func.HttpResponse(json.dumps(results), status_code=200, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error querying entities: {e}", status_code=500)

def update_entity(req: func.HttpRequest, partition_key: str, row_key: str) -> func.HttpResponse:
    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON format.", status_code=400)

    entity = {
        "PartitionKey": partition_key,
        "RowKey": row_key,
        "data": json.dumps(req_body.get('data'))
    }

    try:
        table_client.update_entity(entity, mode=UpdateMode.REPLACE)
        return func.HttpResponse(status_code=204)
    except ResourceNotFoundError:
        return func.HttpResponse("Entity not found.", status_code=404)
    except Exception as e:
        return func.HttpResponse(f"Error updating entity: {e}", status_code=500)

def delete_entity(partition_key: str, row_key: str) -> func.HttpResponse:
    try:
        table_client.delete_entity(partition_key=partition_key, row_key=row_key)
        return func.HttpResponse(status_code=204)
    except ResourceNotFoundError:
        return func.HttpResponse("Entity not found.", status_code=404)
    except Exception as e:
        return func.HttpResponse(f"Error deleting entity: {e}", status_code=500)
