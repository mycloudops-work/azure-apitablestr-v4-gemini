import azure.functions as func
import logging
import json
from pydantic import ValidationError
from azure.core.exceptions import ResourceNotFoundError, HttpResponseError

from models import GenericEntity
from table_storage_client import TableStorageClient

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

try:
    table_client = TableStorageClient()
except ValueError as e:
    logging.error(str(e))
    table_client = None

@app.route(route="data/{partitionKey?}/{rowKey?}")
def crud_api(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    if not table_client:
        return func.HttpResponse(
            "Table storage not configured. Please set environment variables.",
            status_code=500
        )

    partition_key = req.route_params.get('partitionKey')
    row_key = req.route_params.get('rowKey')
    
    if req.method == "POST":
        return create_entity_handler(req)
    elif req.method == "GET":
        if partition_key and row_key:
            return get_entity_handler(partition_key, row_key)
        else:
            return query_entities_handler(req)
    elif req.method == "PUT":
        if partition_key and row_key:
            return update_entity_handler(req, partition_key, row_key)
        else:
            return func.HttpResponse(
                "PartitionKey and RowKey are required for PUT operations.",
                status_code=400
            )
    elif req.method == "DELETE":
        if partition_key and row_key:
            return delete_entity_handler(partition_key, row_key)
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

def create_entity_handler(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        entity = GenericEntity(**req_body)
        table_client.create_entity(entity)
        return func.HttpResponse(entity.model_dump_json(), status_code=201, mimetype="application/json")
    except ValueError:
        return func.HttpResponse("Invalid JSON format.", status_code=400)
    except ValidationError as e:
        return func.HttpResponse(e.json(), status_code=400, mimetype="application/json")
    except HttpResponseError as e:
        return func.HttpResponse(f"Error creating entity: {e}", status_code=500)

def get_entity_handler(partition_key: str, row_key: str) -> func.HttpResponse:
    try:
        entity = table_client.get_entity(partition_key, row_key)
        return func.HttpResponse(json.dumps(entity), status_code=200, mimetype="application/json")
    except ResourceNotFoundError:
        return func.HttpResponse("Entity not found.", status_code=404)
    except Exception as e:
        return func.HttpResponse(f"Error getting entity: {e}", status_code=500)

def query_entities_handler(req: func.HttpRequest) -> func.HttpResponse:
    try:
        filter_query = req.params.get('$filter')
        entities = table_client.query_entities(filter_query)
        return func.HttpResponse(json.dumps(entities), status_code=200, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(f"Error querying entities: {e}", status_code=500)

def update_entity_handler(req: func.HttpRequest, partition_key: str, row_key: str) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        # Ensure the keys from the URL are used
        req_body['PartitionKey'] = partition_key
        req_body['RowKey'] = row_key
        entity = GenericEntity(**req_body)
        table_client.update_entity(entity)
        return func.HttpResponse(status_code=204)
    except ValueError:
        return func.HttpResponse("Invalid JSON format.", status_code=400)
    except ValidationError as e:
        return func.HttpResponse(e.json(), status_code=400, mimetype="application/json")
    except ResourceNotFoundError:
        return func.HttpResponse("Entity not found.", status_code=404)
    except Exception as e:
        return func.HttpResponse(f"Error updating entity: {e}", status_code=500)

def delete_entity_handler(partition_key: str, row_key: str) -> func.HttpResponse:
    try:
        table_client.delete_entity(partition_key, row_key)
        return func.HttpResponse(status_code=204)
    except ResourceNotFoundError:
        return func.HttpResponse("Entity not found.", status_code=404)
    except Exception as e:
        return func.HttpResponse(f"Error deleting entity: {e}", status_code=500)