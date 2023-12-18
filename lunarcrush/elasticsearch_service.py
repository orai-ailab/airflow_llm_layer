try:
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk
except ImportError:
    import os
    os.system('pip install elasticsearch==7.17.9')
    from elasticsearch import Elasticsearch
    from elasticsearch.helpers import bulk


def connect(es_username, es_password, es_host, es_port):
    client = Elasticsearch(f"{es_host}:{es_port}/",
                           http_auth=(es_username, es_password))
    return client


def check_or_create_index(index, client):
    if not client.indices.exists(index=index):
        client.indices.create(index=index)


def search(client, index, field, value):
    search_query = {
        "query": {
            "match": {
                field: value
            }
        }
    }

    return client.search(index=index, body=search_query)


def create_or_update(client, index, condition_field, data):
    assets_to_update = [
        {
            "_op_type": "update",
            "_index": index,
            "_id": str(item[condition_field]),
            "doc": item,
            "upsert": item
        }
        for item in data
    ]

    try:
        success, failed = bulk(client, assets_to_update,
                               index=index, raise_on_error=True)
        print(f"Successfully updated or inserted {success} documents.")
        if failed:
            print(f"Failed to update or insert {failed} documents.")
    except Exception as e:
        print(f"Error updating or inserting documents: {e}")
