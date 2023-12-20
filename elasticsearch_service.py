from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def connect(es_username, es_password, es_host, es_port):
    client = Elasticsearch("{}:{}/".format(es_host, es_port),
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
            "_op_type": "index",
            "_index": index,
            "_id": str(item[condition_field]),
            "_source": item
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


def insert_many(client, index, datas):
    print("🚀 ~ file: elasticsearch_service.py:49 ~ datas:", datas)
    insert_actions = [
        {
            # "_op_type": "insert",
            "_index": index,
            # "_id":  uuid.uuid4(),
            "_source": item
        }
        for item in datas
    ]
    try:
        success, failed = bulk(client, insert_actions,
                               index=index, raise_on_error=True)
        print(f"Successfully updated or inserted {success} documents.")
        if failed:
            print(f"Failed to update or insert {failed} documents.")

    except Exception as e:
        print(f"Error updating or inserting documents: {e}")
