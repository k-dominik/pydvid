import http.client
import contextlib
from pydvid.errors import DvidHttpError, UnexpectedResponseError
from pydvid.util import get_json_generic
import json


def create_new(connection, uuid, data_name):
    """
    Create a new keyvalue table in the dvid server.
    """
    rest_cmd = "/api/repo/{uuid}/instance".format(uuid=uuid)
    config_data = {}
    config_data["dataname"] = data_name
    config_data["typename"] = "keyvalue"
    response = connection.post(rest_cmd, data=json.dumps(config_data))
    if response.status_code != 200:
        raise DvidHttpError(
            "keyvalue.create_new",
            response.status_code,
            response.reason,
            response.text,
            "POST",
            rest_cmd
        )


def get_value(connection, uuid, data_name, key):
    """
    Request the value for the given key and return the whole thing.
    """
    response = get_value_response(connection, uuid, data_name, key)
    return response.text


def put_value(connection, uuid, data_name, key, value):
    """
    Store the given value to the keyvalue data.
    value should be either str or a file-like object with fileno() and read() methods.
    """
    rest_cmd = f"/api/node/{uuid}/{data_name}/{key}"
    headers = {"Content-Type": "application/octet-stream"}
    response = connection.post(rest_cmd, data=value, headers=headers)
    if response.status_code != 200:
        raise DvidHttpError(
            "keyvalue post",
            response.status_code,
            response.reason,
            response.text,
            "POST",
            rest_cmd,
            "<binary data>",
            headers
        )


def del_value(connection, uuid, data_name, key, value):
    raise NotImplementedError('Todo!')


def get_keys(connection, uuid, data_name):
    rest_query = f"/api/node/{uuid}/{data_name}/keys"
    return get_json_generic(connection, rest_query, schema='dvid-keyvalue-keys-v0.01.schema.json')


def get_value_response(connection, uuid, data_name, key):
    """
    Request the value for the given key return the raw HTTPResponse object.
    The caller may opt to 'stream' the data from the response instead of reading it all at once.
    """
    rest_query = f"/api/node/{uuid}/{data_name}/{key}"
    response = connection.get(rest_query)
    if response.status_code != 200:
        raise DvidHttpError(
            "keyvalue request",
            response.status_code,
            response.reason,
            response.text,
            "GET",
            rest_query,
            ""
        )
    return response


if __name__ == "__main__":
    import http.client
    conn = http.client.HTTPConnection("localhost:8000")
    put_value( conn, '4a', 'greetings', 'english', 'hello' )
    print("Got greeting: ", get_value( conn, '4a', 'greetings', 'english'))
    
