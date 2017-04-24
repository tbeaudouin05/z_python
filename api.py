# functions to use on the anaplan api

import os
import requests
import json

def get_model_id(model_name, workspace, header, user):
    """Gets and returns id of the model."""
    uri = "https://api.anaplan.com/1/3/workspaces/{}/models/".format(workspace)
    response = requests.get(uri, headers = header)
    response_json = json.loads(models.text.encode("utf-8"))
    for model in response_json:
        if model[u"name"] == unicode(model_name):
           return model[u"id"]

def get_file_id_dict(model, workspace, header):
    """Gets and returns the id of the files in a model as a dictionary."""
    file_dict = {}
    uri = ("https://api.anaplan.com/1/3/workspaces/{}/models/{}/"
        "files/").format(workspace, model)
    response = requests.get(uri, headers = header)
    response_json = json.loads(response.text.encode("utf-8"))
    for file in response_json:
        file_dict[file["name"]] = file["id"]
    return file_dict

def get_file_id(file_name, model, workspace, header, user):
    """Gets and returns the id of the file."""
    uri = ("https://api.anaplan.com/1/3/workspaces/{}/models/{}/"
        "files/").format(workspace, model)
    response = requests.get(uri, headers = header)
    response_json = json.loads(response.text.encode("utf-8"))
    for file in response_json:
        if file[u"name"] == unicode(file_name):
            return file[u"id"]

def get_file_chunks(file, model, workspace, header, user):
    """Gets and returns the id of the file chunk."""
    uri = ("https://api.anaplan.com/1/3/workspaces/{}/models/{}/"
        "files/{}/chunks/").format(workspace, model, file)
    response = requests.get(uri, headers = header)
    return json.loads(response.text.encode("utf-8"))

def upload_file(file_name, file_id, model, workspace, header_put):
    """Takes a csv file and uploads it to Anaplan."""
    filename = "data_files/{}".format(file_name)
    data_file = open(filename, "r")
    data = data_file.read()
    data_file.close()

    uri = ("https://api.anaplan.com/1/3/workspaces/{}/models/{}/"
    "files/{}/chunks/0").format(workspace, model, file_id)
    response = requests.put(uri, headers = header_put, data = data)
    return response.status_code

def get_import_id_dict(model, workspace, header):
    """Gets and returns the id of the imports in a model as a dictionary."""
    import_dict = {}
    uri = ("https://api.anaplan.com/1/3/workspaces/{}/models/{}/"
        "imports/").format(workspace, model)
    response = requests.get(uri, headers = header)
    response_json = json.loads(response.text.encode("utf-8"))
    for imp in response_json:
        import_dict[imp["name"]] = imp["id"]
    return import_dict

def get_import_id(import_name, model, workspace, header, user):
    """Gets and returns the id of the import."""
    uri = ("https://api.anaplan.com/1/3/workspaces/{}/models/{}/"
        "imports/").format(workspace, model)
    response = requests.get(uri, headers = header)
    response_json = json.loads(response.text.encode("utf-8"))
    for imp in response_json:
        if imp[u"name"] == unicode(import_name):
            return imp[u"id"]

def trigger_import(import_id, model, workspace, header):
    """Triggers the import action in Anaplan."""
    uri = ("https://api.anaplan.com/1/3/workspaces/{}/models/{}/"
        "imports/{}/tasks/").format(workspace, model, import_id)
    response = requests.post(uri, headers = header,
        data = '{"localeName": "en_US"}')
    return response.status_code
