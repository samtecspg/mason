
import json
# from jsonschema import validate as js_validate
import os
from configurations.metastore.glue.client import GlueMetastoreClient

def validate(mason_configuration):
    print("TODO")
    # path = os.path.dirname(os.path.abspath(__file__)) + "/schema.json"
    # with open(path, 'r') as schema_json:
    #     schema = json.load(schema_json)
    #     js_validate(instance=mason_configuration, schema=schema)
    #     print(f"Valid Mason Configuration {mason_configuration}")
    #     configurations = mason_configuration.get("configurations")
    #
    #     if configurations:
    #         metastore_configuration = configurations.get("metastore")
    #         if metastore_configuration:
    #             validate_metastore_configuration(metastore_configuration)
    #             print(metastore_configuration)

def validate_metastore_configuration(metastore_configuration: dict):
    # if metastore_configuration.get("client") == "glue":
        print("TODO")
        # GlueMetastoreClient().validate(metastore_configuration)
