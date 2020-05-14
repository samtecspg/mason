import os

import yaml
from util.yaml import parse_yaml

def update_yaml_file(base_swagger: str, directory: str):
    swagger_file = "api/swagger.yml"
    parsed_swagger = parse_yaml(base_swagger) or {}
    paths: dict = parsed_swagger["paths"]

    for r, d, f in os.walk(directory):
        for file in f:
            if '.yml' in file or '.yaml' in file:
                file_path = os.path.join(r, file)
                if file == "swagger.yml" or file == "swagger.yaml":
                    file_parsed = parse_yaml(file_path) or {}

                    parsed_paths = file_parsed.get('paths') or {}
                    if len(parsed_paths) > 0:
                        paths.update(parsed_paths)

    parsed_swagger['paths'] = paths
    with open(swagger_file, 'w+') as file: # type: ignore
        yaml.dump(parsed_swagger, file) # type: ignore
