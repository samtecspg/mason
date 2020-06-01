import os
from typing import List

import yaml

from mason.definitions import from_root
from mason.util.yaml import parse_yaml

def update_yaml_file(base_swagger: str, directories: List[str]):
    swagger_file = from_root("/api/swagger.yml")
    parsed_swagger = parse_yaml(base_swagger) or {}
    paths: dict = parsed_swagger["paths"]

    for directory in directories:
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
    with open(swagger_file, 'w+') as file: #type: ignore
        yaml.dump(parsed_swagger, file) #type: ignore
