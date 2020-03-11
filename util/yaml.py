import yaml

def parse_yaml(file: str):
    try:
        with open(file, 'r') as stream:
            try:
                return yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(f"Invalid YAML: {exc}")
    except FileNotFoundError as e:
        print(f"Specified YAML does not exist: {e}")

