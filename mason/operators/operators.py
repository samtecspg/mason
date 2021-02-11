from sys import path
from mason.util.swagger import update_yaml_file
from mason.util.environment import MasonEnvironment


def import_all(env: MasonEnvironment):
    path.append(env.mason_home)
    #TODO: Fix
    # namespaces, invalid = list_namespaces(env)
    # for namespace in namespaces:
    #     for op in namespace.operators:
    #         cmd = op.command
    #         import_module(f"{env.operator_module}.{namespace.namespace}.{cmd}")

def update_yaml(env: MasonEnvironment, base_swagger: str):
    update_yaml_file(base_swagger, [env.state_store.operator_home, env.state_store.workflow_home])

