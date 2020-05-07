from typing import Optional

from util.environment import MasonEnvironment


class DagStep:
    def __init__(self, namespace: str, command: str):
        self.namespace = namespace
        self.command = command

def from_dict(dict) -> Optional[DagStep]:
    ns = dict.get("namespace")
    command = dict.get("command")
    if ns and command:
        return DagStep(ns, command)
    else:
        return None

