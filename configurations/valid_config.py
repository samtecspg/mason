from typing import Optional, Dict

from engines.execution import ExecutionEngine
from engines.metastore import MetastoreEngine
from engines.scheduler import SchedulerEngine
from engines.storage import StorageEngine

class ValidConfig:

    def __init__(self, id: str, config: dict, metastore_engine: MetastoreEngine, scheduler_engine: SchedulerEngine, storage_engine: StorageEngine, execution_engine: ExecutionEngine, source_path: Optional[str] = None):
        self.id = id
        self.config = config
        self.metastore = metastore_engine
        self.scheduler = scheduler_engine
        self.storage = storage_engine
        self.execution = execution_engine
        self.source_path = source_path

        self.engines: Dict[str, Dict] = {
            'metastore': metastore_engine.to_dict(),
            'scheduler': scheduler_engine.to_dict(),
            'storage': storage_engine.to_dict(),
            'execution': execution_engine.to_dict()
        }


    def sanitize(self, d: dict):
        REDACTED_KEYS = ["access_key", "secret_key", "aws_role_arn"]

        def redact_value(key: str, value: str):
            if key in REDACTED_KEYS:
                return "REDACTED"
            else:
                return value

        return {key: redact_value(key, value) for (key, value) in d.items()}

    def extended_info(self, config_id: str, current: bool = False):
        ei = []

        if not self.metastore.client_name == "":
            ei.append(
                [
                    "",
                    "metastore",
                    self.metastore.client_name,
                    self.sanitize(self.metastore.config),
                ]
            )

        if not self.scheduler.client_name == "":
            ei.append(
                [
                    "",
                    "scheduler",
                    self.scheduler.client_name,
                    self.sanitize(self.scheduler.config),
                ]
            )

        if not self.storage.client_name == "":
            ei.append(
                [
                    "",
                    "storage",
                    self.storage.client_name,
                    self.sanitize(self.storage.config),
                ]
            )

        if not self.execution.client_name == "":
            ei.append(
                [
                    "",
                    "execution",
                    self.execution.client_name,
                    self.sanitize(self.execution.config),
                ]
            )

        def with_id_item(l, current: bool):
            if current == True:
                cid = "*  " + str(config_id)
            else:
                cid = ".  " + str(config_id)
            return [cid if l.index(x) == 0 else x for x in l]

        with_id = [with_id_item(x, current) if ei.index(x) == 0 else x for x in ei]

        return with_id
