from typing import Optional, Dict

from mason.configurations import REDACTED_KEYS
from mason.engines.execution.execution_engine import ExecutionEngine
from mason.engines.metastore.metastore_engine import MetastoreEngine
from mason.engines.scheduler.scheduler_engine import SchedulerEngine
from mason.engines.storage.storage_engine import StorageEngine
from mason.util.dict import sanitize

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
            'metastore': self.metastore.to_dict(),
            'scheduler': self.scheduler.to_dict(),
            'storage': self.storage.to_dict(),
            'execution': self.execution.to_dict()
        }


    def extended_info(self, config_id: str, current: bool = False):
        ei = []

        if not self.metastore.client_name == "":
            ei.append(
                [
                    "",
                    "metastore",
                    self.metastore.client_name,
                    sanitize(self.metastore.config, REDACTED_KEYS),
                ]
            )

        if not self.scheduler.client_name == "":
            ei.append(
                [
                    "",
                    "scheduler",
                    self.scheduler.client_name,
                    sanitize(self.scheduler.config, REDACTED_KEYS),
                ]
            )

        if not self.storage.client_name == "":
            ei.append(
                [
                    "",
                    "storage",
                    self.storage.client_name,
                    sanitize(self.storage.config, REDACTED_KEYS),
                ]
            )

        if not self.execution.client_name == "":
            ei.append(
                [
                    "",
                    "execution",
                    self.execution.client_name,
                    sanitize(self.execution.config, REDACTED_KEYS),
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
