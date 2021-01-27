from typing import Optional

# from mason.configurations import REDACTED_KEYS
# from mason.util.dict import sanitize
from mason.engines.execution.execution_engine import ExecutionEngine
from mason.engines.metastore.metastore_engine import MetastoreEngine
from mason.engines.scheduler.scheduler_engine import SchedulerEngine
from mason.engines.storage.storage_engine import StorageEngine

class ValidConfig:

    def __init__(self, id: str, config: dict, metastore_engine: MetastoreEngine, scheduler_engine: SchedulerEngine, storage_engine: StorageEngine, execution_engine: ExecutionEngine, source_path: Optional[str] = None):
        pass
    #     self.id = id
    #     self.config = config
    #     self.metastore = metastore_engine
    #     self.scheduler = scheduler_engine
    #     self.storage = storage_engine
    #     self.execution = execution_engine
    #     self.source_path = source_path
    # 
    #     self.engines: Dict[str, Dict] = {
    #         'metastore': self.metastore.to_dict(),
    #         'scheduler': self.scheduler.to_dict(),
    #         'storage': self.storage.to_dict(),
    #         'execution': self.execution.to_dict()
    #     }
    # 
    # 
