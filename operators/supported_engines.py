
from typing import Optional, List, Dict
from configurations import Config
from util.logger import logger

def from_array(a: List[Dict[str, str]]):
    return list(map(lambda item: SupportedEngineSet(metastore=item.get("metastore"), scheduler=item.get("scheduler"), execution=item.get("execution"), storage=item.get("storage")), a))

class SupportedEngineSet:
    def __init__(self, metastore: Optional[str], scheduler: Optional[str], execution: Optional[str], storage: Optional[str]):
        self.metastore = metastore
        self.scheduler = scheduler
        self.execution = execution
        self.storage = storage

        self.all = {
            'metastore': self.metastore,
            'scheduler': self.scheduler,
            'execution': self.execution,
            'storage': self.storage
        }

    def validate_coverage(self, config: Config):
        test = True
        for engine_type, supported_engine in self.all.items():
            config_engine_client = config.engines.get(engine_type, {}).get('client_name', "")
            if supported_engine:
                test = (supported_engine == config_engine_client)
            if not test:
                break

        return test

