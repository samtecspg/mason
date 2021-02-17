from mason.configurations.config import Config
from mason.resources.base import get_config
from mason.test.support.testing_base import get_env


class TestEngines:

    def test_all(self):
        env = get_env("/test/support/", "/test/support/validations/")
        config = get_config(env, '3') 
        assert(isinstance(config, Config))
        execution_clients = list(map(lambda e: e.client.name(), config.execution_clients))
        scheduler_clients = list(map(lambda e: e.client.name(), config.scheduler_clients))
        storage_clients = list(map(lambda e: e.client.name(), config.storage_clients))
        metastore_clients = list(map(lambda e: e.client.name(), config.metastore_clients))
        assert(execution_clients == ["test2"])
        assert(storage_clients == ["test"])
        assert(scheduler_clients == ["test2"])
        assert(metastore_clients == ["test"])
        assert(config.metastore().client.name() == "test")
        assert(config.storage().client.name() == "test")
        assert(config.scheduler().client.name() == "test2")
        assert(config.execution().client.name() == "test2")
