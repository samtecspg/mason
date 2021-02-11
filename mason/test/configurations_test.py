from mason.configurations.config import Config
from mason.resources.base import get_config_by_id
from mason.test.support import testing_base as base


def empty_config():
    return {
        'execution': {'client_name': '', 'configuration': {}},
        'metastore': {'client_name': '', 'configuration': {}},
        'scheduler': {'client_name': '', 'configuration': {}},
        'storage': {'client_name': '', 'configuration': {}}
    }

class TestConfiguration:
        
    def test_get_config_by_id(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        config = get_config_by_id(env, "3")
        assert(isinstance(config, Config))
        assert(config.id == "3")
        assert(config.execution().client.name() == "test2")
        assert(config.metastore().client.name() == "test")
        assert(config.scheduler().client.name() == "test2")
        assert(config.storage().client.name() == "test")

    def test_configuration_invalid_yaml(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        assert(isinstance(conf, InvalidConfig))
        assert("Invalid config schema. Reason: Schema error " in conf.reason)
