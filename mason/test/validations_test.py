from mason.test.support.validations.config import TestConfigProto
from mason.workflows.workflow import Workflow
from mason.configurations.config import Config
from mason.operators.operator import Operator
from mason.definitions import from_root
from mason.util.environment import MasonEnvironment
from mason.test.support import testing_base as base

from mason.validations.validate import validate_operators, validate_workflows, validate_configs
from mason.validations.config import ConfigProto

from typistry.validators.base import sequence

class TestValidations:

    def before(self):
        base.set_log_level("trace")
        
    def test_operators(self):
        env = MasonEnvironment(mason_home=from_root("/test/support/"), validation_path=from_root("/test/support/validations/"))
        operators, invalid = sequence(validate_operators(env), Operator)
        assert(len(operators) == 6)
        assert(len(invalid) == 0)

    def test_workflows(self):
        env = MasonEnvironment(mason_home=from_root("/test/support/"), validation_path=from_root("/test/support/validations/"))
        workflows, invalid = sequence(validate_workflows(env), Workflow)
        assert(len(workflows) == 10)
        assert(len(invalid) == 0)

    def test_configs(self):
        env = MasonEnvironment(mason_home=from_root("/test/support/"), validation_path=from_root("/test/support/validations/"))
        test_configs, invalid = sequence(validate_configs(env, TestConfigProto), Config)
        assert(len(test_configs) == 4)
        assert(len(invalid) == 7)
        clients = test_configs[0].clients
        assert(len(clients) == 2)
        assert(sorted(map(lambda c: c.name(), clients)) == ["test", "test2"])
        assert(len(test_configs[0].invalid_clients) == 0)
        
        # testing with actual clients
        env2 = MasonEnvironment(mason_home=from_root("/test/support/"))
        configs, invalid = sequence(validate_configs(env2, ConfigProto), Config)
        assert(len(configs) == 3)
        assert(len(invalid) == 8)

        config = configs[0]
        clients = config.clients
        assert(config.execution().__class__.__name__ == "SparkExecutionClient")
        assert(config.scheduler().__class__.__name__ == "InvalidClient")
        assert(config.storage().__class__.__name__ == "InvalidClient")
        assert(config.metastore().__class__.__name__ == "InvalidClient")
        assert(sorted(map(lambda c: c.name(), clients)) == ["spark"])
        config = configs[1]
        clients = config.clients
        assert(config.execution().__class__.__name__ == "InvalidClient")
        assert(config.scheduler().__class__.__name__ == "InvalidClient")
        assert(config.storage().__class__.__name__ == "S3StorageClient")
        assert(config.metastore().__class__.__name__ == "InvalidClient")
        assert(sorted(map(lambda c: c.name(), clients)) == ["s3"])
        config = configs[2]
        clients = config.clients
        assert(config.execution().__class__.__name__ == "InvalidClient")
        assert(config.scheduler().__class__.__name__ == "InvalidClient")
        assert(config.storage().__class__.__name__ == "InvalidClient")
        assert(config.metastore().__class__.__name__ == "InvalidClient")
        assert(sorted(map(lambda c: c.name(), clients)) == [])

