from mason.test.support import testing_base as base
from mason.util.environment import MasonEnvironment
from mason.definitions import from_root
from mason.util.yaml import parse_yaml
from mason.configurations.config import Config

def empty_config():
    return {
        'execution': {'client_name': '', 'configuration': {}},
        'metastore': {'client_name': '', 'configuration': {}},
        'scheduler': {'client_name': '', 'configuration': {}},
        'storage': {'client_name': '', 'configuration': {}}
    }

class TestConfiguration:

    def before(self, config: str):
        base.set_log_level("trace")
        config_home = from_root(config)
        env = MasonEnvironment(config_home=config_home)
        config_doc = parse_yaml(env.config_home)
        conf = Config(config_doc).validate(env)
        return conf, env


    def test_configuration_path_dne(self):
        conf, env = self.before("/path_dne")
        assert(conf.engines == empty_config())

    def test_configuration_invalid_yaml(self):
        conf, env = self.before("/test/support/invalid_yaml.yaml")
        assert(conf.engines == empty_config())

    def test_configuration_invalid_yaml_2(self):
        conf, env = self.before("/test/support/invalid_yaml_2.yaml")
        assert(conf.engines == empty_config())

    def test_configuration_invalid_config(self):
        conf, env = self.before("/test/support/test_bad_config.yaml")
        assert(conf.engines == empty_config())

    def test_configuration_valid(self):
        conf, env = self.before("/test/support/configs/valid_config_1.yaml")
        expects = {'execution': {'client_name': '', 'configuration': {}},
             'metastore': {'client_name': '', 'configuration': {}},
             'scheduler': {'client_name': '', 'configuration': {}},
             'storage': {'client_name': 's3', 'configuration': {'aws_region': 'us-west-2', "secret_key": "test", "access_key": "test"}}
       }
        assert(conf.engines == expects)
        extended_info = [['*  0', 'storage', 's3', {'aws_region': 'us-west-2', 'secret_key': 'REDACTED', 'access_key': 'REDACTED'}]]
        assert(conf.extended_info(0, True) == extended_info)


    def test_valid_spark_config(self):
        conf, env = self.before("/test/support/configs/valid_spark_config.yaml")
        expects = {'execution': {'client_name': 'spark',
                       'configuration': {'runner': {'spark_version': '2.4.5',
                                                    'type': 'kubernetes-operator'}}},
         'metastore': {'client_name': '', 'configuration': {}},
         'scheduler': {'client_name': '', 'configuration': {}},
         'storage': {'client_name': '', 'configuration': {}}}
        assert(conf.engines == expects)

    def test_invalid_spark_config(self):
        conf, env = self.before("/test/support/configs/invalid_spark_config.yaml")

        assert(conf.__class__.__name__ == "InvalidConfig")
        assert("'runner' is a required property" in conf.reason)


    def test_invalid_spark_config_2(self):
        conf, env = self.before("/test/support/configs/invalid_spark_config_2.yaml")
        assert(conf.__class__.__name__ == "InvalidConfig")
        assert("Additional properties are not allowed ('bad_attribute' was unexpected)." in conf.reason)




