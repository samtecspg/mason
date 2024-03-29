import shutil

import pytest

from mason.api.get import get
from mason.api.apply import apply
from mason.definitions import from_root
from mason.test.support import testing_base as base
from os import path, mkdir

class TestGetConfiguration:
    
    def test_config_exists(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        response, status = get("config", '5', env=env) 
        expects = [{'current': False,
          'execution_client': [],
          'id': '5',
          'metastore_client': [{'name': 'test'}],
          'scheduler_client': [],
          'storage_client': []}]
        assert(response['Configs'] == expects)
        assert(status == 200)

    def test_config_malformed(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        base.set_log_level()
        response, status = get("config", "0", log_level="fatal", env=env)
        assert(response['Errors'][0][0:18] == "Malformed resource")
        assert(status == 400)

    def test_config_dne(self):
        env = base.get_env("/test/support/", "/test/support/validations/")
        base.set_log_level()
        response, status = get("config", 'monkeys', log_level="fatal", env=env)
        expects = {'Errors': ['No config matching monkeys. Register new resources with \'mason apply\'']}
        assert(response == expects)
        assert(status == 404)

class TestApplyConfig:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        tmp_folder = from_root("/.tmp/")
        if not path.exists(tmp_folder):
            mkdir(tmp_folder)
        yield
        if path.exists(tmp_folder):
            shutil.rmtree(tmp_folder)

    def test_good_configs(self):
        env = base.get_env("/.tmp/", "/test/support/validations/")
        response, status = apply(from_root("/test/support/"), env=env, log_level="fatal")
        assert(len(response["Info"]) == 20)
        assert(len(response["Errors"]) == 8) 
        assert(status == 200)

        response, status = get("config", env=env, log_level="fatal")
        assert(len(response["Configs"]) == 4)

    def test_overwrite(self):
        pass

