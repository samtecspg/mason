

import pytest
from mason.util.notebook_environment import NotebookEnvironment

import os
import shutil

from dotenv import load_dotenv

from mason.definitions import from_root


def print_result(result):
    print()
    if result.output:
        print(result.output)
    if result.exception:
        print(result.exception)


@pytest.mark.skip(reason="This is not mocked, hits live endpoints")
class TestPlayground:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        os.environ["MASON_HOME"] = ".tmp/"
        yield
        shutil.rmtree(".tmp/")
    
    def test_unmocked(self):
        env = NotebookEnvironment() 
        response = env.run(namespace="table", command="export", parameters="database_name:spg-mason-demo,table_name:merged_csv", config_id="1")
        response.formatted()
        


        














