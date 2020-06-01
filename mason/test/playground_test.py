
import os
import pytest
import dotenv
import shutil
from click.testing import CliRunner

from mason.cli import config, operator, register, workflow
from mason.definitions import from_root
from mason.server import MasonServer
from mason.test.support.testing_base import clean_string


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
        pass
















