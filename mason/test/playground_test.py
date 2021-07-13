from os import path
import os
import pytest  # type: ignore
from click.testing import CliRunner
from dotenv import load_dotenv

from mason.cli import config, get
from mason.cli.run import run
from mason.definitions import from_root


def print_result(result):
    print(result.output)
    if result.exception:
        print(result.exception)

# @pytest.mark.skip(reason="This is not mocked, hits live endpoints")
class TestCLI:

    @pytest.fixture(autouse=True)
    def run_around_tests(self):
        examples = from_root("/examples/")
        os.environ["MASON_HOME"] = examples
        yield
        current_config = examples + "configs/CURRENT_CONFIG"
        if path.exists(current_config):
            os.remove(current_config)

    def test_play(self):
        runner = CliRunner()
        load_dotenv(from_root("/../.env"), override=True)
        print_result(runner.invoke(run, ["operator", "table", "query", "-p", "table_path:/Users/kyle/dev/mason/mason/test/sample_data/csv_sample.csv,query_string:'SELECT * FROM csv_sample'"], catch_exceptions=False))

