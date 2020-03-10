from configurations import Config
import unittest

class MasonTest(unittest.TestCase):
    def get_test_config(self):
        return Config({
            'metastore_engine': 'glue'
        })
