
from util.logger import logger

class SchemaElement:
    def __init__(self, attributes: dict):
        self.attributes = attributes

class MetastoreSchema:
    def __init__(self, schema):
        self.schema = schema

    def print(self):
        logger.info(self.schema)
