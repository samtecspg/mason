from test.mocks.clients import glue as GlueMock

class Mock:

    def glue(self, operation_name, kwarg):
        if operation_name == "GetTables":
            return GlueMock.list_tables(kwarg)
        elif operation_name == "GetTable":
            return GlueMock.get_table(kwarg)
        elif operation_name == "StartCrawler":
            return GlueMock.start_crawler(kwarg)
        elif operation_name == "CreateCrawler":
            return GlueMock.create_crawler(kwarg)
        else:
            raise Exception(f"Unmocked API endpoint: {operation_name}")
