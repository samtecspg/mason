import datetime
from botocore.exceptions import ClientError
from dateutil.tz import tzlocal

class GlueMock:

    def get_tables(self, DatabaseName: str):
        db_name = DatabaseName
        if db_name == "bad-database":
            parsed_response = {'Error': {'Message': 'Database bad-database not found.', 'Code': 'EntityNotFoundException'}, 'ResponseMetadata': {'RequestId': '870ac0e9-c1ce-41dc-b5bd-86068805c9d7', 'HTTPStatusCode': 400,  'HTTPHeaders': {'date': 'Thu, 27 Feb 2020 19:33:33 GMT',  'content-type': 'application/x-amz-json-1.1', 'content-length': '81',  'connection': 'keep-alive',  'x-amzn-requestid': '870ac0e9-c1ce-41dc-b5bd-86068805c9d7'},  'RetryAttempts': 0}}
            raise ClientError(parsed_response, 'list_tables')
        elif db_name == "good_database":
            return {'ResponseMetadata': {'HTTPStatusCode': 200},'TableList': [{'Name': 'test_table', 'DatabaseName': 'good_database', 'StorageDescriptor': {'Columns': [{"Name": "test", "Type": "test"}]}}]}
        elif db_name == "access_denied":
            return {'ResponseMetadata': {'HTTPStatusCode': 200},'TableList': [{'Name': 'test_table', 'DatabaseName': 'access_denied', 'StorageDescriptor': {'Columns': [{"Name": "test", "Type": "test"}]}}]}
        elif db_name == "test-database":
            return {'TableList': [{'Name': 'test-table', 'DatabaseName': 'test-database', 'Owner': 'owner','CreateTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),'UpdateTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),'LastAccessTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()), 'Retention': 0,'StorageDescriptor': {'Columns': [{'Name': 'index', 'Type': 'bigint'},{'Name': 'ts', 'Type': 'string'}],'Location': 's3://test_bucket/test_path','InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat','OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat','Compressed': False, 'NumberOfBuckets': -1, 'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe','Parameters': {'serialization.format': '1'}}, 'BucketColumns': [], 'SortColumns': [],'Parameters': {'CrawlerSchemaDeserializerVersion': '1.0',   'CrawlerSchemaSerializerVersion': '1.0',   'UPDATED_BY_CRAWLER': 'test_crawler', 'averageRecordSize': '85',   'classification': 'parquet', 'compressionType': 'none', 'objectCount': '30',   'recordCount': '8637979', 'sizeKey': '294119384', 'typeOfData': 'file'},'StoredAsSubDirectories': False}, 'PartitionKeys': [], 'TableType': 'EXTERNAL_TABLE','Parameters': {'CrawlerSchemaDeserializerVersion': '1.0',   'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'test_crawler',   'averageRecordSize': '85', 'classification': 'parquet',   'compressionType': 'none', 'objectCount': '30', 'recordCount': '8637979',   'sizeKey': '294119384', 'typeOfData': 'file'},'CreatedBy': 'arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler','IsRegisteredWithLakeFormation': False}], 'ResponseMetadata': {'RequestId': '918768f3-552d-469f-ae60-18290b769895', 'HTTPStatusCode': 200,  'HTTPHeaders': {'date': 'Thu, 27 Feb 2020 18:05:35 GMT',  'content-type': 'application/x-amz-json-1.1', 'content-length': '2724',  'connection': 'keep-alive',  'x-amzn-requestid': '918768f3-552d-469f-ae60-18290b769895'},  'RetryAttempts': 0}}
        else:
            raise Exception(f"Unmocked glue api call made to list_tables DatabaseName={db_name}")


    def get_table(self, DatabaseName: str, Name: str):
        db_name = DatabaseName
        table_name = Name
        if (db_name == "good_database") and (table_name == "good_table"):
            return {'Table': {'Name': 'good_table', 'DatabaseName': 'good_database', 'Owner': 'owner','CreateTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),'UpdateTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),'LastAccessTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()), 'Retention': 0,'StorageDescriptor': {'Columns': [{'Name': 'index', 'Type': 'bigint'}, {'Name': 'break1maxqty', 'Type': 'double'},{'Name': 'break1minqty', 'Type': 'double'},{'Name': 'break1price', 'Type': 'double'},{'Name': 'break2maxqty', 'Type': 'double'},{'Name': 'break2minqty', 'Type': 'double'},{'Name': 'break2price', 'Type': 'double'},{'Name': 'break3maxqty', 'Type': 'double'},{'Name': 'break3minqty', 'Type': 'double'},{'Name': 'break3price', 'Type': 'double'},{'Name': 'break4maxqty', 'Type': 'double'},{'Name': 'break4minqty', 'Type': 'double'},{'Name': 'break4price', 'Type': 'double'},{'Name': 'break5maxqty', 'Type': 'double'},{'Name': 'break5minqty', 'Type': 'double'},{'Name': 'break5price', 'Type': 'double'},{'Name': 'maxprice', 'Type': 'double'}, {'Name': 'minprice', 'Type': 'double'},{'Name': 'packaging', 'Type': 'string'},{'Name': 'partnumber', 'Type': 'string'},{'Name': 'qtyperpackage', 'Type': 'double'},{'Name': 'source', 'Type': 'string'}, {'Name': 'stockqty', 'Type': 'double'},{'Name': 'timestamp', 'Type': 'string'},{'Name': 'virtualstockqty', 'Type': 'double'},{'Name': 'break6maxqty', 'Type': 'double'},{'Name': 'break6minqty', 'Type': 'double'},{'Name': 'break6price', 'Type': 'double'},{'Name': 'break7maxqty', 'Type': 'double'},{'Name': 'break7minqty', 'Type': 'double'},{'Name': 'break7price', 'Type': 'double'}, {'Name': 'ts', 'Type': 'string'}],'Location': 's3://test_bucket/test_path','InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat','OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat','Compressed': False, 'NumberOfBuckets': -1, 'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe','Parameters': {'serialization.format': '1'}}, 'BucketColumns': [], 'SortColumns': [],'Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'test_crawler', 'averageRecordSize': '85', 'classification': 'parquet', 'compressionType': 'none', 'objectCount': '30', 'recordCount': '8637979', 'sizeKey': '294119384', 'typeOfData': 'file'},'StoredAsSubDirectories': False}, 'PartitionKeys': [], 'TableType': 'EXTERNAL_TABLE','Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'test_crawler', 'averageRecordSize': '85', 'classification': 'parquet', 'compressionType': 'none', 'objectCount': '30', 'recordCount': '8637979', 'sizeKey': '294119384', 'typeOfData': 'file'},'CreatedBy': 'arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler','IsRegisteredWithLakeFormation': False}, 'ResponseMetadata': {'RequestId': '26ac050d-fb79-468a-bc24-01c44d45385a', 'HTTPStatusCode': 200, 'HTTPHeaders': {'date': 'Thu, 27 Feb 2020 20:05:46 GMT','content-type': 'application/x-amz-json-1.1','content-length': '2718', 'connection': 'keep-alive','x-amzn-requestid': '26ac050d-fb79-468a-bc24-01c44d45385a'}, 'RetryAttempts': 0}}
        if (db_name == "access_denied") and (table_name == "good_table"):
            return {'Table': {'Name': 'good_table', 'DatabaseName': 'good_database', 'Owner': 'owner','CreateTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),'UpdateTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),'LastAccessTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()), 'Retention': 0,'StorageDescriptor': {'Columns': [{'Name': 'index', 'Type': 'bigint'}, {'Name': 'break1maxqty', 'Type': 'double'},{'Name': 'break1minqty', 'Type': 'double'},{'Name': 'break1price', 'Type': 'double'},{'Name': 'break2maxqty', 'Type': 'double'},{'Name': 'break2minqty', 'Type': 'double'},{'Name': 'break2price', 'Type': 'double'},{'Name': 'break3maxqty', 'Type': 'double'},{'Name': 'break3minqty', 'Type': 'double'},{'Name': 'break3price', 'Type': 'double'},{'Name': 'break4maxqty', 'Type': 'double'},{'Name': 'break4minqty', 'Type': 'double'},{'Name': 'break4price', 'Type': 'double'},{'Name': 'break5maxqty', 'Type': 'double'},{'Name': 'break5minqty', 'Type': 'double'},{'Name': 'break5price', 'Type': 'double'},{'Name': 'maxprice', 'Type': 'double'}, {'Name': 'minprice', 'Type': 'double'},{'Name': 'packaging', 'Type': 'string'},{'Name': 'partnumber', 'Type': 'string'},{'Name': 'qtyperpackage', 'Type': 'double'},{'Name': 'source', 'Type': 'string'}, {'Name': 'stockqty', 'Type': 'double'},{'Name': 'timestamp', 'Type': 'string'},{'Name': 'virtualstockqty', 'Type': 'double'},{'Name': 'break6maxqty', 'Type': 'double'},{'Name': 'break6minqty', 'Type': 'double'},{'Name': 'break6price', 'Type': 'double'},{'Name': 'break7maxqty', 'Type': 'double'},{'Name': 'break7minqty', 'Type': 'double'},{'Name': 'break7price', 'Type': 'double'}, {'Name': 'ts', 'Type': 'string'}],'Location': 's3://test_bucket/test_path','InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat','OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat','Compressed': False, 'NumberOfBuckets': -1, 'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe','Parameters': {'serialization.format': '1'}}, 'BucketColumns': [], 'SortColumns': [],'Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'test_crawler', 'averageRecordSize': '85', 'classification': 'parquet', 'compressionType': 'none', 'objectCount': '30', 'recordCount': '8637979', 'sizeKey': '294119384', 'typeOfData': 'file'},'StoredAsSubDirectories': False}, 'PartitionKeys': [], 'TableType': 'EXTERNAL_TABLE','Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'test_crawler', 'averageRecordSize': '85', 'classification': 'parquet', 'compressionType': 'none', 'objectCount': '30', 'recordCount': '8637979', 'sizeKey': '294119384', 'typeOfData': 'file'},'CreatedBy': 'arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler','IsRegisteredWithLakeFormation': False}, 'ResponseMetadata': {'RequestId': '26ac050d-fb79-468a-bc24-01c44d45385a', 'HTTPStatusCode': 200, 'HTTPHeaders': {'date': 'Thu, 27 Feb 2020 20:05:46 GMT','content-type': 'application/x-amz-json-1.1','content-length': '2718', 'connection': 'keep-alive','x-amzn-requestid': '26ac050d-fb79-468a-bc24-01c44d45385a'}, 'RetryAttempts': 0}}
        elif (db_name == "test-database") and (table_name == "test-table"):
            return {'Table': {'Name': 'test-table', 'DatabaseName': 'test-database', 'Owner': 'owner','CreateTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),'UpdateTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),'LastAccessTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()), 'Retention': 0,'StorageDescriptor': {'Columns': [{'Name': 'index', 'Type': 'bigint'}, {'Name': 'break1maxqty', 'Type': 'double'},{'Name': 'break1minqty', 'Type': 'double'},{'Name': 'break1price', 'Type': 'double'},{'Name': 'break2maxqty', 'Type': 'double'},{'Name': 'break2minqty', 'Type': 'double'},{'Name': 'break2price', 'Type': 'double'},{'Name': 'break3maxqty', 'Type': 'double'},{'Name': 'break3minqty', 'Type': 'double'},{'Name': 'break3price', 'Type': 'double'},{'Name': 'break4maxqty', 'Type': 'double'},{'Name': 'break4minqty', 'Type': 'double'},{'Name': 'break4price', 'Type': 'double'},{'Name': 'break5maxqty', 'Type': 'double'},{'Name': 'break5minqty', 'Type': 'double'},{'Name': 'break5price', 'Type': 'double'},{'Name': 'maxprice', 'Type': 'double'}, {'Name': 'minprice', 'Type': 'double'},{'Name': 'packaging', 'Type': 'string'},{'Name': 'partnumber', 'Type': 'string'},{'Name': 'qtyperpackage', 'Type': 'double'},{'Name': 'source', 'Type': 'string'}, {'Name': 'stockqty', 'Type': 'double'},{'Name': 'timestamp', 'Type': 'string'},{'Name': 'virtualstockqty', 'Type': 'double'},{'Name': 'break6maxqty', 'Type': 'double'},{'Name': 'break6minqty', 'Type': 'double'},{'Name': 'break6price', 'Type': 'double'},{'Name': 'break7maxqty', 'Type': 'double'},{'Name': 'break7minqty', 'Type': 'double'},{'Name': 'break7price', 'Type': 'double'}, {'Name': 'ts', 'Type': 'string'}],'Location': 's3://test_bucket/test_path','InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat','OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat','Compressed': False, 'NumberOfBuckets': -1, 'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe','Parameters': {'serialization.format': '1'}}, 'BucketColumns': [], 'SortColumns': [],'Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'test_crawler', 'averageRecordSize': '85', 'classification': 'parquet', 'compressionType': 'none', 'objectCount': '30', 'recordCount': '8637979', 'sizeKey': '294119384', 'typeOfData': 'file'},'StoredAsSubDirectories': False}, 'PartitionKeys': [], 'TableType': 'EXTERNAL_TABLE','Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'test_crawler', 'averageRecordSize': '85', 'classification': 'parquet', 'compressionType': 'none', 'objectCount': '30', 'recordCount': '8637979', 'sizeKey': '294119384', 'typeOfData': 'file'},'CreatedBy': 'arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler','IsRegisteredWithLakeFormation': False}, 'ResponseMetadata': {'RequestId': '26ac050d-fb79-468a-bc24-01c44d45385a', 'HTTPStatusCode': 200, 'HTTPHeaders': {'date': 'Thu, 27 Feb 2020 20:05:46 GMT','content-type': 'application/x-amz-json-1.1','content-length': '2718', 'connection': 'keep-alive','x-amzn-requestid': '26ac050d-fb79-468a-bc24-01c44d45385a'}, 'RetryAttempts': 0}}
        elif (db_name == "test-database") and (table_name == "test-table_refreshing"):
            return {'Table': {'Name': 'test-table', 'DatabaseName': 'test-database', 'Owner': 'owner','CreateTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),'UpdateTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),'LastAccessTime': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()), 'Retention': 0,'StorageDescriptor': {'Columns': [{'Name': 'index', 'Type': 'bigint'}, {'Name': 'break1maxqty', 'Type': 'double'},{'Name': 'break1minqty', 'Type': 'double'},{'Name': 'break1price', 'Type': 'double'},{'Name': 'break2maxqty', 'Type': 'double'},{'Name': 'break2minqty', 'Type': 'double'},{'Name': 'break2price', 'Type': 'double'},{'Name': 'break3maxqty', 'Type': 'double'},{'Name': 'break3minqty', 'Type': 'double'},{'Name': 'break3price', 'Type': 'double'},{'Name': 'break4maxqty', 'Type': 'double'},{'Name': 'break4minqty', 'Type': 'double'},{'Name': 'break4price', 'Type': 'double'},{'Name': 'break5maxqty', 'Type': 'double'},{'Name': 'break5minqty', 'Type': 'double'},{'Name': 'break5price', 'Type': 'double'},{'Name': 'maxprice', 'Type': 'double'}, {'Name': 'minprice', 'Type': 'double'},{'Name': 'packaging', 'Type': 'string'},{'Name': 'partnumber', 'Type': 'string'},{'Name': 'qtyperpackage', 'Type': 'double'},{'Name': 'source', 'Type': 'string'}, {'Name': 'stockqty', 'Type': 'double'},{'Name': 'timestamp', 'Type': 'string'},{'Name': 'virtualstockqty', 'Type': 'double'},{'Name': 'break6maxqty', 'Type': 'double'},{'Name': 'break6minqty', 'Type': 'double'},{'Name': 'break6price', 'Type': 'double'},{'Name': 'break7maxqty', 'Type': 'double'},{'Name': 'break7minqty', 'Type': 'double'},{'Name': 'break7price', 'Type': 'double'}, {'Name': 'ts', 'Type': 'string'}],'Location': 's3://test_bucket/test_path','InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat','OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat','Compressed': False, 'NumberOfBuckets': -1, 'SerdeInfo': {'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe','Parameters': {'serialization.format': '1'}}, 'BucketColumns': [], 'SortColumns': [],'Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'test_crawler_refreshing', 'averageRecordSize': '85', 'classification': 'parquet', 'compressionType': 'none', 'objectCount': '30', 'recordCount': '8637979', 'sizeKey': '294119384', 'typeOfData': 'file'},'StoredAsSubDirectories': False}, 'PartitionKeys': [], 'TableType': 'EXTERNAL_TABLE','Parameters': {'CrawlerSchemaDeserializerVersion': '1.0', 'CrawlerSchemaSerializerVersion': '1.0', 'UPDATED_BY_CRAWLER': 'test_crawler_refreshing', 'averageRecordSize': '85', 'classification': 'parquet', 'compressionType': 'none', 'objectCount': '30', 'recordCount': '8637979', 'sizeKey': '294119384', 'typeOfData': 'file'},'CreatedBy': 'arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler','IsRegisteredWithLakeFormation': False}, 'ResponseMetadata': {'RequestId': '26ac050d-fb79-468a-bc24-01c44d45385a', 'HTTPStatusCode': 200, 'HTTPHeaders': {'date': 'Thu, 27 Feb 2020 20:05:46 GMT','content-type': 'application/x-amz-json-1.1','content-length': '2718', 'connection': 'keep-alive','x-amzn-requestid': '26ac050d-fb79-468a-bc24-01c44d45385a'}, 'RetryAttempts': 0}}
        elif db_name == "bad-database":
            return {'Error': {'Message': 'Database bad-database not found.', 'Code': 'EntityNotFoundException'}, 'ResponseMetadata': {'RequestId': 'f4279e1d-945d-4cf3-a5ed-9932c94b4453', 'HTTPStatusCode': 400,    'HTTPHeaders': {'date': 'Thu, 27 Feb 2020 20:14:14 GMT',  'content-type': 'application/x-amz-json-1.1', 'content-length': '81',  'connection': 'keep-alive',  'x-amzn-requestid': 'f4279e1d-945d-4cf3-a5ed-9932c94b4453'},    'RetryAttempts': 0}}
        elif (table_name == "bad-table"):
            return {'Error': {'Message': 'Table bad_table not found.', 'Code': 'EntityNotFoundException'}, 'ResponseMetadata': {'RequestId': 'cb62bb64-c3cc-44c8-a88f-c3590d621f12', 'HTTPStatusCode': 400, 'HTTPHeaders': {'date': 'Thu, 27 Feb 2020 20:14:14 GMT',  'content-type': 'application/x-amz-json-1.1', 'content-length': '75',  'connection': 'keep-alive',  'x-amzn-requestid': 'cb62bb64-c3cc-44c8-a88f-c3590d621f12'}, 'RetryAttempts': 0}}
        else:
            raise Exception(f"Unmocked glue api call made to get_table DatabaseName={db_name} Name={table_name}")

    def start_crawler(self, Name: str):
        if (Name == 'test_crawler') | (Name == 'test_crawler_new') | (Name[0:11] == "table_infer"):
            return {'ResponseMetadata': {'RequestId': '6d9a47be-a8de-4b0c-9dd5-6fee482bbf82', 'HTTPStatusCode': 200,    'HTTPHeaders': {'date': 'Fri, 28 Feb 2020 17:01:03 GMT','content-type': 'application/x-amz-json-1.1', 'content-length': '2','connection': 'keep-alive','x-amzn-requestid': '6d9a47be-a8de-4b0c-9dd5-6fee482bbf82'},    'RetryAttempts': 0}}
        elif (Name == 'test_crawler_refreshing'):
            return {'Error': {'Message': 'Crawler with name test_crawler has already started',   'Code': 'CrawlerRunningException'},   'ResponseMetadata': {'RequestId': '540d8901-1f0e-405d-9fd0-5b79fde6595b', 'HTTPStatusCode': 400,    'HTTPHeaders': {'date': 'Fri, 28 Feb 2020 17:03:24 GMT','content-type': 'application/x-amz-json-1.1', 'content-length': '99','connection': 'keep-alive','x-amzn-requestid': '540d8901-1f0e-405d-9fd0-5b79fde6595b'},    'RetryAttempts': 0}}
        else:
            raise Exception(f"Unmocked glue api call made to start_crawler Name={Name}")

    def create_crawler(self, DatabaseName: str, Name: str, Role: str, Targets: dict):
        targets = {'S3Targets': [{'Path': 's3://test_bucket/test_path','Exclusions': []}]}
        if DatabaseName == 'test-database' and (Name == 'test_crawler' or Name[0:11] == "table_infer") and Targets == targets:
            return {'Error': {'Message': '062325279035:test_crawler already exists', 'Code': 'AlreadyExistsException'}, 'ResponseMetadata': {'RequestId': 'f9bc413f-bf72-4bba-98f0-116bf363b1a6', 'HTTPStatusCode': 400,  'HTTPHeaders': {'date': 'Thu, 27 Feb 2020 20:42:35 GMT',  'content-type': 'application/x-amz-json-1.1',  'content-length': '88', 'connection': 'keep-alive',  'x-amzn-requestid': 'f9bc413f-bf72-4bba-98f0-116bf363b1a6'},  'RetryAttempts': 0}}
        elif DatabaseName == 'test-database' and (Name == 'test_crawler_new' or Name[0:11]) and Targets == targets:
            return {'ResponseMetadata': {'RequestId': '40f04642-e3a9-431e-96f0-e24bb9291706', 'HTTPStatusCode': 200,    'HTTPHeaders': {'date': 'Thu, 27 Feb 2020 20:52:39 GMT','content-type': 'application/x-amz-json-1.1', 'content-length': '2','connection': 'keep-alive','x-amzn-requestid': '40f04642-e3a9-431e-96f0-e24bb9291706'},    'RetryAttempts': 0}}
        else:
            raise Exception(f"Unmocked glue api call made Targets={Targets}")

    def delete_table(self, *args, **kwargs):
        if kwargs["DatabaseName"] == "good_database" and kwargs["Name"] == "good_table":
            return {'ResponseMetadata': {'RequestId': 'test_id', 'HTTPStatusCode': 200}}
        elif kwargs["DatabaseName"] == "bad_database":
            return {'Error': {'Message': 'Database bad_database not found.', 'Code': 'EntityNotFoundException'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}
        elif kwargs["DatabaseName"] == "good_database" and kwargs["Name"] == "bad_table":
            return {'Error': {'Message': 'Table bad_table not found.', 'Code': 'EntityNotFoundException'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}
        else:
            raise Exception(f"Unmocked glue api call")

    def delete_crawler(self, *args, **kwargs):
        if kwargs["Name"] == "good_schedule":
            return {'ResponseMetadata': {'RequestId': 'test_id', 'HTTPStatusCode': 200}}
        elif kwargs["Name"] == "bad_schedule":
            return {'Error': {'Message': 'Crawler entry with name bad_schedule does not exist', 'Code': 'EntityNotFoundException'}, 'ResponseMetadata': {'HTTPStatusCode': 400}}
        else:
            raise Exception(f"Unmocked glue api call")








