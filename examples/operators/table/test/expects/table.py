import datetime
from dateutil.tz import tzlocal


def index(exists = True):
    if exists:
        return ({'Warnings': [], 'Errors': [], 'Info': [], 'Data': [ {'name': 'catalog_poc_data', 'created_at': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),  'created_by': 'arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler',  'database_name': 'crawler-poc',  'schema': [{'Name': 'index', 'Type': 'bigint'}, {'Name': 'break1maxqty', 'Type': 'double'}, {'Name': 'break1minqty', 'Type': 'double'}, {'Name': 'break1price', 'Type': 'double'}, {'Name': 'break2maxqty', 'Type': 'double'}, {'Name': 'break2minqty', 'Type': 'double'}, {'Name': 'break2price', 'Type': 'double'}, {'Name': 'break3maxqty', 'Type': 'double'}, {'Name': 'break3minqty', 'Type': 'double'}, {'Name': 'break3price', 'Type': 'double'}, {'Name': 'break4maxqty', 'Type': 'double'}, {'Name': 'break4minqty', 'Type': 'double'}, {'Name': 'break4price', 'Type': 'double'}, {'Name': 'break5maxqty', 'Type': 'double'}, {'Name': 'break5minqty', 'Type': 'double'}, {'Name': 'break5price', 'Type': 'double'}, {'Name': 'maxprice', 'Type': 'double'}, {'Name': 'minprice', 'Type': 'double'}, {'Name': 'packaging', 'Type': 'string'}, {'Name': 'partnumber', 'Type': 'string'}, {'Name': 'qtyperpackage', 'Type': 'double'}, {'Name': 'source', 'Type': 'string'}, {'Name': 'stockqty', 'Type': 'double'}, {'Name': 'timestamp', 'Type': 'string'}, {'Name': 'virtualstockqty', 'Type': 'double'}, {'Name': 'break6maxqty', 'Type': 'double'}, {'Name': 'break6minqty', 'Type': 'double'}, {'Name': 'break6price', 'Type': 'double'}, {'Name': 'break7maxqty', 'Type': 'double'}, {'Name': 'break7minqty', 'Type': 'double'}, {'Name': 'break7price', 'Type': 'double'}, {'Name': 'ts', 'Type': 'string'}]}]}, 200)
    else:
        return ({'Warnings': [], 'Errors': ['Database bad-database not found'], 'Info': [], 'Data': {}}, 404)

def get(switch: int):
    if switch == 1:
        return ({'Warnings': [], 'Errors': [], 'Info': [], 'Data': {'name': 'catalog_poc_data', 'created_at': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()), 'created_by': 'arn:aws:sts::062325279035:assumed-role/AWSGlueServiceRole-anduin-data-glue/AWS-Crawler', 'database_name': 'crawler-poc', 'schema': [{'Name': 'index', 'Type': 'bigint'},{'Name': 'break1maxqty', 'Type': 'double'},{'Name': 'break1minqty', 'Type': 'double'},{'Name': 'break1price', 'Type': 'double'},{'Name': 'break2maxqty', 'Type': 'double'},{'Name': 'break2minqty', 'Type': 'double'},{'Name': 'break2price', 'Type': 'double'},{'Name': 'break3maxqty', 'Type': 'double'},{'Name': 'break3minqty', 'Type': 'double'},{'Name': 'break3price', 'Type': 'double'},{'Name': 'break4maxqty', 'Type': 'double'},{'Name': 'break4minqty', 'Type': 'double'},{'Name': 'break4price', 'Type': 'double'},{'Name': 'break5maxqty', 'Type': 'double'},{'Name': 'break5minqty', 'Type': 'double'},{'Name': 'break5price', 'Type': 'double'},{'Name': 'maxprice', 'Type': 'double'},{'Name': 'minprice', 'Type': 'double'},{'Name': 'packaging', 'Type': 'string'},{'Name': 'partnumber', 'Type': 'string'},{'Name': 'qtyperpackage', 'Type': 'double'},{'Name': 'source', 'Type': 'string'},{'Name': 'stockqty', 'Type': 'double'},{'Name': 'timestamp', 'Type': 'string'},{'Name': 'virtualstockqty', 'Type': 'double'},{'Name': 'break6maxqty', 'Type': 'double'},{'Name': 'break6minqty', 'Type': 'double'},{'Name': 'break6price', 'Type': 'double'},{'Name': 'break7maxqty', 'Type': 'double'},{'Name': 'break7minqty', 'Type': 'double'},{'Name': 'break7price', 'Type': 'double'},{'Name': 'ts', 'Type': 'string'}]}}, 200)
    elif switch == 2:
        return ({'Warnings': [], 'Errors': ['Database bad-database or table catalog_poc_data not found'], 'Info': [], 'Data': {}}, 404)
    elif switch == 3:
        return ({'Warnings': [], 'Errors': ['Database crawler-poc or table bad-table not found'], 'Info': [], 'Data': {}}, 404)
    else:
        return {}


def post(exists = True):
    if exists:
        return ({'Errors': [], 'Info': ['Refreshing Table Crawler: test_crawler'], 'Warnings': ['Table crawler test_crawler already exists. Skipping '  'creation.']},201)
    else:
        return ({'Warnings': [], 'Errors': [],'Info': ['Created table crawler test_crawler_new.', 'Refreshing Table Crawler: test_crawler_new']}, 201)

def refresh(refreshing = True):
    if refreshing:
        return ({'Warnings': ['Table crawler test_crawler_refreshing is already refreshing.'], 'Errors': [], 'Info': []}, 202)
    else:
        return ({'Warnings': [], 'Errors': [], 'Info': ['Refreshing Table Crawler: test_crawler']}, 201)

