import datetime
from dateutil.tz import tzlocal


def index(client_type: str, exists=True):
    # TODO: Unify s3 and glue responses here
    if client_type == "glue":
        if exists:
            tables = {'Tables': [
                {'Name': 'catalog_poc_data', 'CreatedAt': datetime.datetime(2020, 2, 26, 12, 57, 31, tzinfo=tzlocal()),
                 'CreatedBy': 'crawler:test_crawler',
                  'Schema': {'SchemaType': 'glue',
                                                           'Columns': [{'Name': 'index', 'Type': 'bigint'},
                                                                       {'Name': 'ts', 'Type': 'string'}]}}]}
            return ({'Data':[tables],'Errors':[],'Info':[],'Warnings':[]},200)
        else:
            return ({'Errors': ['Database bad-database not found'], 'Info': [], 'Warnings': []}, 404)
    elif client_type == "s3":
        if exists:
            return ({'Data': [{'Prefixes': [{'Prefix': '/test_path/'}]}],
              'Errors': [],
              'Info': [],
              'Warnings': []},
             200)
        else:
            return ({'Errors': ['The specified bucket does not exist: bad-database'], 'Info': [],  'Warnings': []}, 404)

def get(client_type: str, switch: int):
    if client_type == "glue":
        if switch == 1:
            return ({'Data':[{'CreatedAt':datetime.datetime(2020,2,26,12,57,31,tzinfo=tzlocal()),'CreatedBy':'crawler:test_crawler','Name':'catalog_poc_data','Schema':{'Columns':[{'Name':'index','Type':'bigint'},{'Name':'break1maxqty','Type':'double'},{'Name':'break1minqty','Type':'double'},{'Name':'break1price','Type':'double'},{'Name':'break2maxqty','Type':'double'},{'Name':'break2minqty','Type':'double'},{'Name':'break2price','Type':'double'},{'Name':'break3maxqty','Type':'double'},{'Name':'break3minqty','Type':'double'},{'Name':'break3price','Type':'double'},{'Name':'break4maxqty','Type':'double'},{'Name':'break4minqty','Type':'double'},{'Name':'break4price','Type':'double'},{'Name':'break5maxqty','Type':'double'},{'Name':'break5minqty','Type':'double'},{'Name':'break5price','Type':'double'},{'Name':'maxprice','Type':'double'},{'Name':'minprice','Type':'double'},{'Name':'packaging','Type':'string'},{'Name':'partnumber','Type':'string'},{'Name':'qtyperpackage','Type':'double'},{'Name':'source','Type':'string'},{'Name':'stockqty','Type':'double'},{'Name':'timestamp','Type':'string'},{'Name':'virtualstockqty','Type':'double'},{'Name':'break6maxqty','Type':'double'},{'Name':'break6minqty','Type':'double'},{'Name':'break6price','Type':'double'},{'Name':'break7maxqty','Type':'double'},{'Name':'break7minqty','Type':'double'},{'Name':'break7price','Type':'double'},{'Name':'ts','Type':'string'}],'SchemaType':'glue'}}],'Errors':[],'Info':[],'Warnings':[]},200)
        elif switch == 2:
            return ({'Warnings': [], 'Errors': ['Database bad-database or table catalog_poc_data not found'], 'Info': []}, 404)
        elif switch == 3:
            return ({'Warnings': [], 'Errors': ['Database crawler-poc or table bad-table not found'], 'Info': []}, 404)
        else:
            return {}
    elif client_type == "s3":
        if switch == 1:
            return ({'Errors': [], 'Info': [], 'Warnings': [], 'Data': [{'Name': 'catalog_poc_data', 'CreatedAt': '', 'CreatedBy': 'mason', 'Schema': {'SchemaType': 'parquet', 'Columns': [ {'Name': 'test_column_1', 'Type': 'INT32', 'ConvertedType': 'REQUIRED', 'RepititionType': None}, {'Name': 'test_column_2', 'Type': 'BYTE_ARRAY', 'ConvertedType': 'UTF8', 'RepititionType': 'OPTIONAL'}]}}]}, 200)
        elif switch == 2:
            return ({'Errors': ['No keys at s3://bad-database/catalog_poc_data'], 'Info': [],'Warnings': []}, 404)
        elif switch == 3:
            return ({'Errors': ['No keys at s3://crawler-poc/bad-table'], 'Info': [], 'Warnings': []}, 404)

def post(exists = True):
    if exists:
        info = ['Registering workflow dag test_crawler with glue.',
         'Registered schedule test_crawler',
         'Triggering schedule: test_crawler',
         'Refreshing Table Crawler: test_crawler']
        return ({'Errors': [], 'Info': info, 'Warnings': ['Table crawler test_crawler already exists. Skipping '  'creation.']},201)
    else:
        info = ['Registering workflow dag test_crawler_new with glue.',
                'Created table crawler test_crawler_new.',
                'Registered schedule test_crawler_new',
                'Triggering schedule: test_crawler_new',
                'Refreshing Table Crawler: test_crawler_new']
        return ({'Warnings': [], 'Errors': [],'Info': info}, 201)

def refresh(refreshing = True):
    if refreshing:
        return ({'Warnings': ['Table crawler test_crawler_refreshing is already refreshing.'], 'Errors': [], 'Info': []}, 202)
    else:
        return ({'Errors': [],
        'Info': ['Refreshing Table Crawler: test_crawler'],
        'Warnings': []},
        201)


