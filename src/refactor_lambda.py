import logging
import boto3
import json
import os
import csv


def get_region(): 

    """ Returns region env var """

    region = os.environ["REGION"]

    log.debug(f" Region: {region}")

    return region


def get_session(region):

    """get session by region"""

    session = boto3.Session(region_name = region)

    return session


def get_dynamodb_client():

    """ Sets up boto3 ddb resource and dynamo db client """
    dynamodb_client = boto3.client('dynamodb')
    return dynamodb_client


def get_s3_resource():

    """ Sets up boto3 ddb resource and dynamo db client """

    s3_resource = boto3.resource('s3')

    return s3_resource


LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)
root_logger = logging.getLogger()
root_logger.setLevel(LOG_LEVEL)
log = logging.getLogger(__name__)


# session = get_session(region_name = os.environ['REGION'])
dynamodb_client = get_dynamodb_client()
s3_resource = get_s3_resource()



class WrongTypeObjectPassedAsAParameter(Exception):
    """Custom exception raised"""
    pass

class InputDataNotFoundException(Exception):
    """ Custom Exception to indicate that the body section was missing from the event """
    pass

class KeyNotFoundException(Exception):
    """ Custom Exception to indicate that the parameter phoneNumber was missing from body """
    pass


def get_s3_bucket():
    
    """return s3 bucketname env var"""

    bucket_name = os.environ["S3_BUCKET"]

    log.debug(f"S3 Bucket -> {bucket_name}")

    return bucket_name


def get_s3_data_by_name(filename):

    """get data by filename"""

    s3_bucket = get_s3_bucket()

    data = s3_resource.Object(s3_bucket,filename).get()['Body'].read().decode('utf-8').splitlines()

    return data


def get_table_name(): 

    """ Returns the ddb table region env var """

    tablename = os.environ["CUSTOMER_TABLE"]

    log.debug(f"Table -> {tablename}")

    return tablename


def get_csv_file_name():

    """returns csv file name"""

    csv_file = os.environ["CSV_FILE_NAME"]

    log.debug(f"csv file name -> {csv_file}")

    return csv_file


def get_reader_csv_object(data):

    """read csv data"""

    csv_reader_object = csv.reader(data)

    return csv_reader_object


def extract_item_from_row(row):

    """extract item from row"""

    try:
        if len(row) > 6:
            """length condition to check"""
            return {"messgae": "please provide proper length of list with proper values"}

        dict_item = {
            "PhoneNumber": {"S": row[0]},
            "FirstName": {"S": row[1]},
            "LastName": {"S": row[2]},
            "Greeting": {"S": row[3]},
            "Email": {"S": row[4]},
            "PostCode": {"S": row[5]}
        }

    except IndexError as error:

        return "list index out of range"

    except KeyError as error:

        return "please pass list type of object"

    return dict_item


def validate_item(item):

    """Validate input item"""

    if "PhoneNumber" not in item:
        raise KeyNotFoundException("PhoneNumber not found in item")
    
    dict_item = {
        'PhoneNumber': "",
        'FirstName': "",
        'LastName': "",
        'Greeting': "",
        'Email': "",
        'PostCode': ""
    }
    
    if set(dict_item.keys()) != set(item.keys()):
        raise InputDataNotFoundException("Item does not have correct keys")




def put_item_to_dynamo_db(tablename, item):

    """put item to dynamo db"""

    log.debug(f"item -> {item} ")

    dynamodb_response = dynamodb_client.put_item(TableName=tablename, Item = item)

 


def write_items_dynamo_db(csv_reader_object):

    """extract item from csv row and itertae for put item in dynamo db"""

    if type(csv_reader_object) != type(csv.reader([])):

        """check type of object"""

        raise WrongTypeObjectPassedAsAParameter("csv reader type of object is not passed")

    try:
        for row in csv_reader_object:

            tablename = get_table_name()

            item = extract_item_from_row(row)

            validate_item(item)

            put_item_to_dynamo_db(tablename, item)

        return {
            'statusCode': 201,
            'body': '{"status":"Customers created"}'
        }

    except Exception as e:

        logging.error(e)

        return {

            'statusCode': 500,
            'body': '{"status":"Server error"}'
        }
        


def lambda_handler(event, context):

    """lambda handler"""

    file_name = get_csv_file_name()

    data = get_s3_data_by_name(file_name)

    csv_reader_object = get_reader_csv_object(data)

    response = write_items_dynamo_db(csv_reader_object)

    return response
