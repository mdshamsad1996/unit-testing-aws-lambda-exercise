from botocore.retries import bucket
import pytest
import boto3
import json
import csv
import os
from moto import mock_dynamodb2, mock_s3
import botocore.session
from botocore.stub import Stubber
from refactor_lambda import (
    lambda_handler,
    get_region,
    get_s3_bucket,
    get_table_name,
    get_csv_file_name,
    get_reader_csv_object,
    extract_item_from_row,
    validate_item,
    KeyNotFoundException,
    InputDataNotFoundException,
    WrongTypeObjectPassedAsAParameter,
    write_items_dynamo_db,
    get_s3_data_by_name
)

@pytest.fixture(autouse=True)
def set_env_var(monkeypatch):
    monkeypatch.setenv('REGION', 'eu-west-2')
    monkeypatch.setenv('S3_BUCKET', 'm2-shamsad-customer-bucket')
    monkeypatch.setenv('CUSTOMER_TABLE','m2-shamsad-customer-table')
    monkeypatch.setenv('CSV_FILE_NAME', 'customer_info.csv')


def test_get_region_succeeds(monkeypatch):

    """unit test of get_region() method"""

    region = get_region()
    assert region == 'eu-west-2'


def test_get_region_fails_if_not_set(monkeypatch):

    """unit test of get_region()  method with unset env"""

    monkeypatch.delenv('REGION')
    with pytest.raises(KeyError):
        get_region()


def test_get_s3_bucket_succeeds():

    """unittest of get_s3_bucket() method"""

    bucket_name = get_s3_bucket()
    assert bucket_name == 'm2-shamsad-customer-bucket'


def test_get_s3_bucket_if_not_set(monkeypatch):

    """unittest of get_s3_bucket() method with unset env"""

    monkeypatch.delenv('S3_BUCKET')
    with pytest.raises(KeyError):
        get_s3_bucket()


def test_get_table_name_succeeds():

    """unittest of get_table_name() method"""

    table_name = get_table_name()
    assert table_name == 'm2-shamsad-customer-table'


def test_get_table_name_if_not_set(monkeypatch):

    """unittest of get_table_name() method"""

    monkeypatch.delenv("CUSTOMER_TABLE")
    with pytest.raises(KeyError):
        get_table_name()


def test_get_csv_file_name_succeeds():

    """unittest of get_csv_file_name() method"""

    csv_file_name = get_csv_file_name()
    assert csv_file_name == 'customer_info.csv'


def test_get_csv_file_name_if_not_set(monkeypatch):

    """unittest of get_csv_file_name() method"""

    monkeypatch.delenv("CSV_FILE_NAME")
    with pytest.raises(KeyError):
        get_csv_file_name()


def test_get_reader_csv_object():

    """unit test of get_reader_csv_object() method"""

    csv_object = get_reader_csv_object("")
    assert type(csv_object) == type(csv.reader(""))


valid_input = ['1234567891', 'Huey', 'Duck', 'Master', 'huey.duck@duckmail.com', 'SWIM-777']
valid_response = {'PhoneNumber': {'S': '1234567891'}, 'FirstName': {'S': 'Huey'}, 'LastName': {'S': 'Duck'}, 'Greeting': {'S': 'Master'}, 'Email': {'S': 'huey.duck@duckmail.com'}, 'PostCode': {'S': 'SWIM-777'}}
@pytest.mark.parametrize("input_val,expected",[({},"please pass list type of object"),([1],"list index out of range"),(valid_input, valid_response)])
def test_extract_item_from_row(input_val, expected):

    """unit test of extract_item_from_row() method"""

    actual = extract_item_from_row(input_val)
    assert expected == actual


@pytest.mark.parametrize("input",[{"name": "shams"}])
def test_validate_item_with_missing_key(input):

    """unit test of validate item method with missing key"""

    with pytest.raises(KeyNotFoundException):
        validate_item(input)


@pytest.mark.parametrize("input",[{"PhoneNumber": "+677899","name": "shams"}])
def test_validate_item_with_invalid_dict(input):

    """unit test of validate item method with missing key"""

    with pytest.raises(InputDataNotFoundException):
        validate_item(input)


@pytest.mark.parametrize("input",[""])
def test_write_items_dynamo_db_with_wrong_type_object(input):

    """unit test of write_items_dynamo_db with wrong type of parameter"""

    with pytest.raises(WrongTypeObjectPassedAsAParameter):
        write_items_dynamo_db(input)


def s3_upload_file(filename, bucket, object_name = None):

    """upload file in s3 bucket"""   

    if object_name is None:
        object_name = filename

    s3_client = boto3.client("s3", region_name=get_region())
    response = s3_client.upload_file(filename, bucket, object_name)


@mock_s3
def test_get_s3_data_by_name():

    """mock test of get_s3_data_by_name method using file name"""

    region = get_region()
    s3_client = boto3.client("s3")
    bucket_name = get_s3_bucket()
    s3_client.create_bucket(Bucket = bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
    file_name = get_csv_file_name()
    response = s3_upload_file(file_name, bucket_name)
    expected = s3_client.get_object(Bucket = bucket_name, Key = file_name)['Body'].read().decode('utf-8').splitlines()
    actual = get_s3_data_by_name(file_name)
    assert actual == expected


def create_empty_table():

    """create empty table"""

    region = get_region()
    table = get_table_name()
    ddb_client = boto3.client("dynamodb", region)
    pk = "phoneNumber"
    ddb_client.create_table(
        TableName=table,
        KeySchema=[
            {"AttributeName": pk, "KeyType": "HASH"}
        ],
        AttributeDefinitions=[
            {"AttributeName": pk, "AttributeType": "S"}
        ],
    )
    return ddb_client


# def create_empty_table_with_bad_key():

#     """create empty table with bad key"""

#     region = get_region()
#     table = get_table_name()
#     ddb_client = boto3.client("dynamodb", region)
#     pk = "Number"
#     ddb_client.create_table(
#         TableName=table,
#         KeySchema=[
#             {"AttributeName": pk, "KeyType": "HASH"}
#         ],
#         AttributeDefinitions=[
#             {"AttributeName": pk, "AttributeType": "S"}
#         ],
#     )
#     return ddb_client


@mock_dynamodb2
def test_write_items_dynamo_db_succeeds():

    """write items to dynamo db with table exist"""

    create_empty_table()
    reader_obj = csv.reader("")
    response = write_items_dynamo_db(reader_obj)
    assert response == {'statusCode': 201,'body': '{"status":"Customers created"}'}

