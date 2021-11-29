"""
    Dummy conftest.py for pytargetingutilities.

    If you don't know what this is for, just leave it empty.
    Read more about conftest.py under:
    - https://docs.pytest.org/en/stable/fixture.html
    - https://docs.pytest.org/en/stable/writing_plugins.html
"""


import sys
from datetime import datetime, timedelta, timezone

import boto3
import moto
import pytest
import os

sys.path.insert(0, 'src')

os.environ['AWS_DEFAULT_REGION'] = 'us-east-1'


def add_dummy_data(
    bucket_name,
    file_name,
    data,
    last_modified=datetime.utcnow().replace(tzinfo=timezone.utc),
    region_name='us-east-1',
):
    s3 = boto3.client('s3', region_name=region_name)
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=data)
    if last_modified:
        moto.s3.models.s3_backend.buckets[bucket_name].keys[
            file_name
        ].last_modified = last_modified


def get_date(day_minus=0):
    return datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(
        days=day_minus
    )


def create_bucket(bucket_name, region_name='us-east-1'):
    con = boto3.client('s3', region_name=region_name)
    con.create_bucket(Bucket=bucket_name)


def pytest_configure():
    pytest.add_dummy_data = add_dummy_data
    pytest.create_bucket = create_bucket
    pytest.get_date = get_date
