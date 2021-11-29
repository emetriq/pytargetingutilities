import unittest
from datetime import datetime, timedelta, timezone

import boto3
from moto import mock_s3
from pytargetingutilities.aws.s3.iterator import S3Iterator
import pytest


class TestS3Iterator(unittest.TestCase):
    @staticmethod
    def add_s3_data(item_length, bucket_name, region_name='us-east-1'):
        con = boto3.client('s3', region_name=region_name)
        con.create_bucket(Bucket=bucket_name)
        for idx in range(0, item_length):
            my_date = datetime.utcnow().replace(
                tzinfo=timezone.utc
            ) - timedelta(days=idx)
            pytest.add_dummy_data(
                bucket_name,
                f'crtt-profiling-{my_date.timestamp()}.json',
                f'testdata {idx}',
                my_date,
            )

    @mock_s3
    def test_aggregate_with_page_size_limit(self):
        item_length = 15
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        result = S3Iterator.paginator(bucket_name, max_items=1).aggregate()
        self.assertEqual(len(result), item_length)

    @mock_s3
    def test_aggregate_with_max_age(self):
        item_length = 15
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        my_iter = S3Iterator.date_paginator(bucket_name, 2).aggregate()
        result = [item for item in my_iter]
        self.assertTrue(len(result) >= 2)

    @mock_s3
    def test_paginator_with_file_ends_with(self):
        bucket_name = 'test'
        con = boto3.client('s3', region_name='us-east-1')
        con.create_bucket(Bucket=bucket_name)
        pytest.add_dummy_data(
            bucket_name,
            'file1.json',
            'test1',
            datetime.utcnow(),
        )
        pytest.add_dummy_data(
            bucket_name,
            'file2.json',
            'test2',
            datetime.utcnow(),
        )
        pytest.add_dummy_data(
            bucket_name,
            'file3.exe',
            'test3',
            datetime.utcnow(),
        )

        my_iter = S3Iterator.paginator(
            bucket_name, 2, ends_with='.json'
        ).aggregate()
        result = [item for item in my_iter]
        self.assertEqual(len(result), 2)

        my_iter = S3Iterator.paginator(
            bucket_name, 2, ends_with='.exe'
        ).aggregate()
        result = [item for item in my_iter]
        self.assertEqual(len(result), 1)

        my_iter = S3Iterator.date_paginator(
            bucket_name, 2, ends_with='.json'
        ).aggregate()
        result = [item for item in my_iter]
        self.assertEqual(len(result), 2)

        my_iter = S3Iterator.date_paginator(
            bucket_name, 2, ends_with='.exe'
        ).aggregate()
        result = [item for item in my_iter]
        self.assertEqual(len(result), 1)


if __name__ == '__main__':
    unittest.main()
