import unittest
from datetime import datetime, timedelta, timezone
from typing import List

from moto import mock_s3
import boto3
import pytest

from pytargetingutilities.aws.s3.line_iterator import S3LineIterator


class TestS3LineIterator(unittest.TestCase):
    @staticmethod
    def add_s3_data(item_lengths: List[int], bucket_name: str, region_name='us-east-1'):
        con = boto3.client('s3', region_name=region_name)
        con.create_bucket(Bucket=bucket_name)
        for idx, length in enumerate(item_lengths):
            my_date = datetime.utcnow().replace(
                tzinfo=timezone.utc
            ) - timedelta(days=len(item_lengths)-idx)
            data = '\n'.join([f'testdata {idx}, {l}' for l in range(length)])
            pytest.add_dummy_data(
                bucket_name,
                f'crtt-profiling-{my_date.timestamp()}.json',
                data,
                my_date,
            )

    @mock_s3
    def test_aggregate_one_file(self):
        item_length = [15]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        result = S3LineIterator.paginator(bucket_name).aggregate()
        self.assertEqual(len(result), sum(item_length))

    @mock_s3
    def test_aggregate_two_files(self):
        item_length = [12, 3]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        result = S3LineIterator.paginator(bucket_name).aggregate()
        self.assertEqual(len(result), sum(item_length))

    @mock_s3
    def test_len(self):
        item_length = [5, 3, 4, 8]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        s3iter = S3LineIterator.paginator(bucket_name)
        self.assertEqual(len(s3iter), sum(item_length))

    @mock_s3
    def test_slicing_infile(self):
        item_length = [10]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        lines = S3LineIterator.paginator(bucket_name)[2:5]
        is_ = [f'testdata {0}, {l}' for l in range(2, 5)]
        self.assertListEqual(lines, is_)

    @mock_s3
    def test_slicing_over_two_files(self):
        item_length = [10, 8]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        lines = S3LineIterator.paginator(bucket_name)[5: 12]
        self.assertEqual(lines[0], 'testdata 0, 5')
        self.assertEqual(lines[-1], 'testdata 1, 1')

    @mock_s3
    def test_slicing_over_three_files(self):
        item_length = [10, 5, 5]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        lines = S3LineIterator.paginator(bucket_name)[9:17]
        self.assertEqual('testdata 0, 9', lines[0])
        self.assertEqual('testdata 1, 0', lines[1])
        self.assertEqual('testdata 2, 1', lines[-1])

    @mock_s3
    def test_slicing_on_file_end(self):
        item_length = [2, 2]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        lines = S3LineIterator.paginator(bucket_name)[1:2]
        self.assertListEqual(['testdata 0, 1'], lines)

    @mock_s3
    def test_slicing_on_file_start(self):
        item_length = [2, 2]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        lines = S3LineIterator.paginator(bucket_name)[2:3]
        self.assertListEqual(['testdata 1, 0'], lines)

    @mock_s3
    def test_slicing_with_empty_file(self):
        item_length = [2, 0, 2]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        lines = S3LineIterator.paginator(bucket_name)[1:3]
        self.assertListEqual(['testdata 0, 1', 'testdata 2, 0'], lines)

    @mock_s3
    def test_slicing_start_to_x(self):
        item_length = [4, 8]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        lines = S3LineIterator.paginator(bucket_name)[:6]
        self.assertEqual(6, len(lines))

    @mock_s3
    def test_slicing_x_to_end(self):
        item_length = [4, 8]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        lines = S3LineIterator.paginator(bucket_name)[6:]
        self.assertEqual(6, len(lines))

    @mock_s3
    def test_slicing_start_to_end(self):
        item_length = [4, 8]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        lines = S3LineIterator.paginator(bucket_name)[:]
        self.assertEqual(sum(item_length), len(lines))

    @mock_s3
    def test_slicing_end_out_of_range(self):
        item_length = [4, 8]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        lines = S3LineIterator.paginator(bucket_name)[10:15]  # that this works is consistent with list slicing
        self.assertEqual(2, len(lines))

    @mock_s3
    def test_slicing_indexing_error_start_out_of_range(self):
        item_length = [4, 8]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        with self.assertRaises(IndexError):
            _ = S3LineIterator.paginator(bucket_name)[15:]

    @mock_s3
    def test_slicing_indexing_error_start_greater_stop(self):
        item_length = [4, 8]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        with self.assertRaises(IndexError):
            _ = S3LineIterator.paginator(bucket_name)[5:2]

    @mock_s3
    def test_slicing_not_implemented_single_subscript(self):
        item_length = [4, 8]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        with self.assertRaises(NotImplementedError):
            _ = S3LineIterator.paginator(bucket_name)[8]

    @mock_s3
    def test_slicing_not_implemented_iter_subscript(self):
        item_length = [4, 8]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        with self.assertRaises(NotImplementedError):
            for _ in S3LineIterator.paginator(bucket_name):
                pass

    @mock_s3
    def test_slicing_not_implemented_ste(self):
        item_length = [4, 8]
        bucket_name = 'test'
        self.add_s3_data(item_length, bucket_name)
        with self.assertRaises(NotImplementedError):
            _ = S3LineIterator.paginator(bucket_name)[8:12:2]
