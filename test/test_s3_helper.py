import unittest

import pytest
from moto import mock_s3
from pytargetingutilities.aws.s3.helper import S3Helper


class TestS3Helper(unittest.TestCase):
    @mock_s3
    def test_get_latest_directory(self):
        pytest.create_bucket('test')
        mydates = [
            pytest.get_date(100),
            pytest.get_date(40),
            pytest.get_date(2),
            pytest.get_date(80),
        ]
        for idx, mydate in enumerate(mydates):
            pytest.add_dummy_data(
                'test', f'test/123456{idx}/{idx}.json', f'{idx}', mydate
            )
        latest_directory = S3Helper.get_latest_directory(
            'test', '', 'us-east-1'
        )
        self.assertEqual(latest_directory, 'test/1234562')
        for idx, mydate in enumerate(mydates):
            pytest.add_dummy_data(
                'test', f'newer/558{idx}/{idx}.json', f'{idx}', mydate
            )
        latest_directory = S3Helper.get_latest_directory(
            'test', 'test', 'us-east-1'
        )
        self.assertEqual(latest_directory, 'test/1234562')
        latest_directory = S3Helper.get_latest_directory(
            'test', '', 'us-east-1'
        )
        self.assertEqual(latest_directory, 'newer/5582')

    @mock_s3
    def test_object_newer_than_isnewer(self):
        pytest.create_bucket('test')
        pytest.add_dummy_data(
            'test', 'test/file/jay.son', "test", pytest.get_date(0)
        )
        older_date = pytest.get_date(10)
        self.assertTrue(S3Helper.object_newer_than(
            "test", "test/file/jay.son", older_date
        ))

    @mock_s3
    def test_object_newer_than_isolder(self):
        pytest.create_bucket('test')
        pytest.add_dummy_data(
            'test', 'test/file/jay.son', "test", pytest.get_date(10)
        )
        older_date = pytest.get_date(0)
        self.assertFalse(S3Helper.object_newer_than(
            "test", "test/file/jay.son", older_date
        ))

    @mock_s3
    def test_object_newer_than_sameage(self):
        pytest.create_bucket('test')
        pytest.add_dummy_data(
            'test', 'test/file/jay.son', "test", pytest.get_date(0)
        )
        older_date = pytest.get_date(0)
        self.assertFalse(S3Helper.object_newer_than(
            "test", "test/file/jay.son", older_date
        ))


if __name__ == '__main__':
    unittest.main()
