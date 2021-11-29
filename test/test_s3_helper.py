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
        latest_directory = S3Helper.get_latest_top_url_directory(
            'test', 'us-east-1'
        )
        self.assertEqual(latest_directory, 'test/1234562')


if __name__ == '__main__':
    unittest.main()
