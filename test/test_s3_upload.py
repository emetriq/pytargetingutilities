import unittest

import pytest
from moto import mock_s3
from pytargetingutilities.aws.s3.helper import S3Helper
import json
from pytargetingutilities.tools.hash import md5_str


class TestS3Helper(unittest.TestCase):

    @mock_s3
    def test_write_with_hash(self):
        def write(output_path):
            with open(output_path, 'w') as f:
                json.dump({'TEST': 1}, f)
        pytest.create_bucket('test')
        S3Helper.write_with_hash('test', 'my_file.txt', write)
        my_file_content = S3Helper.read('test', 'my_file.txt')
        my_file_hash_content = S3Helper.read('test', 'my_file.txt.md5')
        self.assertEqual(my_file_content, b'{"TEST": 1}')
        self.assertEqual(my_file_hash_content.decode('utf-8'), md5_str('{"TEST": 1}'))

    @mock_s3
    def test_write_with_hash_with_hash_check(self):
        def write(output_path):
            with open(output_path, 'w') as f:
                json.dump({'TEST': 1}, f)
        pytest.create_bucket('test')
        S3Helper.write_with_hash('test', 'my_file.txt', write)
        my_file_content = S3Helper.read('test', 'my_file.txt')
        my_file_hash_content = S3Helper.read('test', 'my_file.txt.md5')
        self.assertEqual(my_file_content, b'{"TEST": 1}')
        self.assertEqual(my_file_hash_content.decode('utf-8'), md5_str('{"TEST": 1}'))
        hashed = S3Helper.write_with_hash('test', 'my_file.txt', write, True)
        self.assertTrue(hashed)

    @mock_s3
    def test_hash_check(self):
        def write(output_path):
            with open(output_path, 'w') as f:
                json.dump({'TEST': 1}, f)
        pytest.create_bucket('test')
        self.assertFalse(S3Helper.hash_check("test", "my_file.txt", write))


if __name__ == '__main__':
    unittest.main()
