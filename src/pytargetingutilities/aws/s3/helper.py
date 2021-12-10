import datetime as dt
import json
import os
from tempfile import NamedTemporaryFile as tmp

import boto3
from botocore.exceptions import ClientError

from pytargetingutilities.aws.s3.iterator import S3Iterator
from pytargetingutilities.tools.hash import md5, md5_str


class S3Helper:
    @staticmethod
    def get_latest_directory(s3bucket, prefix='', region_name='eu-west-1'):
        if prefix is None:
            prefix = ''
        s3_client = boto3.client('s3', region_name=region_name)
        response = s3_client.list_objects_v2(Bucket=s3bucket, Prefix=prefix)
        if 'Contents' not in response:
            return None
        # Get the key of the object with the highest LastModified date
        latest_object = max(
            response['Contents'], key=lambda obj: obj['LastModified']
        )['Key']
        return os.path.dirname(latest_object)

    @staticmethod
    def merge_json(bucket, input_directory, output_directory, filename):
        s3_paginator = S3Iterator.paginator(
            bucket_name=bucket,
            directory=input_directory,
            ends_with='.json',
        )

        json_data = json.dumps(
            dict(
                (key, d[key])
                for d in [json.loads(content) for content in s3_paginator]
                for key in d
            )
        )

        hash_str = md5_str(json_data)

        boto3.resource('s3').Bucket('crtt-batch').objects.filter(
            Prefix=f'{output_directory}'
        ).delete()

        s3 = boto3.client('s3')
        s3.put_object(
            Bucket='crtt-batch',
            Key=f'{output_directory}{filename}',
            Body=json_data.encode(),
        )

        s3.put_object(
            Bucket='crtt-batch',
            Key=f'{output_directory}{filename}.md5',
            Body=hash_str.encode(),
        )

    @staticmethod
    def write_with_hash(
        bucket, file_path, writer_func, hash_check: bool = False
    ):
        with tmp() as map_file, tmp() as hash_file:
            writer_func(map_file.name)
            myhash = md5(map_file.name)
            if hash_check and S3Helper.__hash_check(bucket, file_path, myhash):
                return False
            hash_file.write(myhash.encode())
            hash_file.flush()
            S3Helper.__write(map_file.name, bucket, file_path)
            S3Helper.__write(hash_file.name, bucket, f'{file_path}.md5')
            return True

    @staticmethod
    def __hash_check(bucket, file_path, file_hash):
        """Returns true if hash has not changed"""
        # be agnostic whether file or its md5 file is in file_path
        file_path.rstrip(".md5")
        try:
            if S3Helper.read(bucket, f'{file_path}.md5') == file_hash:
                return True
            return False
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'NoSuchKey':
                return False
            raise

    @staticmethod
    def hash_check(bucket, file_path, writer_func):
        """Returns true if hash of file on s3 has not changed"""
        with tmp() as map_file:
            writer_func(map_file.name)
            myhash = md5(map_file.name)
            return S3Helper.__hash_check(bucket, file_path, myhash)

    @staticmethod
    def write(bucket, file_path, writer_func):
        with tmp() as map_file:
            writer_func(map_file.name)
            S3Helper.__write(map_file.name, bucket, file_path)

    @staticmethod
    def __write(file_name, bucket, key):
        boto3.resource('s3').meta.client.upload_file(file_name, bucket, key)

    @staticmethod
    def read(bucket_name, key):
        s3 = boto3.client('s3')
        data = s3.get_object(Bucket=bucket_name, Key=key)
        return data['Body'].read()

    @staticmethod
    def download(bucket_name: str, key: str, destination: str):
        """
        Writes content of s3 object to file
        If the file already exists it is overwritten

        Args:
            bucket_name (str): name of the s3 bucket
            key (str): key of object in s3
            destination (str): path of file

        Returns:
            None - content of object is written to file
        """
        s3 = boto3.resource('s3')
        s3.Object(bucket_name, key).download_file(destination)

    @staticmethod
    def download_latest(
            bucket_name: str,
            key: str,
            destination: str,
            timestamp: dt.datetime
    ) -> bool:
        """
        Writes content of s3 object to file, if age of object is younger than
        timestamp

        Args:
            bucket_name (str): name of the s3 bucket
            key (str): key of object in s3
            destination (str): path of file
            timestamp (datetime): timestamp against which the age of the
                object is compared

        Returns:
            bool: whether file was written
        """
        if S3Helper.object_newer_than(bucket_name, key, timestamp):
            S3Helper.download(bucket_name, key, destination)
            return True
        return False

    @staticmethod
    def object_newer_than(
            bucket_name: str, key: str, timestamp: dt.datetime
    ) -> bool:
        """
        Returns true if object on s3 is younger than timestamp, else false

        Args:
            bucket_name (str): name of the s3 bucket
            key (str): key of object in s3
            timestamp (int): timestamp against which the age of the object is
                compared

        Returns:
            bool: True if object is younger, else false
        """
        s3 = boto3.resource('s3')
        last_modified = s3.Object(bucket_name, key).last_modified
        return last_modified > timestamp

    @staticmethod
    def upload_filtered_directory(
        bucket, prefix, local_directory, extension, delete_existing
    ):
        """Uploads local directory to s3 bucket.
        The local directory is filtered by given extension

        Args:
            bucket (str): s3 target bucket
            prefix (str): s3 upload location. Prefix must end with /
            local_directory (str): local directory
            extension (str): filter for local directory. If you want to upload only
            wheel packages set extension to .whl
            delete_existing (bool): deletes target directory if set to true
        """
        if not prefix.endswith('/'):
            raise AttributeError('Prefix must end with /')
        s3 = boto3.resource('s3')
        if delete_existing:
            s3.Bucket(bucket).objects.filter(Prefix=prefix).delete()
        list(
            map(
                lambda local_file: s3.meta.client.upload_file(
                    os.path.join(local_directory, local_file),
                    bucket,
                    f'{prefix}{local_file}',
                ),
                filter(
                    lambda x: x.endswith(extension),
                    os.listdir(local_directory),
                ),
            )
        )
