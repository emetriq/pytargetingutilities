import boto3
from datetime import datetime, timedelta, timezone
from pytargetingutilities.aws.s3.base_paginator import S3BasePaginator

class S3Iterator(S3BasePaginator):

    def __init__(self, pages, meta):
        self._ctx = 0
        super(S3Iterator,self).__init__(pages, meta)

    def __iter__(self):
        for page in self._pages:
            if 'Contents' in page:
                for item in page['Contents']:
                    self._keys.append(item)
            else:
                self._keys.append(page)
        return self

    def __next__(self):
        if self._ctx >= len(self._keys):
            raise StopIteration
        result = (
            boto3.resource('s3')
            .Object(self._meta['bucket'], self._keys[self._ctx]['Key'])
            .get()['Body']
            .read()
            .decode('utf-8')
            .strip()
        )
        self._ctx += 1
        return result

    def aggregate(self):
        ys = []
        for page in self:
            ys.append(page)
        return ys
