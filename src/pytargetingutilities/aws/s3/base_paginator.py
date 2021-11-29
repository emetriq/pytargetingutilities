import boto3
from datetime import datetime, timedelta, timezone


class S3BasePaginator(object):
    def __init__(self, pages, meta):
        self._pages = pages
        self._meta = meta
        self._keys = []

    @staticmethod
    def __paginator(bucket_name, directory, max_items, continuation_token):
        cfg = {
            'Bucket': bucket_name,
            'PaginationConfig': {'PageSize': max_items},
        }
        if directory:
            cfg['Prefix'] = directory

        if continuation_token:
            cfg['ContinuationToken'] = continuation_token

        return (
            boto3.client('s3').get_paginator('list_objects_v2').paginate(**cfg)
        )

    @staticmethod
    def __build_filter(filter_query):
        return "Contents[?" + " && ".join(filter_query) + "]"

    @classmethod
    def date_paginator(
            cls,
            bucket_name,
            max_age,
            max_items=1000,
            directory=None,
            ends_with=None,
            continuation_token=None,
    ):
        pages = cls.__paginator(
            bucket_name, directory, max_items, continuation_token
        )
        filter_date = (
            datetime.utcnow().replace(tzinfo=timezone.utc)
            - timedelta(days=max_age)
        ).strftime('%Y-%m-%d %H:%M:%S')
        query_filters = [
            f"to_string(LastModified)>='\"{filter_date}\"'"
        ]
        if ends_with:
            query_filters.append(f"ends_with(Key, `{ends_with}`)")
        querystr = cls.__build_filter(query_filters)

        return cls(
            pages.search(querystr),
            meta={
                'bucket': bucket_name,
                'max_items': max_items,
                'directory': directory,
            },
        )

    @classmethod
    def paginator(
            cls,
            bucket_name,
            max_items=1000,
            directory=None,
            ends_with=None,
            continuation_token=None,
    ):
        pages = cls.__paginator(
            bucket_name, directory, max_items, continuation_token
        )

        if ends_with:
            querystr = cls.__build_filter([f"ends_with(Key, `{ends_with}`)"])
            pages = pages.search(querystr)
        return cls(
            pages,
            meta={
                'bucket': bucket_name,
                'max_items': max_items,
                'directory': directory,
            },
        )