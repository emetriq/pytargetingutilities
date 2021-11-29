from typing import List, Tuple, Union

import boto3

from pytargetingutilities.aws.s3.base_paginator import S3BasePaginator


class S3LineIterator(S3BasePaginator):
    """
    An iterator over all lines of all given s3 objects.
    Itarting over multiple objects is achieved by fixing the order of the objects and keeping track at which index of
    the iterator each object begins. Thus, if we take a slice out of our iterator, we know which lines to take of which
    objects.
    This is a 'nested' iterator, since we iterate over objects and inside the objects we iterate over lines. We identify
    the objects by its key. Thus, we call the start and stop of a slice of the iterator iter_start(stop) and the start
    and stop of a slice inside a object key_start(stop).
    """

    def __init__(self, pages, meta):
        super().__init__(pages, meta)
        self._key_len_map = None
        self.total_lines = None
        self._load_keys()
        self._set_key_len_map()

    def __len__(self) -> int:
        return self.total_lines

    def __getitem__(self, item: slice) -> List[str]:
        """ slice of lines from s3 objects; is only defined for slices without step [a:b:None], not subscript [a] """
        if not isinstance(item, slice):
            raise NotImplementedError('Only slicing is allowed, not subscripting; '
                                      'if you want to iterate over all lines, use [:]')
        if item.step is not None:
            raise NotImplementedError('Slicing with step is not implemented')
        if item.start is not None and item.start > self.__len__():
            raise IndexError('index is out of range')
        if item.start is not None and item.stop is not None and item.start >= item.stop:
            raise IndexError('start must be lesser than stop')
        key_slices = self._get_key_slices(item.start, item.stop)
        data = []
        for key_start_stop in key_slices:
            data.extend(self._object_get_lines(*key_start_stop))
        return data

    def _load_keys(self) -> None:
        """ load the keys of all objects in pages """
        for page in self._pages:
            if 'Contents' in page:
                for item in page['Contents']:
                    self._keys.append(item['Key'])
            else:
                self._keys.append(page['Key'])
        self._keys.sort()

    def _set_key_len_map(self) -> None:
        """ determine len of objects and the index of their first line in the context of the whole iterator """
        key_len_map = []  # list of tuples (key of object, number of lines in object, index of line 0 in iterator)
        iter_index = 0
        for key in self._keys:
            key_len = self._object_len(key)
            key_len_map.append((key, key_len, iter_index))
            iter_index += key_len
        self._key_len_map = key_len_map
        self.total_lines = iter_index

    def _object_len(self, key: str) -> int:
        i = -1  # in case there are no lines to iterate over
        for i, _ in enumerate(boto3.resource('s3')
                                   .Object(self._meta['bucket'], key)
                                   .get()['Body']
                                   .iter_lines()):
            pass
        return i + 1

    def _get_key_slices(self, iter_start: Union[int, None], iter_stop: Union[int, None]) -> List[Tuple[str, int, int]]:
        """
        for each key get the start and stop of the slice of the object that belong to the iterator slice defined by
        iter_start and iter_stop; if the key is not part of the slice, it is not in the returned list; key_start may
        be negative and key_stop may be greater than the object_len, both of which work as intended.
        Args:
            iter_start: start of iterator
            iter_stop: stop of iterator

        Returns:
            [(key, key_start, key_stop),]; only contains keys that are part of the slice
        """
        if iter_start is None:
            iter_start = 0
        if iter_stop is None:
            iter_stop = len(self)
        key_slices = []
        for key, object_len, line0_index in self._key_len_map:
            if iter_stop < line0_index or (iter_start >= (line0_index + object_len)):  # object not in iter slice
                continue
            key_start = iter_start - line0_index  # may be negative, which does not hurt
            key_stop = iter_stop - line0_index  # if iter_stop in later object, this is > object_len
            key_slices.append((key, key_start, key_stop))
        return key_slices

    def _object_get_lines(self, key: str, key_start: int, key_stop: int) -> List[str]:
        """ iterate over the object belonging to key and get the lines from start to stop """
        lines = []
        for index, line in enumerate(boto3.resource('s3')
                                          .Object(self._meta['bucket'], key)
                                          .get()['Body']
                                          .iter_lines()):
            if index < key_start:
                continue
            if index >= key_stop:
                break
            lines.append(line.decode('utf-8').strip())
        return lines

    def aggregate(self) -> List[str]:
        """ Return all lines of all objects """
        return [line for line in self[:]]
