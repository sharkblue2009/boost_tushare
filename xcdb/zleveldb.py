import copy
from logbook import Logger
import pickle
from functools import partial
from enum import IntEnum

import pandas as pd
import numpy as np
import plyvel

from .xcdb import *

log = Logger('lvdb')

LEVELDB_NAME = 'D:/Database/stock_db/TUS_LVDB'


class XcLevelDB(XCacheDB):
    """
    Database for caching.
    """

    def __init__(self, name, readonly=True, **kwargs):
        global DBS_OPENED
        self.name = name
        if not self.name in DBS_OPENED:
            DBS_OPENED[self.name] = plyvel.DB(
                self.name, create_if_missing=True, **kwargs)
        self.db = DBS_OPENED[self.name]

    def close(self):
        if isinstance(self.db, plyvel.DB):
            self.db.close()
        else:
            self.db.db.close()
        if self.name in DBS_OPENED.keys():
            print('Close DB: {}'.format(self.name))
            DBS_OPENED.pop(self.name)

    # def keys(self):
    #     return self.db.iterator(include_value=False)
    #
    # def values(self):
    #     return self.db.iterator(include_key=False)
    #
    # def empty(self):
    #     for key in self.db.iterator(include_value=False):
    #         return False
    #     return True
    #
    # def items(self):
    #     return self.db.iterator()

    def show(self):
        print(list(self.db.iterator(include_value=False)))
        log.info('create subdb: {}'.format(self.db.get_property(b'leveldb.stats')))

    @staticmethod
    def _get_sdb(master_db, sdb_path: str):
        lst_prefix = sdb_path.split(':')
        cur_db = master_db
        for pre in lst_prefix:
            cur_db = cur_db.prefixed_db(force_bytes(pre))
        return cur_db

    def get_sdb(self, sdb_path: str):
        try:
            # log.info('>>SDB>>{}'.format(sdb_path))
            sdb = self._get_sdb(self.db, sdb_path)
        except:
            raise ValueError('create sdb error')

        return sdb


class XcLevelDBAccessor(XcAccessor):
    """
    对于序列保存的数据，由于原始不存在的值不存储(如股票停牌，则没有对应时间的价格值)，
    因此我们必须保证数据库中，头和尾之间的数据是完整的。
    这样才能和原始数据对齐， 从而能判断key(head<key<tail)对应的数据是否存在。
    例如价格数据，如果所取得key没有对应的value,且key位于head和tail之间，那么可判断该价格数据不存在（股票停牌）
    Note: read-write transactions may be nested.
        write transition begin-commit block can not insert other transition without nesting.

    """
    metadata = {}

    def __init__(self, master_db: XcLevelDB, sdb: str, metadata=None, readonly=False):
        self.master = master_db
        self.db = master_db.get_sdb(sdb)
        self.tpkey = metadata['tpk']
        self.tpval = metadata['tpv']
        self.metadata = metadata
        return

    def __del__(self):
        """"""


    def load(self, key, vtype=None):
        """

        :param key:
        :param vtype: override value type for output.
        :return:
        """
        key = self.to_db_key(key)
        if key:
            val = self.db.get(key)
            if val:
                return self.to_val_out(val, vtype)
        return None

    def save(self, key, val, vtype=None):
        """"""
        key = self.to_db_key(key)
        dbval, appval = self.to_val_in(val, vtype)
        if key and dbval:
            self.db.put(key, dbval)
        return appval

    def remove(self, key):
        """"""
        key = self.to_db_key(key)
        if key:
            self.db.delete(key)

    def commit(self):
        """"""

    def load_range(self, kstart, kend, vtype):
        """"""
        # out = {}
        # with self.txn.cursor(self.db) as cur:
        #     bstr = cur.set_range(force_bytes(kstart))
        #     if bstr:
        #         while True:
        #             k, v = cur.item()
        #             sk = force_string(k)
        #             out[sk] = self.to_val_out(v, vtype)
        #             if sk >= kend:
        #                 break
        #             vld = cur.next()
        #             if not vld:
        #                 break
        #     cur.close()
        #
        # return out
