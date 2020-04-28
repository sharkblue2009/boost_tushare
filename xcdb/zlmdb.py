import copy
from logbook import Logger
import pickle
from functools import partial
from enum import IntEnum

import pandas as pd
import numpy as np
import lmdb

from .xcdb import *

log = Logger('lmdb')

LMDB_NAME = 'D:/Database/stock_db/TUS_LMDB'


class XcLMDB(XCacheDB):
    """
    Database for caching.
    """

    def __init__(self, name, readonly=True, **kwargs):
        global DBS_OPENED
        self.name = name
        log.info('LMDB version: {}'.format(lmdb.version()))
        if not self.name in DBS_OPENED.keys():
            log.info('Open DB: {}'.format(self.name))
            DBS_OPENED[self.name] = lmdb.open(
                self.name, create=True, max_dbs=1000000, readonly=readonly, map_size=32 * 0x40000000, **kwargs)
        else:
            raise FileExistsError('Already opened')

        self.env = DBS_OPENED[self.name]
        self.show()

    def close(self):
        if isinstance(self.env, lmdb.Environment):
            self.env.close()
            self.env = None
        global DBS_OPENED
        if self.name in DBS_OPENED.keys():
            print('Close DB: {}'.format(self.name))
            DBS_OPENED.pop(self.name)

    def __del__(self):
        self.close()

    def show(self):
        print(self.env.info())
        # print(self.env.stat())
        # print(self.env.max_key_size())

    @staticmethod
    def _get_sdb(master_db, sdb_path: str):
        cur_db = master_db.open_db(force_bytes(sdb_path))
        return cur_db

    def get_sdb(self, sdb_path: str):
        try:
            # log.info('>>SDB>>{}'.format(sdb_path))
            sdb = self._get_sdb(self.env, sdb_path)
        except:
            print('sdb_path not exist: {}'.format(sdb_path))
            raise ValueError('create sdb error')

        return sdb


class XcLMDBAccessor(XcAccessor):
    """
    对于序列保存的数据，由于原始不存在的值不存储(如股票停牌，则没有对应时间的价格值)，
    因此我们必须保证数据库中，头和尾之间的数据是完整的。
    这样才能和原始数据对齐， 从而能判断key(head<key<tail)对应的数据是否存在。
    例如价格数据，如果所取得key没有对应的value,且key位于head和tail之间，那么可判断该价格数据不存在（股票停牌）
    Note: read-write transactions may be nested.
        write transition begin-commit block can not insert other transition without nesting.

    """
    metadata = {}

    def __init__(self, master_db: XcLMDB, sdb: str, metadata=None, readonly=False):
        self.master = master_db
        self.db = master_db.get_sdb(sdb)
        self.tpkey = metadata['tpk']
        self.tpval = metadata['tpv']
        self.metadata = metadata
        self.txn = self.master.env.begin(db=self.db, write=True, parent=None)
        return

    def __del__(self):
        """"""
        try:
            self.txn.commit()
        except:
            pass

    def load(self, key, vtype=None):
        """

        :param key:
        :param vtype: override value type for output.
        :return:
        """
        # self.txn = self.master.env.begin(db=self.db, write=True)
        key = self.to_db_key(key)
        if key:
            val = self.txn.get(key)
            if val:
                return self.to_val_out(val, vtype)
        return None

    def save(self, key, val, vtype=None):
        """"""
        # self.txn = self.master.env.begin(db=self.db, write=True)
        key = self.to_db_key(key)
        dbval, appval = self.to_val_in(val, vtype)
        if key and dbval:
            self.txn.put(key, dbval)
        return appval

    def remove(self, key):
        """"""
        key = self.to_db_key(key)
        if key:
            self.txn.delete(key)

    def commit(self):
        self.txn.commit()

    def load_range(self, kstart, kend, vtype):
        """"""
        out = {}
        with self.txn.cursor(self.db) as cur:
            bstr = cur.set_range(force_bytes(kstart))
            if bstr:
                while True:
                    k, v = cur.item()
                    sk = force_string(k)
                    out[sk] = self.to_val_out(v, vtype)
                    if sk >= kend:
                        break
                    vld = cur.next()
                    if not vld:
                        break
            cur.close()

        return out
