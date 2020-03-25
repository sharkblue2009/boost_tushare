import copy
from logbook import Logger
import pickle
from functools import partial
from enum import IntEnum

import pandas as pd
import numpy as np
import plyvel

log = Logger('tusdb')

LEVEL_DB_NAME = 'D:\Database\stock_db\TUS_DB'
LEVEL_DBS = {}

DATE_FORMAT = '%Y%m%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'

def force_bytes(s):
    if isinstance(s, str):
        return s.encode()
    else:
        return s


def force_string(s):
    if isinstance(s, bytes):
        return s.decode()
    else:
        return s


class XCacheDB(object):
    name = LEVEL_DB_NAME

    def __init__(self, **kwargs):
        global LEVEL_DBS
        if not self.name in LEVEL_DBS:
            LEVEL_DBS[self.name] = plyvel.DB(
                self.name, create_if_missing=True, **kwargs)
        self.db = LEVEL_DBS[self.name]

    def close(self):
        del LEVEL_DBS[self.name]
        if isinstance(self.db, plyvel.DB):
            self.db.close()
        else:
            self.db.db.close()

    def keys(self):
        return self.db.iterator(include_value=False)

    def values(self):
        return self.db.iterator(include_key=False)

    def empty(self):
        for key in self.db.iterator(include_value=False):
            return False
        return True

    def items(self):
        return self.db.iterator()

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


class KVTYPE(IntEnum):
    TPK_RAW = 1  # Raw key, o order
    TPK_DATE = 2  # Datatime,
    TPK_INT_SEQ = 4  # sequence

    TPV_DFRAME = 11  # metadata colume names
    TPV_SER_ROW = 12  # metadata colume names
    TPV_SER_COL = 13  #
    TPV_NARR_1D = 14
    TPV_NARR_2D = 15


"""
 if value not exist, use this valid to indicate 
"""
NOT_EXIST = b'NA'


class XcAccessor(object):
    """
    对于序列保存的数据，由于原始不存在的值不存储(如股票停牌，则没有对应时间的价格值)，
    因此我们必须保证数据库中，头和尾之间的数据是完整的。
    这样才能和原始数据对齐， 从而能判断key(head<key<tail)对应的数据是否存在。
    例如价格数据，如果所取得key没有对应的value,且key位于head和tail之间，那么可判断该价格数据不存在（股票停牌）
    """
    metadata = {}

    def __init__(self, sdb, t_key, t_val, metadata=None):
        self.db = sdb
        self.tpkey = t_key
        self.tpval = t_val
        self.metadata = metadata
        return

    def to_db_key(self, key):
        if self.tpkey == KVTYPE.TPK_DATE:
            if isinstance(key, pd.Timestamp):
                real_key = force_bytes(key.strftime('%Y%m%d'))
            else:
                real_key = force_bytes(key)
        elif self.tpkey == KVTYPE.TPK_DATE:
            if isinstance(key, pd.Timestamp):
                real_key = force_bytes(key.strftime('%Y%m%d'))
            else:
                real_key = force_bytes(key)
        else:
            real_key = force_bytes(key)

        return real_key

    def to_val_in(self, val):
        """
        format val which need to database.
        :param val: value to be save
        :return: data to DB, data to APP.
        """
        if self.tpval == KVTYPE.TPV_DFRAME:
            if isinstance(val, pd.DataFrame):
                if val.empty:
                    return NOT_EXIST, pd.DataFrame(columns=self.metadata['columns'])
                if self.metadata:
                    val = val.reindex(columns=self.metadata['columns'])
                return pickle.dumps(val.values), val
            else:
                return None, pd.DataFrame(columns=self.metadata['columns'])
        elif self.tpval == KVTYPE.TPV_SER_ROW:
            if isinstance(val, pd.Series):
                if val.empty:
                    return NOT_EXIST, pd.Series(index=self.metadata['columns'])
                if self.metadata:
                    val = val.reindex(index=self.metadata['columns'])
                return pickle.dumps(val.values), val
        elif self.tpval == KVTYPE.TPV_SER_COL:
            if isinstance(val, pd.Series):
                if val.empty:
                    return NOT_EXIST, pd.Series()
                return pickle.dumps(val.values), val
        elif self.tpval == KVTYPE.TPV_NARR_1D or \
                self.tpval == KVTYPE.TPV_NARR_2D:
            if isinstance(val, np.ndarray):
                if val.size == 0:
                    return NOT_EXIST, val
                return pickle.dumps(val), val
        else:
            return force_bytes(val), val

        return None, None

    def to_val_out(self, val):

        if self.tpval == KVTYPE.TPV_DFRAME:
            cols = self.metadata['columns']
            if val == NOT_EXIST:
                realval = pd.DataFrame(columns=cols)
            else:
                val = pickle.loads(val)
                realval = pd.DataFrame(data=val, columns=cols)
        elif self.tpval == KVTYPE.TPV_SER_ROW:
            cols = self.metadata['columns']
            if val == NOT_EXIST:
                realval = pd.Series(index=cols)
            else:
                val = pickle.loads(val)
                realval = pd.Series(data=val, index=cols)
        elif self.tpval == KVTYPE.TPV_SER_COL:
            if val == NOT_EXIST:
                realval = pd.Series()
            else:
                val = pickle.loads(val)
                realval = pd.Series(data=val)
        else:
            realval = val

        return realval

    def load(self, key):
        """

        :param key:
        :return:
        """
        key = self.to_db_key(key)
        if key:
            val = self.db.get(key)
            if val:
                return self.to_val_out(val)
        return None

    def save(self, key, val):
        """"""
        key = self.to_db_key(key)
        dbval, appval = self.to_val_in(val)
        if key and dbval:
            self.db.put(key, dbval)
        return appval

    def remove(self, key):
        """"""
        key = self.to_db_key(key)
        if key:
            self.db.delete(key)

    def get_keys(self):
        """"""
        keys = list(self.db.iterator(include_value=False))
        return keys

    def get_key_range(self):
        if self.tpkey == KVTYPE.TPK_DATE or self.tpkey == KVTYPE.TPK_DATE:
            iter = self.db.iterator(include_value=False)
            start = force_string(iter.next())
            end = force_string(iter.seek_to_stop().prev())
            return (start, end)
        else:
            return None
