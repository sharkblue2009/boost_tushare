import copy
from logbook import Logger
import pickle
from functools import partial
from enum import IntEnum

import pandas as pd
import numpy as np
import plyvel

log = Logger('tusdb')

LEVEL_DB_NAME = 'TUS_DB'
LEVEL_DBS = {}


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
            log.info('>>SDB>>{}'.format(sdb_path))
            sdb = self._get_sdb(self.db, sdb_path)
        except:
            raise ValueError('create sdb error')

        return sdb


def find_closest_date(all_dates, dt, mode='backward'):
    """

    :param all_dates:
    :param dt:
    :param mode:
    :return:
    """
    tt_all_dates = pd.to_datetime(all_dates, format='%Y%m%d')
    tt_dt = pd.Timestamp(dt)
    if mode == 'backward':
        valid = tt_all_dates[tt_all_dates <= tt_dt]
        if len(valid) > 0:
            return valid[-1].strftime('%Y%m%d')
    else:
        valid = tt_all_dates[tt_all_dates >= tt_dt]
        if len(valid) > 0:
            return valid[0].strftime('%Y%m%d')
    return None


class KVTYPE(IntEnum):
    TPK_RAW = 1  # Raw key, o order
    TPK_DT_DAY = 2  # Datatime,
    TPK_DT_MONTH = 3
    TPK_INT_SEQ = 4  # sequence

    TPV_DFRAME = 11  # metadata colume names
    TPV_SER_ROW = 12  # metadata colume names
    TPV_SER_COL = 13  #
    TPV_NARR_1D = 14
    TPV_NARR_2D = 15


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
        if self.tpkey == KVTYPE.TPK_DT_DAY:
            if isinstance(key, pd.Timestamp):
                real_key = force_bytes(key.strftime('%Y%m%d'))
            else:
                real_key = force_bytes(key)
        elif self.tpkey == KVTYPE.TPK_DT_MONTH:
            if isinstance(key, pd.Timestamp):
                real_key = force_bytes(key.strftime('%Y%m%d'))
            else:
                real_key = force_bytes(key)
        else:
            real_key = force_bytes(key)

        return real_key

    def to_val_in(self, val):
        """"""
        if self.tpval == KVTYPE.TPV_DFRAME:
            if isinstance(val, pd.DataFrame):
                return pickle.dumps(val.values)
            else:
                return None

        elif self.tpval == KVTYPE.TPV_SER_ROW:
            if isinstance(val, pd.Series):
                return pickle.dumps(val.values)
        elif self.tpval == KVTYPE.TPV_SER_COL:
            if isinstance(val, pd.Series):
                return pickle.dumps(val.values)

        elif self.tpval == KVTYPE.TPV_NARR_1D or \
                self.tpval == KVTYPE.TPV_NARR_2D:
            if isinstance(val, np.ndarray):
                return pickle.dumps(val.values)
        else:
            return force_bytes(val)

        return None

    def to_val_out(self, val):

        if self.tpval == KVTYPE.TPV_DFRAME:
            val = pickle.loads(val)
            cols = self.metadata['columns']
            realval = pd.DataFrame(data=val, columns=cols)
        elif self.tpval == KVTYPE.TPV_SER_ROW:
            val = pickle.loads(val)
            cols = self.metadata['columns']
            realval = pd.Series(data=val, columns=cols)
        elif self.tpval == KVTYPE.TPV_SER_COL:
            val = pickle.loads(val)
            realval = pd.Series(data=val)
        else:
            realval = val

        return realval

    def load(self, key):
        """"""
        key = self.to_db_key(key)
        if key:
            val = self.db.get(key)
            if val:
                return self.to_val_out(val)
        return None

    def save(self, key, val):
        """"""
        key = self.to_db_key(key)
        val = self.to_val_in(val)
        if key and val:
            self.db.put(key, val)

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
        if self.tpkey == KVTYPE.TPK_DT_DAY or self.tpkey == KVTYPE.TPK_DT_MONTH:
            iter = self.db.iterator(include_value=False)
            start = force_string(iter.next())
            end = force_string(iter.seek_to_stop().prev())
            return (start, end)
        else:
            return None
