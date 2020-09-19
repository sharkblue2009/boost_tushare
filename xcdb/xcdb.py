"""
numpy对象，做序列化（pickle）的时候，f4, f8, RAW, Unicode类型操作效率基本相当，
    object类型效率很低，原因可能是object是对象的引用，因此需要两层解析。
    结构体类型效率narray, 低于单一类型。
    pickle对structed_narray的读写效率，也略高于对recarray的效率(10%左右的提升)
"""
import pickle
from abc import abstractmethod
from enum import IntEnum

import numpy as np
import pandas as pd
from logbook import Logger

log = Logger('xcdb')

DBS_OPENED = {}

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
    """
    Database for caching.
    """

    @abstractmethod
    def close(self):
        """"""

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

    @abstractmethod
    def show(self):
        """"""

    @abstractmethod
    def get_sdb(self, sdb_path: str):
        """"""


class KVTYPE(IntEnum):
    TPK_RAW = 1  # Raw key, o order
    TPK_DATE = 2  # Datatime,
    TPK_INTS = 4  # integer sequence

    TPV_DFRAME = 11  # metadata colume names
    TPV_SERIES = 13  #
    TPV_INDEX = 14
    TPV_OBJECT = 16  # object


class IOFLAG(IntEnum):
    READ_DBONLY = 0  # Read from DB cache only
    READ_XC = 1  # Read from BD cache, if miss, load from NET
    READ_NETDB = 2  # Read from Net first, then flush to DB.
    # READ_NETONLY = 3   # Read from Net only, not flush to DB.

    ERASE_INVALID = 10  # Erase invalid&missed data
    ERASE_ALL = 11  # Erase SDB

    # UPDATE_MISS = 20  # Update missed/NA data
    # UPDATE_INVALID = 21  # Update invalid&missed data
    # UPDATE_ALL = 23  # Update all data


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
    Note: read-write transactions may be nested.
        write transition begin-commit block can not insert other transition without nesting.

    """
    metadata = {}
    tpkey = KVTYPE.TPK_RAW
    tpval = KVTYPE.TPV_DFRAME

    def to_db_key(self, key):
        """

        :param key:
        :return:
        """
        if self.tpkey == KVTYPE.TPK_DATE:
            if isinstance(key, pd.Timestamp):
                real_key = force_bytes(key.strftime('%Y%m%d'))
            else:
                real_key = force_bytes(key)
        elif self.tpkey == KVTYPE.TPK_INTS:
            if isinstance(key, int):
                real_key = force_bytes(str(key))
            else:
                real_key = force_bytes(key)
        else:
            real_key = force_bytes(key)

        return real_key

    def to_val_in(self, val, raw_mode=False):
        """
        format val which need to database.
        :param val: value to be save
        :param raw_mode: if to use ndarray instead of pandas objects(DF, Ser, Index) as app_val for output
        :return: data to DB, data to APP.
        """
        if self.tpval == KVTYPE.TPV_DFRAME:
            if not raw_mode:
                if isinstance(val, pd.DataFrame):
                    if val.empty:
                        return NOT_EXIST, pd.DataFrame(columns=self.metadata['columns'])
                    val = val.reindex(columns=self.metadata['columns'])
                    dbval = val.values
                    if 'dtype' in self.metadata.keys():
                        dbval = dbval.astype(self.metadata['dtype'])
                    return pickle.dumps(dbval), val
                else:
                    return None, pd.DataFrame(columns=self.metadata['columns'])
            else:
                if isinstance(val, pd.DataFrame):
                    if val.empty:
                        return NOT_EXIST, np.empty((0, len(self.metadata['columns'])))
                    val = val.reindex(columns=self.metadata['columns'])
                    dbval = val.values
                    if 'dtype' in self.metadata.keys():
                        dbval = dbval.astype(self.metadata['dtype'])
                    return pickle.dumps(dbval), dbval
                else:
                    return None, pd.DataFrame(columns=self.metadata['columns'])
        elif self.tpval == KVTYPE.TPV_SERIES:
            if isinstance(val, pd.Series):
                if val.empty:
                    return NOT_EXIST, val
                dbval = val.values
                if 'dtype' in self.metadata.keys():
                    dbval = dbval.astype(self.metadata['dtype'])
                return pickle.dumps(dbval), val
        elif self.tpval == KVTYPE.TPV_INDEX:
            if isinstance(val, pd.Index):
                val = val.values
            if isinstance(val, np.ndarray):
                if val.size == 0:
                    return NOT_EXIST, val
                return pickle.dumps(val), val
        elif self.tpval == KVTYPE.TPV_OBJECT:
            return pickle.dumps(val), val
        else:
            return force_bytes(val), val

        return None, None

    def to_val_out(self, val, raw_mode=False):
        """

        :param val:
        :param raw_mode: if use ndarray as app_val for output
        :return:
        """
        realval = val
        if self.tpval == KVTYPE.TPV_DFRAME:
            cols = self.metadata['columns']
            if not raw_mode:
                if val == NOT_EXIST:
                    realval = pd.DataFrame(columns=cols)
                else:
                    dbval = pickle.loads(val)
                    realval = pd.DataFrame(data=dbval, columns=cols)
            else:
                if val == NOT_EXIST:
                    realval = np.empty((0, len(cols)))
                else:
                    dbval = pickle.loads(val)
                    realval = dbval
        elif self.tpval == KVTYPE.TPV_SERIES:
            if val == NOT_EXIST:
                realval = pd.Series()
            else:
                dbval = pickle.loads(val)
                realval = pd.Series(data=dbval)
        elif self.tpval == KVTYPE.TPV_INDEX:
            if val == NOT_EXIST:
                realval = np.empty((0,))
            else:
                realval = pickle.loads(val)
        elif self.tpval == KVTYPE.TPV_OBJECT:
            realval = pickle.loads(val)

        return realval

    @abstractmethod
    def load(self, key, raw_mode=False):
        """

        :param key:
        :param raw_mode: override value type for output.
        :return:
        """

    @abstractmethod
    def save(self, key, val, raw_mode=None):
        """"""

    @abstractmethod
    def remove(self, key):
        """"""

    @abstractmethod
    def commit(self):
        """"""

    @abstractmethod
    def load_range(self, kstart, kend, raw_mode):
        """"""
