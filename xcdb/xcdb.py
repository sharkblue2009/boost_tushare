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
    TPK_INT_ID = 4  # integer sequence

    TPV_DFRAME = 11  # metadata colume names
    TPV_SERIES = 13  #
    TPV_NARR_1D = 14
    TPV_NARR_2D = 15


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
        elif self.tpkey == KVTYPE.TPK_INT_ID:
            if isinstance(key, int):
                real_key = force_bytes(str(key))
            else:
                real_key = force_bytes(key)
        else:
            real_key = force_bytes(key)

        return real_key

    def to_val_in(self, val, vtype):
        """
        format val which need to database.
        :param val: value to be save
        :return: data to DB, data to APP.
        """
        if self.tpval == KVTYPE.TPV_DFRAME:
            if vtype is None:
                if isinstance(val, pd.DataFrame):
                    if val.empty:
                        return NOT_EXIST, pd.DataFrame(columns=self.metadata['columns'])
                    if self.metadata:
                        val = val.reindex(columns=self.metadata['columns'])
                    return pickle.dumps(val.values), val
                else:
                    return None, pd.DataFrame(columns=self.metadata['columns'])
            elif vtype == KVTYPE.TPV_NARR_2D:
                if isinstance(val, pd.DataFrame):
                    if val.empty:
                        return NOT_EXIST, np.empty((0, len(self.metadata['columns'])))
                    if self.metadata:
                        val = val.reindex(columns=self.metadata['columns'])
                    return pickle.dumps(val.values), val.values
                else:
                    return None, pd.DataFrame(columns=self.metadata['columns'])
        elif self.tpval == KVTYPE.TPV_SERIES:
            if 'columns' in self.metadata.keys():
                if isinstance(val, pd.Series):
                    if val.empty:
                        return NOT_EXIST, pd.Series(index=self.metadata['columns'])
                    if self.metadata:
                        val = val.reindex(index=self.metadata['columns'])
                    return pickle.dumps(val.values), val
            else:
                if isinstance(val, pd.Series):
                    if val.empty:
                        return NOT_EXIST, pd.Series()
                    return pickle.dumps(val.values), val
        # elif self.tpval == KVTYPE.TPV_NARR_2D:
        #     if isinstance(val, np.ndarray):
        #         if val.size == 0:
        #             return NOT_EXIST, val
        #         return pickle.dumps(val), val
        else:
            return force_bytes(val), val

        return None, None

    def to_val_out(self, val, vtype):
        """

        :param val:
        :param vtype:
        :return:
        """
        realval = None
        if self.tpval == KVTYPE.TPV_DFRAME:
            cols = self.metadata['columns']
            if vtype is None:
                if val == NOT_EXIST:
                    realval = pd.DataFrame(columns=cols)
                else:
                    val = pickle.loads(val)
                    realval = pd.DataFrame(data=val, columns=cols)
            elif vtype == KVTYPE.TPV_NARR_2D:
                if val == NOT_EXIST:
                    realval = np.empty((0, len(cols)))
                else:
                    val = pickle.loads(val)
                    realval = val
        elif self.tpval == KVTYPE.TPV_SERIES:
            if 'columns' in self.metadata.keys():
                cols = self.metadata['columns']
                if val == NOT_EXIST:
                    realval = pd.Series(index=cols)
                else:
                    val = pickle.loads(val)
                    realval = pd.Series(data=val, index=cols)
            else:
                if val == NOT_EXIST:
                    realval = pd.Series()
                else:
                    val = pickle.loads(val)
                    realval = pd.Series(data=val)
        # elif self.tpval == KVTYPE.TPV_NARR_2D:
        #     cols = self.metadata['columns']
        #     if val == NOT_EXIST:
        #         realval = np.empty((0, len(cols)))
        #     else:
        #         val = pickle.loads(val)
        #         realval = val
        else:
            realval = val

        return realval

    @abstractmethod
    def load(self, key, vtype=None):
        """

        :param key:
        :param vtype: override value type for output.
        :return:
        """

    @abstractmethod
    def save(self, key, val, vtype=None):
        """"""

    @abstractmethod
    def remove(self, key):
        """"""

    @abstractmethod
    def commit(self):
        """"""

    @abstractmethod
    def load_range(self, kstart, kend, vtype):
        """"""
