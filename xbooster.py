"""
CN Tushare Cache Reader.
"""
from .apiwrapper import set_algo_instance
from .rdbasic import XcReaderBasic
from .rdfinance import XcReaderFinance
from .rdprice import XcReaderPrice
from .utils.xcutils import *
from .utils.memoize import lazyval
from .proloader import netloader_init, TusNetLoader
# from .xcdb.zleveldb import *
from .xcdb.zlmdb import *
from functools import partial
from .domain import XcDomain
from .xcdb.xcdb import *
from .layout import *


class XcTusBooster(XcReaderBasic, XcReaderFinance, XcReaderPrice):
    """
    Cache Reader for tushare data.
    """

    @lazyval
    def netloader(self) -> TusNetLoader:
        return netloader_init()

    def __init__(self, last_day=None, dbtype=DBTYPE.DB_LMDB):
        """
        :param last_day: Tushare last date with data available,
                            we assume yesterday's data is available in today.
        """
        if dbtype == DBTYPE.DB_LMDB:
            self.master_db = XcLMDB(LMDB_NAME, readonly=False)
            self.acc = XcLMDBAccessor
            self.facc = partial(XcLMDBAccessor, self.master_db)

            # , write_buffer_size = 0x400000, block_size = 0x4000,
            # max_file_size = 0x1000000, lru_cache_size = 0x100000, bloom_filter_bits = 0
        else:
            # self.master_db = XcLevelDB(LEVELDB_NAME, readonly=False)
            # self.acc = XcLevelDBAccessor
            # self.facc = partial(XcLevelDBAccessor, self.master_db)
            pass

        if last_day is None:
            """
            Last date always point to the end of Today. but tushare data may not exist at this time.
            """
            self.xctus_last_day = pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
        else:
            self.xctus_last_day = last_day

        self.xctus_first_day = pd.Timestamp('20080101')

        print('TuBooster: date range:{}>>>{}'.format(self.xctus_first_day, self.xctus_last_day))
        # self.domain = XcDomain()

        super(XcTusBooster, self).__init__()

    def __del__(self):
        self.master_db.close()


###############################################################################
g_booster: XcTusBooster = None


def tusbooster_init() -> XcTusBooster:
    global g_booster
    if g_booster is None:
        g_booster = XcTusBooster()
        set_algo_instance(g_booster)
    return g_booster
