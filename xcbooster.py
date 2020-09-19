"""
CN Tushare Cache Reader.
"""
from .apiwrapper import set_algo_instance
from .rdbasic import XcReaderBasic
from .rdfinance import XcReaderFinance
from .rdprice import XcReaderPrice
from .dxupdater import XcUpdaterPrice
from .dxchecker import XcCheckerPrice
from .utils.memoize import lazyval
from .proloader import netloader_init, TusNetLoader
# from .xcdb.zleveldb import *
from .xcdb.zlmdb import *
from functools import partial


class XcTusBooster(XcReaderBasic, XcReaderFinance, XcReaderPrice, XcUpdaterPrice, XcCheckerPrice):
    """
    Cache Reader for tushare data.
    """

    @lazyval
    def netloader(self) -> TusNetLoader:
        return netloader_init()

    def __init__(self, xctus_current_day=None, xctus_lmdb=True):
        """
        :param xctus_current_day: Tushare last date with data available,
                            we assume yesterday's data is available in today.
        """
        if xctus_lmdb:
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

        if xctus_current_day is None:
            """
            Last date always point to the end of Today. but tushare data may not exist at this time.
            """
            self.xctus_current_day = pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
        else:
            self.xctus_current_day = xctus_current_day

        print('TuBooster: last trade date:{}'.format(self.xctus_current_day))

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

