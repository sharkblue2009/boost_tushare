"""
CN Tushare Cache Reader.
"""

from cntus.xcdb.zleveldb import *
from cntus.xcdb.zlmdb import *
from .rdbasic import XcReaderBasic
from .rdfinance import XcReaderFinance
from .rdprice import XcReaderPrice
from .updater import XcUpdaterPrice
from .utils.parallelize import parallelize
from .utils.memoize import lazyval
from .proloader import get_netloader, TusNetLoader


class XcTusReader(XcReaderBasic, XcReaderFinance, XcReaderPrice, XcUpdaterPrice):
    """
    Cache Reader for tushare data.
    """

    @lazyval
    def netloader(self) -> TusNetLoader:
        return get_netloader()

    def __init__(self, xctus_last_date=None):
        """
        :param xctus_last_date: Tushare last date with data available,
                            we assume yesterday's data is available in today.
        """
        self.master_db = XcLMDB(LMDB_NAME, readonly=False)
        self.acc = XcLMDBAccessor
        self.facc = partial(XcLMDBAccessor, self.master_db)

        # , write_buffer_size = 0x400000, block_size = 0x4000,
        # max_file_size = 0x1000000, lru_cache_size = 0x100000, bloom_filter_bits = 0

        if xctus_last_date is None:
            self.xctus_last_date = pd.Timestamp.today().normalize() #- pd.Timedelta(days=1)
        else:
            self.xctus_last_date = xctus_last_date

        print('Last date:{}'.format(self.xctus_last_date))

        super(XcTusReader, self).__init__()

    def __del__(self):
        self.master_db.close()


greader: XcTusReader = None


def get_tusreader() -> XcTusReader:
    global greader
    if greader is None:
        greader = XcTusReader()
    return greader


###############################################################################
import math, sys


def progress_bar(cur, total):
    percent = '{:.0%}'.format(cur / total)
    sys.stdout.write('\r')
    sys.stdout.write("[%-50s] %s" % ('=' * int(math.floor(cur * 50 / total)), percent))
    sys.stdout.flush()


def cntus_update_basic():
    """

    :param b_stock_day:
    :param b_stock_min:
    :param b_index_day:
    :param b_index_min:
    :return:
    """
    reader = get_tusreader()
    log.info('Download basic information...(Trading Calendar, Asset info)')
    reader.get_trade_cal(IOFLAG.READ_NETDB)
    reader.get_index_info(IOFLAG.READ_NETDB)
    reader.get_stock_info(IOFLAG.READ_NETDB)
    reader.get_fund_info(IOFLAG.READ_NETDB)
    reader.get_index_classify(level='L1', flag=IOFLAG.READ_NETDB)
    reader.get_index_classify(level='L2', flag=IOFLAG.READ_NETDB)
    return


def cntus_update_stock_day(start_date='20150101'):
    reader = get_tusreader()
    df_stock = reader.get_stock_info()

    def _fetch_day(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = reader.update_price_daily(stk, t_start, t_end, astype)

        return results

    def _fetch_stock_ext(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']

            reader.get_stock_xdxr(stk, IOFLAG.READ_NETDB)
            reader.update_stock_adjfactor(stk, t_start, t_end)
            results[stk] = reader.update_stock_dayinfo(stk, t_start, t_end)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    log.info('Downloading stocks suspend data: {}, {}-{}'.format(len(df_stock), start_date, end_date))
    reader.update_suspend_d(start_date, end_date)

    #dummy read suspend_info
    suspend_info = reader.get_suspend_d(start_date, end_date)

    all_symbols = []
    for k, stk in df_stock['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'E'})

    all_symbols = list(reversed(all_symbols))
    log.info('Downloading stocks data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

    batch_size = 60
    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))
        symbol_batch = all_symbols[idx:idx + batch_size]

        # result = _fetch_day(symbol_batch)
        result = parallelize(_fetch_day, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))

    log.info('Downloading stocks extension data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))
        symbol_batch = all_symbols[idx:idx + batch_size]
        parallelize(_fetch_stock_ext, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))


def cntus_update_index_day(start_date):
    reader = get_tusreader()
    df_index = reader.get_index_info()

    def _fetch_day(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            if astype == 'E':
                reader.update_suspend(stk, t_start, t_end)
            results[stk] = reader.update_price_daily(stk, t_start, t_end, astype)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    log.info('Downloading index daily quotations: {}, {}-{}'.format(len(df_index), start_date, end_date))

    all_symbols = []
    for k, stk in df_index['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'I'})

    batch_size = 60
    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))
        symbol_batch = all_symbols[idx:idx + batch_size]
        result = parallelize(_fetch_day, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))


def cntus_update_stock_min(start_date='20190101'):
    reader = get_tusreader()
    df_stock = reader.get_stock_info()

    def _fetch_min(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = reader.update_price_minute(stk, t_start, t_end, freq='1min', astype=astype,
                                                      flag=IOFLAG.UPDATE_MISS)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    log.info('Downloading stocks minute price: {}, {}-{}'.format(len(df_stock), start_date, end_date))

    all_symbols = []
    for k, stk in df_stock['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'E'})

    # all_symbols = all_symbols[::-1]

    batch_size = 60
    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))

        symbol_batch = all_symbols[idx:idx + batch_size]

        result = parallelize(_fetch_min, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
        # _fetch_min(symbol_batch)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))


def cntus_update_index_min(start_date='20190101'):
    reader = get_tusreader()
    df_index = reader.get_index_info()

    def _fetch_min(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = reader.update_price_minute(stk, t_start, t_end, freq='1min', astype=astype,
                                                      flag=IOFLAG.UPDATE_MISS)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    log.info('Downloading stocks minute quotation: {}, {}-{}'.format(len(df_index), start_date, end_date))

    all_symbols = []
    for k, stk in df_index['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'I'})

    batch_size = 60
    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))

        symbol_batch = all_symbols[idx:idx + batch_size]

        result = parallelize(_fetch_min, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))
    return


