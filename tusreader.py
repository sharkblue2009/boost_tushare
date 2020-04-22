# from trading_calendars import get_calendar

import tushare as ts
from ._passwd import TUS_TOKEN
import pandas as pd

from .xcachedb import *
from .dbschema import *
from .utils.xctus_utils import *
from .tusbasic import TusBasicInfo
from .tusfinance import TusFinanceInfo
from .tusprice import TusPriceInfo
from .utils.qos import ThreadingTokenBucket
from .utils.parallelize import parallelize


class TusReader(TusBasicInfo, TusFinanceInfo, TusPriceInfo):

    def __init__(self, tus_last_date=None):
        """
        :param tus_last_date: Tushare last date with data available,
                            we assume yesterday's data is available in today.
        """
        # self.calendar = get_calendar('XSHG')
        ts.set_token(TUS_TOKEN)
        self.pro_api = ts.pro_api()
        self.master_db = XCacheDB(LEVEL_DB_NAME)
        # , write_buffer_size = 0x400000, block_size = 0x4000,
        # max_file_size = 0x1000000, lru_cache_size = 0x100000, bloom_filter_bits = 0

        # 每分钟不超过500次，每秒8次，同时api调用不超过300个。
        self.ts_token = ThreadingTokenBucket(80, 300)

        if tus_last_date is None:
            self.tus_last_date = pd.Timestamp.today() - pd.Timedelta(days=1)
        else:
            self.tus_last_date = tus_last_date

        super(TusReader, self).__init__()


greader: TusReader = None


def get_tusreader() -> TusReader:
    global greader
    if greader is None:
        greader = TusReader()
    return greader


def get_all_price_day():
    start_date = '20160101'
    end_date = '20200101'

    reader = get_tusreader()

    df_stock = reader.get_stock_info()[:]
    out = {}
    print('total stocks: {}, {}-{}'.format(len(df_stock), start_date, end_date))
    for k, stk in df_stock['ts_code'].items():
        # log.info('-->{}'.format(stk))
        # reader.get_stock_adjfactor(stk, start_date, end_date)
        # reader.get_stock_xdxr(stk, refresh=True)
        bars1 = reader.get_price_daily(stk, start_date, end_date)
        out[stk] = bars1

    return out


def get_all_price_day_parallel():
    start_date = '20160101'
    end_date = '20200101'

    reader = get_tusreader()

    def _fetch(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']

            # adj1 = reader.get_stock_adjfactor(stk, t_start, t_end)
            # reader.get_stock_xdxr(stk, refresh=True)
            bars1 = reader.get_price_daily(stk, t_start, t_end)

            results[stk] = bars1

        return results

    df_stock = reader.get_stock_info()[:]

    all_symbols = []
    for k, stk in df_stock['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date})

    all_symbols = all_symbols[::-1]
    all_out = {}
    print('parallel total stocks: {}, {}-{}'.format(len(df_stock), start_date, end_date))
    batch_size = 200
    for idx in range(0, len(all_symbols), batch_size):
        # progress_bar(idx, len(all_symbols))

        symbol_batch = all_symbols[idx:idx + batch_size]

        results = parallelize(_fetch, workers=20, splitlen=10)(symbol_batch)
        all_out.update(results)

    return all_out


def get_all_dayinfo():
    start_date = '20160101'
    end_date = '20200101'

    reader = get_tusreader()

    df_stock = reader.get_stock_info()[:]
    print('total stocks: {}, {}-{}'.format(len(df_stock), start_date, end_date))
    for k, stk in df_stock['ts_code'].items():
        # log.info('-->{}'.format(stk))
        reader.get_stock_daily_info(stk, start_date, end_date)


def get_all_price_min():
    start_date = '20191001'
    end_date = '20200101'

    reader = get_tusreader()

    df_stock = reader.get_stock_info()[:]
    print('total stocks: {}, {}-{}'.format(len(df_stock), start_date, end_date))
    for k, stk in df_stock['ts_code'].items():
        # log.info('-->{}'.format(stk))
        reader.get_price_minute(stk, start_date, end_date)


def tst_get_min(reader):
    start_date = '20180101'
    end_date = '20191231'

    stks = ['000002.XSHE', '000155.XSHE']

    for freq in ['5min']:
        for stk in stks:
            log.info('-->{}'.format(stk))
            # df = reader.get_stock_suspend_d(stk, refresh=True)
            log.info('min1')

            df_day1 = reader.get_price_minute(stk, start_date, end_date, freq, resample=True)

            log.info('min2')
            df_day2 = reader.get_price_minute(stk, start_date, end_date, freq, resample=False)

            df_day1.drop(columns='amount', inplace=True)
            df_day2.drop(columns='amount', inplace=True)
            v_day1 = df_day1.values  # .ravel()
            v_day2 = df_day2.values  # .ravel()
            res = (v_day1 == v_day2)
            ii = np.argwhere(res == False)

            vii = [df_day1.index[m] for m, n in ii]

            if np.all(res):
                print('right')
            else:
                print('wrong')
