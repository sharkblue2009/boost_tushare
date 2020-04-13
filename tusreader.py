# from trading_calendars import get_calendar

import tushare as ts
from cntus._passwd import TUS_TOKEN
import pandas as pd

from cntus.xcachedb import *
from cntus.dbschema import *
from cntus.utils.xctus_utils import *
from cntus.tusbasic import TusBasicInfo
from cntus.tusfinance import TusFinanceInfo
from cntus.tusprice import TusPriceInfo
from cntus.utils.qos import ThreadingTokenBucket


class TusReader(TusBasicInfo, TusFinanceInfo, TusPriceInfo):

    def __init__(self, tus_last_date=None):
        """
        :param tus_last_date: Tushare last date with data available,
                            we assume yesterday's data is available in today.
        """
        # self.calendar = get_calendar('XSHG')
        ts.set_token(TUS_TOKEN)
        self.pro_api = ts.pro_api()
        self.master_db = XCacheDB()

        # 每分钟不超过500次，每秒8次，同时api调用不超过300个。
        self.ts_token = ThreadingTokenBucket(8, 300)

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
    print('total stocks: {}, {}-{}'.format(len(df_stock), start_date, end_date))
    for k, stk in df_stock['ts_code'].items():
        # log.info('-->{}'.format(stk))
        reader.get_stock_adjfactor(stk, start_date, end_date)
        # reader.get_stock_xdxr(stk, refresh=True)
        reader.get_price_daily(stk, start_date, end_date)


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


if __name__ == '__main__':
    import logbook, sys
    import timeit

    zipline_logging = logbook.NestedSetup([
        logbook.NullHandler(),
        logbook.StreamHandler(sys.stdout, level=logbook.INFO),
        logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
    ])
    zipline_logging.push_application()

    # print(timeit.Timer(lambda: get_all_price_day()).timeit(1))

    # print(timeit.Timer(lambda: get_all_dayinfo()).timeit(1))
    #
    # print(timeit.Timer(lambda: get_all_price_min()).timeit(1))

    reader = TusReader()
    tst_get_min(reader)

    # df = reader.get_index_info()
    # df = reader.get_stock_info()
    # df = reader.get_fund_info()
    # df = reader.trade_cal
    # df = reader.get_index_weight('399300.XSHE', '20200318', refresh=False)
    # df = reader.get_stock_xdxr('002465.XSHE', refresh=True)
    # df = reader.get_stock_xdxr('000002.XSHE', refresh=False)

    # stocks = ['000001.XSHE']
    # for stk in stocks:
    #     df = reader.get_price_minute('000001.XSHE', '20190117', '20200227', refresh=True)
    #     print(df[-10:])

    # df = reader.get_price_minute('002465.XSHE', '20150117', '20150227', refresh=0)
    # df_day = reader.get_price_daily('002465.XSHE', '20150201', '20200207', refresh=1)
    # df = reader.get_stock_adjfactor('002465.XSHE', '20150201', '20200207', refresh=1)
    # # df = df.reindex(df_day.index)
    # # print(df_day['close']*df['adj_factor']/df['adj_factor'][-1])
    #
    # df = reader.get_stock_suspend('000002.XSHE', refresh=False)
    # df = reader.get_stock_daily_info('002465.XSHE', '20150201', '20200207', refresh=1)

    # df = reader.get_stock_suspend_d('000155.XSHE', refresh=False)
    # print(df)

    # print(timeit.Timer(lambda: reader.get_stock_xdxr('002465.XSHE', refresh=True)).timeit(1))
    # print(timeit.Timer(lambda: reader.get_index_weight('399300.XSHE', '20200318', refresh=True)).timeit(1))
    # print(timeit.Timer(lambda: reader.get_stock_xdxr('002465.XSHE', refresh=False)).timeit(1))
    # print(timeit.Timer(lambda: reader.get_index_weight('399300.XSHE', '20200318', refresh=False)).timeit(1))

    # print(timeit.Timer(lambda: reader.get_price_daily('002465.XSHE', '20190101', '20200303', refresh=2)).timeit(3))
    # print(timeit.Timer(lambda: reader.get_price_daily('002465.XSHE', '20190101', '20200303', refresh=0)).timeit(3))

    # df = reader.get_income('002465.XSHE', '20150630', refresh=True)
    #
    # print(df)
    # df = reader.get_balancesheet('002465.XSHE', '20150630')
    # print(df)
    # df = reader.get_cashflow('002465.XSHE', '20150630')
    # print(df)
    # df = reader.get_fina_indicator('002465.XSHE', '20150630')
    # print(df)
    # df = reader.get_index_classify('L1')
    # print(df)
    # print(timeit.Timer(lambda: reader.get_income('002465.XSHE', '20150630', refresh=True)).timeit(1))
    # print(timeit.Timer(lambda: reader.get_income('002465.XSHE', '20150630')).timeit(1))
