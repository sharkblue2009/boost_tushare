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


if __name__ == '__main__':
    import logbook, sys
    import timeit

    zipline_logging = logbook.NestedSetup([
        logbook.NullHandler(),
        logbook.StreamHandler(sys.stdout, level=logbook.INFO),
        logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
    ])
    zipline_logging.push_application()

    reader = TusReader()

    # df = reader.get_index_info()
    # df = reader.get_stock_info()
    # df = reader.get_fund_info()
    # df = reader.trade_cal
    # df = reader.get_index_weight('399300.XSHE', '20200318', refresh=False)
    # df = reader.get_stock_xdxr('002465.XSHE', refresh=True)
    # df = reader.get_stock_xdxr('000002.XSHE', refresh=False)
    #
    # df = reader.get_price_minute('000001.XSHE', '20150117', '20150227', refresh=0)
    # df = reader.get_price_minute('002465.XSHE', '20150117', '20150227', refresh=0)
    # df_day = reader.get_price_daily('002465.XSHE', '20150201', '20200207', refresh=1)
    # df = reader.get_stock_adjfactor('002465.XSHE', '20150201', '20200207', refresh=1)
    # # df = df.reindex(df_day.index)
    # # print(df_day['close']*df['adj_factor']/df['adj_factor'][-1])
    #
    # df = reader.get_stock_suspend('000002.XSHE', refresh=False)
    # df = reader.get_stock_daily_info('002465.XSHE', '20150201', '20200207', refresh=1)

    df = reader.get_stock_suspend_d('000155.XSHE', refresh=False)
    print(df)

    # print(timeit.Timer(lambda: reader.get_stock_xdxr('002465.XSHE', refresh=True)).timeit(1))
    # print(timeit.Timer(lambda: reader.get_index_weight('399300.XSHE', '20200318', refresh=True)).timeit(1))
    # print(timeit.Timer(lambda: reader.get_stock_xdxr('002465.XSHE', refresh=False)).timeit(1))
    # print(timeit.Timer(lambda: reader.get_index_weight('399300.XSHE', '20200318', refresh=False)).timeit(1))

    # print(timeit.Timer(lambda: reader.get_price_daily('002465.XSHE', '20190101', '20200303', refresh=2)).timeit(3))
    # print(timeit.Timer(lambda: reader.get_price_daily('002465.XSHE', '20190101', '20200303', refresh=0)).timeit(3))

    df = reader.get_income('002465.XSHE', '20150630', refresh=True)

    print(df)
    df = reader.get_balancesheet('002465.XSHE', '20150630')
    print(df)
    df = reader.get_cashflow('002465.XSHE', '20150630')
    print(df)
    df = reader.get_fina_indicator('002465.XSHE', '20150630')
    print(df)
    df = reader.get_index_classify('L1')
    print(df)
    # print(timeit.Timer(lambda: reader.get_income('002465.XSHE', '20150630', refresh=True)).timeit(1))
    # print(timeit.Timer(lambda: reader.get_income('002465.XSHE', '20150630')).timeit(1))
