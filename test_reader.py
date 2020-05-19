from cntus.xcreader import *
import unittest
from pandas.testing import assert_frame_equal


def check_df_equal(x, y):
    try:
        assert_frame_equal(x, y, check_dtype=False)
    except Exception as e:
        print(e)
        return False
    return True
    # # res1 = (x == y)
    # res = (x.values.ravel() == y.values.ravel())
    #
    # if np.all(res):
    #     return True
    # return False


class TestTusReader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """"""
        cls.reader = get_tusreader()  # TusXcReader()

    @classmethod
    def tearDownClass(cls) -> None:
        """"""
        # del cls.reader

    def test_get_assets_info(self):
        reader = self.reader
        df = reader.get_index_info()
        print('Index info')
        print(df)
        df = reader.get_stock_info()
        print('Stock info')
        print(df.iloc[-10:])
        df = reader.get_fund_info()
        print(df.iloc[-10:])
        df = reader.trade_cal
        print(df.iloc[-10:])

    def test_index_weight(self):
        reader = self.reader
        code = '399300.SZ'
        date = '20171010'
        df1 = reader.get_index_weight(code, date, IOFLAG.READ_NETDB)
        print(df1.iloc[-10:])
        df2 = reader.get_index_weight(code, date, IOFLAG.READ_XC)
        self.assertTrue(check_df_equal(df1, df2))

    def test_index_member(self):
        reader = self.reader
        index_code = '000001.SZ'
        df1 = reader.get_index_member(index_code, IOFLAG.READ_NETDB)
        df2 = reader.get_index_member(index_code, IOFLAG.READ_XC)
        self.assertTrue(check_df_equal(df1, df2))

    def test_index_classify(self):
        reader = self.reader
        df = reader.get_index_classify('L1')
        print(df.iloc[-10:])

    def test_suspend(self):
        reader = self.reader
        start = '20190101'
        end = '20200310'
        stk = '000029.SZ'
        df = reader.get_suspend_d(start, end)
        print(df.loc[pd.IndexSlice[:, stk], :])

        self.assertTrue(not df.empty)

    def test_stock_daily(self):
        reader = self.reader
        stk = '002465.SZ'
        start = '20190101'
        end = '20200310'
        df = reader.get_price_daily(stk, start, end)
        self.assertFalse(df.empty)
        df = reader.get_stock_adjfactor(stk, start, end)
        self.assertFalse(df.empty)

    def test_finance(self):
        reader = self.reader
        stk = '002465.SZ'
        start = '20190101'
        df1 = reader.get_income(stk, start, IOFLAG.READ_NETDB)
        self.assertFalse(df1.empty)
        df2 = reader.get_income(stk, start)
        self.assertTrue(check_df_equal(df1, df2))

        df1 = reader.get_cashflow(stk, start, IOFLAG.READ_NETDB)
        self.assertFalse(df1.empty)
        df2 = reader.get_cashflow(stk, start)
        self.assertTrue(check_df_equal(df1, df2))

        df1 = reader.get_balancesheet(stk, start, IOFLAG.READ_NETDB)
        self.assertFalse(df1.empty)
        df2 = reader.get_balancesheet(stk, start)
        df2 = df2.astype(df1.dtypes)
        self.assertTrue(check_df_equal(df1, df2))

        df1 = reader.get_fina_indicator(stk, start, IOFLAG.READ_NETDB)
        self.assertFalse(df1.empty)
        df2 = reader.get_fina_indicator(stk, start)
        self.assertTrue(check_df_equal(df1, df2))

    # def test_min_data_resample(self):
    #     reader = self.reader
    #     start_date = '20190101'
    #     end_date = '20191231'
    #
    #     stks = ['000002.SZ', '000155.SZ']
    #
    #     for freq in ['5min']:
    #         for stk in stks:
    #             log.info('-->{}'.format(stk))
    #             # df = reader.get_stock_suspend_d(stk, refresh=True)
    #             df_day1 = reader.get_price_minute(stk, start_date, end_date, freq, resample=True)
    #             df_day2 = reader.get_price_minute(stk, start_date, end_date, freq, resample=False)
    #
    #             df_day1.drop(columns='amount', inplace=True)
    #             df_day2.drop(columns='amount', inplace=True)
    #             v_day1 = df_day1.values  # .ravel()
    #             v_day2 = df_day2.values  # .ravel()
    #             res = (v_day1 == v_day2)
    #             ii = np.argwhere(res == False)
    #
    #             vii = [df_day1.index[m] for m, n in ii]
    #
    #             if np.all(res):
    #                 print('right')
    #             else:
    #                 print('wrong')

    def test_benchmark_stock_price(self):
        reader = self.reader
        print(timeit.Timer(lambda: reader.get_price_daily('002465.SZ', '20150101', '20200303')).timeit(10))
        print(timeit.Timer(lambda: reader.get_price_daily('002465.SZ', '20150101', '20200303')).timeit(10))

        print(timeit.Timer(lambda: reader.get_price_minute('002465.SZ', '20190101', '20200303')).timeit(10))

    def test_benchmark_all(self):
        # print(timeit.Timer(lambda: get_all_price_day_parallel()).timeit(1))
        # print(timeit.Timer(lambda: get_all_price_day()).timeit(1))
        #
        # print(timeit.Timer(lambda: get_all_dayinfo()).timeit(1))

        print(timeit.Timer(lambda: get_all_price_min('20190501', '20200301')).timeit(1))


#####################################################################

def get_all_price_day(start_date='20160101', end_date='20200101'):
    reader = get_tusreader()

    df_stock = reader.get_stock_info()[:]
    out = {}
    print('total stocks: {}, {}-{}'.format(len(df_stock), start_date, end_date))
    for k, stk in df_stock['ts_code'].items():
        # log.info('-->{}'.format(stk))
        # reader.get_stock_adjfactor(stk, start_date, end_date)
        # reader.get_stock_xdxr(stk)
        bars1 = reader.get_price_daily(stk, start_date, end_date)
        out[stk] = bars1

    return out


def get_all_price_day_parallel(start_date='20160101', end_date='20200101'):
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


def get_all_dayinfo(start_date='20160101', end_date='20200101'):
    reader = get_tusreader()

    df_stock = reader.get_stock_info()[:]
    print('total stocks: {}, {}-{}'.format(len(df_stock), start_date, end_date))
    for k, stk in df_stock['ts_code'].items():
        # log.info('-->{}'.format(stk))
        reader.get_stock_daily_info(stk, start_date, end_date)


def get_all_price_min(start_date='20190101', end_date='20200101'):
    reader = get_tusreader()

    df_stock = reader.get_stock_info()[:]
    print('get_min, total stocks: {}, {}-{}'.format(len(df_stock), start_date, end_date))
    for k, stk in df_stock['ts_code'].items():
        # log.info('-->{}'.format(stk))
        reader.get_price_minute(stk, start_date, end_date, freq='5min')


if __name__ == '__main__':
    import logbook, sys
    import timeit

    zipline_logging = logbook.NestedSetup([
        logbook.NullHandler(),
        logbook.StreamHandler(sys.stdout, level=logbook.INFO),
        logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
    ])
    zipline_logging.push_application()

    unittest.main(verbosity=2)
