from cntus.tusreader import TusReader
import pandas as pd
import tushare as ts
from cntus._passwd import TUS_TOKEN
from cntus.xcachedb import *
from cntus.dbschema import *
from cntus.utils.xctus_utils import *
import numpy as np


class TusUpdater(TusReader):

    def update_price_daily(self, code, start, end):
        """

        :param code:
        :param start:
        :param end:
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_DAILY_PRICE.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, EQUITY_DAILY_PRICE_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = self.gen_keys_monthly(tstart, tend, list_date, delist_date)

        bvalid = np.ndarray((len(vdates),), dtype=np.bool)

        for n, dd in enumerate(vdates):
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey)
            if val is not None:
                # price_data integrity check.
                if self.integrity_check_monthly(code, dd, val, astype):
                    bvalid[n] = True
                    continue
                else:
                    bvalid[n] = False
                    continue

            bvalid[n] = False

        max_units = 12
        nd_s = None
        nd_e = None
        cnt = 0
        for n, dd in enumerate(vdates):
            b_fetch = False
            if bvalid[n]:
                if nd_e is not None:
                    tstart = vdates[nd_s]
                    tend = vdates[nd_e]
                    b_fetch = True
                    nd_e = None
                    nd_s = None
                cnt = 0
            else:
                if nd_s is None:
                    nd_s = n
                nd_e = n
                cnt += 1
                if cnt > max_units or n == (len(vdates) - 1):
                    b_fetch = True
                    tstart = nd_s
                    tend = nd_e
                    nd_e = None
                    nd_s = None
                    cnt = 0

            if b_fetch:
                start_raw = vdates[tstart].strftime(DATE_FORMAT)
                tt1 = vdates[tend]
                end_raw = pd.Timestamp(year=tt1.year, month=tt1.month, day=tt1.days_in_month).strftime(DATE_FORMAT)
                data = ts.pro_bar(tscode, asset=astype, start_date=start_raw, end_date=end_raw, freq='D')
                if data is not None:
                    data = data.rename(columns={'vol': 'volume'})
                xxdates = vdates[tstart: tend + 1]
                for xx in xxdates:
                    dtkey = xx.strftime(DATE_FORMAT)
                    xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                    db.save(dtkey, xxd)

        return

    def update_price_minute(self, code, start, end):
        """

        :param code:
        :param start:
        :param end:
        :return:
        """

    def update_adjfactor(self, code, start, end):
        """

        :param code:
        :param start:
        :param end:
        :return:
        """


if __name__ == '__main__':
    import logbook, sys
    import timeit

    log = logbook.Logger('upd')
    start_date = '20150101'
    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    zipline_logging = logbook.NestedSetup([
        logbook.NullHandler(),
        logbook.StreamHandler(sys.stdout, level=logbook.INFO),
        logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
    ])
    zipline_logging.push_application()

    reader = TusUpdater()

    df = reader.get_trade_cal(refresh=True)

    df_index = reader.get_index_info(refresh=True)
    df_stock = reader.get_stock_info(refresh=True)
    df_fund = reader.get_fund_info(refresh=True)

    for k, stk in df_stock['ts_code'].items():
    # for stk in ['000002.XSHE']:
        log.info('--_>{}'.format(stk))
        df = reader.get_stock_suspend(stk, refresh=True)
        log.info('xdxr')
        df = reader.get_stock_xdxr(stk, refresh=True)
        log.info('daily')
        df_day = reader.update_price_daily(stk, start_date, end_date)
        # log.info('min')
        # df_min = reader.get_price_minute(stk, start_date, end_date)
        # log.info('adj')
        # df_adj = reader.get_stock_adjfactor(stk, start_date, end_date)
        # log.info('dayinfo')
        # df_daily = reader.get_stock_daily_info(stk, start_date, end_date, )
