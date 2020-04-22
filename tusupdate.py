from .tusreader import TusReader
import tushare as ts
from .xcachedb import *
from .dbschema import *
from .utils.xctus_utils import *
import numpy as np
import pandas as pd
from .utils.parallelize import parallelize
from collections import OrderedDict
import logbook, sys
import math

log = logbook.Logger('upd')


def nadata_iter(ar_flags, max_length):
    """
    生成器，查找连续的False序列，返回位置 start, end
    :param ar_flags:
    :param max_length:
    :return:
    """
    pps = None
    ppe = None
    cnt = 0
    for n, dd in enumerate(ar_flags):
        if dd:
            if ppe is not None:
                tstart = pps
                tend = ppe
                yield tstart, tend
                # print('[1]', tstart, tend)
                ppe = None
                pps = None
            cnt = 0
        else:
            if pps is None:
                pps = n
            ppe = n
            cnt += 1
            if cnt > max_length or n == (len(ar_flags) - 1):
                tstart = pps
                tend = ppe
                yield tstart, tend
                # print('[2]', tstart, tend)
                ppe = None
                pps = None
                cnt = 0
    yield None, None


class TusUpdater(TusReader):

    def _integrity_check_daily_data(self, code, m_start, val, astype='E'):
        """
        日度数据完整性检查
        :param code:
        :param m_start: Timestamp, Month start
        :param val: daily data, with column trade_date
        :return:
        """
        if astype != 'E':
            return True

        m_end = pd.Timestamp(year=m_start.year, month=m_start.month, day=m_start.days_in_month)
        m_end = min(self.tus_last_date, m_end)

        # try:
        suspend = self.get_stock_suspend_d(code, m_start.strftime(DATE_FORMAT),
                                           m_end.strftime(DATE_FORMAT), refresh=False)
        suspend_v = suspend.loc[(suspend['suspend_type'] == 'S') & (suspend['suspend_timing'].isna()), :]
        b_suspend = False
        if not suspend_v.empty:
            b_suspend = True
            sus_dates = suspend_v.index
        else:
            sus_dates = pd.DatetimeIndex([], freq='D')
        # except Exception as e:
        #     # create empty index
        #     sus_dates = pd.DatetimeIndex([], freq='D')

        trd_dates = self.trade_cal_index
        days_tcal = (trd_dates[(trd_dates >= m_start) & (trd_dates <= m_end)])
        days_susp = (sus_dates[(sus_dates >= m_start) & (sus_dates <= m_end)])

        if len(days_tcal) <= len(val) + len(days_susp):
            # 股票存在停牌半天的情况，也会被计入suspend列表
            return True

        if b_suspend:
            log.info(
                '[Day]-incomplete: {}-{}:: {}-{}-{} '.format(code, m_start, len(days_tcal), len(days_susp), len(val)))
            # log.info('{}'.format(val['trade_date']))

        return False

    def _integrity_check_minute_data(self, code, d_start, val, freq, astype='E'):
        """"""
        if astype != 'E':
            return True

        cc = {'1min': 241, '5min': 49, '15min': 17, '30min': 9, '60min': 5, '120min': 3}
        nbars = cc[freq]
        d_end = d_start
        d_end = min(self.tus_last_date, d_end)

        suspend = self.get_stock_suspend_d(code, d_start.strftime(DATE_FORMAT),
                                           d_end.strftime(DATE_FORMAT), refresh=False)
        suspend_v = suspend.loc[(suspend['suspend_type'] == 'S') & (suspend.index == d_start), :]
        b_suspend = False
        if not suspend_v.empty:
            b_suspend = True
            if suspend_v['suspend_timing'].isna().iloc[-1]:
                # 当日全天停牌
                if len(val) == nbars or len(val) == 0:  #
                    return True
            else:
                # 部分时间停牌
                if len(val) == nbars:
                    return True
        else:
            if len(val) == nbars:
                return True

        if b_suspend:
            log.info('[Min]-incomplete: {}-{}-{}: {}'.format(code, d_start, len(val), b_suspend))
        return False

    def price_daily_update(self, code, start, end, mode=0):
        """

        :param code:
        :param start:
        :param end:
        :param mode: -1: erase, 0: update from last valid, 1: update all
        :return:
        """
        db = XcAccessor(self.master_db, TusSdbs.SDB_DAILY_PRICE.value + code,
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, EQUITY_DAILY_PRICE_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = self.gen_keys_monthly(tstart, tend, list_date, delist_date)

        if mode == -1:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                db.remove(dtkey)
            return

        bvalid = np.full((len(vdates),), True, dtype=np.bool)

        for n in range(len(vdates) - 1, -1, -1):
            # reverse order.
            dd = vdates[n]
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey)
            if val is not None:
                # price_data integrity check.
                if self._integrity_check_daily_data(code, dd, val, astype):
                    bvalid[n] = True
                    if mode == 0:
                        # log.info('{} last:{}'.format(code, dtkey))
                        break
                    continue
                else:
                    bvalid[n] = False
                    continue

            bvalid[n] = False

        need_update = nadata_iter(bvalid, 50)
        while True:
            tstart, tend = next(need_update)
            if tstart is None:
                break
            start_raw = vdates[tstart].strftime(DATE_FORMAT)
            tt1 = vdates[tend]
            end_raw = pd.Timestamp(year=tt1.year, month=tt1.month, day=tt1.days_in_month).strftime(DATE_FORMAT)
            self.ts_token.block_consume(1)
            data = ts.pro_bar(tscode, asset=astype, start_date=start_raw, end_date=end_raw, freq='D')
            if data is not None:
                data = data.rename(columns={'vol': 'volume'})
            dts_upd = vdates[tstart: tend + 1]
            for xx in dts_upd:
                dtkey = xx.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                db.save(dtkey, xxd)

        return

    def price_minute_update(self, code, start, end, freq='1min', mode=0):
        """

        :param code:
        :param start:
        :param end:
        :param freq:
        :param mode: -1: erase, 0: update from last valid, 1: update all
        :return:
        """
        if freq not in ['1min', '5min', '15min', '30min', '60min', '120m']:
            return None

        db = XcAccessor(self.master_db, (TusSdbs.SDB_MINUTE_PRICE.value + code + freq),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, EQUITY_MINUTE_PRICE_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = self.gen_keys_daily(tstart, tend, list_date, delist_date)

        if mode == -1:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                db.remove(dtkey)
            return

        bvalid = np.full((len(vdates),), True, dtype=np.bool)

        for n in range(len(vdates) - 1, -1, -1):
            dd = vdates[n]
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey)
            if val is not None:
                # price_data integrity check.
                if self._integrity_check_minute_data(code, dd, val, freq, astype):
                    bvalid[n] = True
                    if mode == 0:
                        # log.info('{} last:{}'.format(code, dtkey))
                        break
                    continue
                else:
                    bvalid[n] = False
                    continue

            bvalid[n] = False

        # from ratelimit import limits, sleep_and_retry
        # @sleep_and_retry
        # @limits(30, period=120)
        # def _fetch(tscode, astype, start_raw, end_raw):
        #     return ts.pro_bar(tscode, asset=astype, start_date=start_raw, end_date=end_raw, freq='1min')

        need_update = nadata_iter(bvalid, 12)
        while True:
            tstart, tend = next(need_update)
            if tstart is None:
                break
            start_raw = vdates[tstart].strftime(DATETIME_FORMAT)
            tt1 = vdates[tend]
            end_raw = (tt1 + pd.Timedelta(hours=17)).strftime(DATETIME_FORMAT)
            self.ts_token.block_consume(10)
            data = ts.pro_bar(tscode, asset=astype, start_date=start_raw, end_date=end_raw, freq=freq)
            # data = _fetch(tscode, astype, start_raw, end_raw)
            if data is not None:
                data = data.rename(columns={'vol': 'volume'})
                # convert %Y-%m-%d %H:%M:%S to %Y%m%d %H:%M:%S
                data['trade_time'] = data['trade_time'].apply(lambda x: x.replace('-', ''))
            dts_upd = vdates[tstart: tend + 1]
            for xx in dts_upd:
                dtkey = xx.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_time'].map(lambda x: x[:8] == dtkey[:8]), :]
                db.save(dtkey, xxd)

        return

    def stock_adjfactor_update(self, code, start, end, mode=0):
        """

        :param code:
        :param start:
        :param end:
        :return:
        """
        db = XcAccessor(self.master_db, (TusSdbs.SDB_STOCK_ADJFACTOR.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_ADJFACTOR_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = self.gen_keys_monthly(tstart, tend, list_date, delist_date)

        if mode == -1:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                db.remove(dtkey)
            return

        bvalid = np.full((len(vdates),), True, dtype=np.bool)
        fcols = STOCK_ADJFACTOR_META['columns']

        for n in range(len(vdates) - 1, -1, -1):
            dd = vdates[n]
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey)
            if val is not None:
                # price_data integrity check.
                if self._integrity_check_daily_data(code, dd, val, astype):
                    bvalid[n] = True
                    if mode == 0:
                        break
                    continue
                else:
                    bvalid[n] = False
                    continue

            bvalid[n] = False

        need_update = nadata_iter(bvalid, 50)
        while True:
            tstart, tend = next(need_update)
            if tstart is None:
                break
            # print(tstart, tend)

            start_raw = vdates[tstart].strftime(DATE_FORMAT)
            tt1 = vdates[tend]
            end_raw = pd.Timestamp(year=tt1.year, month=tt1.month, day=tt1.days_in_month).strftime(DATE_FORMAT)
            self.ts_token.block_consume(1)
            data = self.pro_api.adj_factor(ts_code=tscode, start_date=start_raw, end_date=end_raw, fields=fcols)
            dts_upd = vdates[tstart: tend + 1]
            for xx in dts_upd:
                dtkey = xx.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                db.save(dtkey, xxd)

        return

    def stock_dayinfo_update(self, code, start, end, mode=0):
        """

        :param code:
        :param start:
        :param end:
        :return:
        """
        db = XcAccessor(self.master_db, (TusSdbs.SDB_STOCK_DAILY_INFO.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_DAILY_INFO_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = self.gen_keys_monthly(tstart, tend, list_date, delist_date)

        if mode == -1:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                db.remove(dtkey)
            return

        bvalid = np.full((len(vdates),), True, dtype=np.bool)
        fcols = STOCK_DAILY_INFO_META['columns']

        for n in range(len(vdates) - 1, -1, -1):
            dd = vdates[n]
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey)
            if val is not None:
                # price_data integrity check.
                if self._integrity_check_daily_data(code, dd, val, astype):
                    bvalid[n] = True
                    if mode == 0:
                        break
                    continue
                else:
                    bvalid[n] = False
                    continue

            bvalid[n] = False

        need_update = nadata_iter(bvalid, 50)
        while True:
            tstart, tend = next(need_update)
            if tstart is None:
                break
            # print(tstart, tend)

            start_raw = vdates[tstart].strftime(DATE_FORMAT)
            tt1 = vdates[tend]
            end_raw = pd.Timestamp(year=tt1.year, month=tt1.month, day=tt1.days_in_month).strftime(DATE_FORMAT)
            self.ts_token.block_consume(1)
            data = self.pro_api.daily_basic(ts_code=tscode, start_date=start_raw, end_date=end_raw,
                                            fields=fcols)
            dts_upd = vdates[tstart: tend + 1]
            for xx in dts_upd:
                dtkey = xx.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                db.save(dtkey, xxd)
        return


def progress_bar(cur, total):
    percent = '{:.0%}'.format(cur / total)
    sys.stdout.write('\r')
    sys.stdout.write("[%-50s] %s" % ('=' * int(math.floor(cur * 50 / total)), percent))
    sys.stdout.flush()


def tus_update_all(b_stock_day, b_stock_min, b_index_day, b_index_min):
    """

    :param b_stock_day:
    :param b_stock_min:
    :param b_index_day:
    :param b_index_min:
    :return:
    """
    reader = TusUpdater()
    reader.get_trade_cal(refresh=True)

    df_index = reader.get_index_info(refresh=True)
    df_stock = reader.get_stock_info(refresh=True)
    df_fund = reader.get_fund_info(refresh=True)

    def _fetch_day(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            reader.get_stock_suspend_d(stk, t_start, t_end)
            reader.price_daily_update(stk, t_start, t_end)

        return results

    def _fetch_stock_ext(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']

            reader.get_stock_xdxr(stk, refresh=True)
            reader.stock_adjfactor_update(stk, t_start, t_end)
            reader.stock_dayinfo_update(stk, t_start, t_end)

            # results[stk] = bars

        return results

    if b_stock_day:
        start_date = '20150101'
        end_date = pd.Timestamp.today().strftime('%Y%m%d')

        all_symbols = []
        for k, stk in df_stock['ts_code'].items():
            all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date})

        all_symbols = all_symbols[::-1]
        log.info('Downloading stocks data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

        batch_size = 60
        for idx in range(0, len(all_symbols), batch_size):
            progress_bar(idx, len(all_symbols))
            symbol_batch = all_symbols[idx:idx + batch_size]
            parallelize(_fetch_day, workers=20, splitlen=3)(symbol_batch)

        log.info('Downloading stocks extension data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

        for idx in range(0, len(all_symbols), batch_size):
            progress_bar(idx, len(all_symbols))
            symbol_batch = all_symbols[idx:idx + batch_size]
            parallelize(_fetch_stock_ext, workers=20, splitlen=3)(symbol_batch)

    if b_index_day:
        start_date = '20150101'
        end_date = pd.Timestamp.today().strftime('%Y%m%d')

        log.info('Downloading index data: {}, {}-{}'.format(len(df_index), start_date, end_date))

        all_symbols = []
        for k, stk in df_index['ts_code'].items():
            all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date})

        # all_symbols = all_symbols[::-1]

        batch_size = 60
        for idx in range(0, len(all_symbols), batch_size):
            progress_bar(idx, len(all_symbols))
            symbol_batch = all_symbols[idx:idx + batch_size]
            parallelize(_fetch_day, workers=20, splitlen=3)(symbol_batch)

    def _fetch_min(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            reader.price_minute_update(stk, t_start, t_end, freq=DEFAULT_MINUTE_PRICE_FREQ, mode=0)

        return results

    if b_stock_min:
        start_date = '20190101'
        end_date = pd.Timestamp.today().strftime('%Y%m%d')

        log.info('Downloading stocks minute data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

        all_symbols = []
        for k, stk in df_stock['ts_code'].items():
            all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date})

        # all_symbols = all_symbols[::-1]

        batch_size = 60
        for idx in range(0, len(all_symbols), batch_size):
            progress_bar(idx, len(all_symbols))

            symbol_batch = all_symbols[idx:idx + batch_size]

            parallelize(_fetch_min, workers=20, splitlen=3)(symbol_batch)
            # _fetch_min(symbol_batch)

    if b_index_min:
        start_date = '20190101'
        end_date = pd.Timestamp.today().strftime('%Y%m%d')

        log.info('Downloading stocks minute data: {}, {}-{}'.format(len(df_index), start_date, end_date))

        all_symbols = []
        for k, stk in df_index['ts_code'].items():
            all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date})

        # all_symbols = all_symbols[::-1]

        batch_size = 60
        for idx in range(0, len(all_symbols), batch_size):
            progress_bar(idx, len(all_symbols))

            symbol_batch = all_symbols[idx:idx + batch_size]

            parallelize(_fetch_min, workers=20, splitlen=3)(symbol_batch)

    return
