from trading_calendars import get_calendar

import tushare as ts
from cntus._passwd import TUS_TOKEN
import pandas as pd
from utils.memoize import lazyval

from cntus.xcachedb import *
from cntus.dbschema import *


def symbol_tus_to_std(symbol: str):
    stock, market = symbol.split('.')

    if market == 'SH':
        code = stock + '.XSHG'
    elif market == 'SZ':
        code = stock + '.XSHE'
    else:
        raise ValueError('Symbol error{}'.format(symbol))

    return code


def symbol_std_to_tus(symbol: str):
    """
    convert STD symbol string to Tushare format,
    Tushare symbol format is like 000001.SZ, 000001.SH
    :param symbol:
    :return: tdx code,
    """
    stock, market = symbol.split('.')
    if market == 'XSHG':
        code = stock + '.SH'
    elif market == 'XSHE':
        code = stock + '.SZ'
    else:
        raise ValueError('Symbol error{}'.format(symbol))

    return code


def find_closest_date(all_dates, dt, mode='backward'):
    """

    :param all_dates:
    :param dt:
    :param mode:
    :return:
    """
    tt_all_dates = pd.to_datetime(all_dates, format='%Y%m%d')
    tt_dt = pd.Timestamp(dt)
    if mode == 'backward':
        valid = tt_all_dates[tt_all_dates <= tt_dt]
        if len(valid) > 0:
            return valid[-1].strftime('%Y%m%d')
    else:
        valid = tt_all_dates[tt_all_dates >= tt_dt]
        if len(valid) > 0:
            return valid[0].strftime('%Y%m%d')
    return None


class TusReader(object):

    def __init__(self):
        self.calendar = get_calendar('XSHG')
        ts.set_token(TUS_TOKEN)
        self.pro_api = ts.pro_api()
        self.master_db = XCacheDB()

    @lazyval
    def trade_cal(self):
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_TRADE_CALENDAR.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_SER_COL, None)
        val = db.load('trade_cal')
        if val is not None:
            return val

        log.info('update...')
        info = self.pro_api.trade_cal()
        info_to_db = info[info['is_open'] == 1].loc[:, 'cal_date']
        db.save('trade_cal', info_to_db)
        return info_to_db

    @lazyval
    def trade_cal_index(self):
        return pd.to_datetime(self.trade_cal.tolist(), format='%Y%m%d')

    @lazyval
    def index_info(self):
        return self.get_index_info()

    @lazyval
    def stock_info(self):
        return self.get_stock_info()

    @lazyval
    def fund_info(self):
        return self.get_fund_info()

    def _code_to_type(self, code):
        if code in self.stock_info['ts_code'].values:
            return 'E'
        if code in self.index_info['ts_code'].values:
            return 'I'
        if code in self.fund_info['ts_code'].values:
            return 'FD'

    def get_price_daily(self, code, start: str, end: str, refresh=0):
        """
        按月存取股票的日线数据
        1. 如当月停牌无交易，则存入空数据(或0)
        2. 股票未上市，或已退市，则对应月份键值不存在
        3. 当月有交易，则存储交易日的价格数据
        4. 如交易月键值不存在，但股票状态是正常上市，则该月数据需要下载
        5. refresh两种模式，1: 一种是只刷新末月数据，2: 另一种是刷新start-end所有数据
        :param code:
        :param start:
        :param end:
        :param refresh:
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_DAILY_PRICE.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, EQUITY_DAILY_PRICE_META)

        tscode = symbol_std_to_tus(code)
        astype = self._code_to_type(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        m_start = pd.Timestamp(year=tstart.year, month=tstart.month, day=1)
        m_end = pd.Timestamp(year=tend.year, month=tend.month, day=1)

        vdates = pd.date_range(m_start, m_end, freq='MS')
        out = {}
        for dd in vdates:
            dtkey = dd.strftime(DATE_FORMAT)
            if refresh == 0 or (refresh == 1 and dd != vdates[-1]):
                # print(dd)
                val = db.load(dtkey)
                if val is not None:
                    out[dtkey] = val
                    continue
            start_raw = dd.strftime(DATE_FORMAT)
            end_raw = pd.Timestamp(year=dd.year, month=dd.month, day=dd.days_in_month).strftime(DATE_FORMAT)
            data = ts.pro_bar(tscode, asset=astype, start_date=start_raw, end_date=end_raw, freq='D')
            data = data.rename(columns={'vol': 'volume'})
            out[dtkey] = data.reindex(columns=EQUITY_DAILY_PRICE_META['columns'])
            db.save(dtkey, out[dtkey])

        all_out = pd.concat(out)
        all_out = all_out.set_index('trade_date', drop=True)
        all_out = all_out.sort_index(ascending=True)
        return all_out

    def get_price_minute(self, code, start, end, refresh=False):
        """
        按日存取股票的分钟线数据
        1. 如当日停牌无交易，则存入空数据
        2. 股票未上市，或已退市，则对应日键值不存在
        3. 当日有交易，则存储交易日的数据
        4. 如交易日键值不存在，但股票状态是正常上市，则该月数据需要下载
        5. refresh两种模式，1: 一种是只刷新末月数据，2: 另一种是刷新start-end所有数据
        :param code:
        :param start:
        :param end:
        :param refresh:
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_DAILY_PRICE.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, EQUITY_DAILY_PRICE_META)

        tscode = symbol_std_to_tus(code)
        astype = self._code_to_type(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        trade_cal = self.trade_cal_index
        vdates = trade_cal[(trade_cal > tstart) & (trade_cal <= tend)]

        out = {}
        for dd in vdates:
            dtkey = dd.strftime(DATE_FORMAT)
            if refresh == 0 or (refresh == 1 and dd != vdates[-1]):
                # print(dd)
                val = db.load(dtkey)
                if val is not None:
                    out[dtkey] = val
                    continue
            start_raw = dd.strftime(DATETIME_FORMAT)
            end_raw = (dd+pd.Timedelta(hours=17)).strftime(DATETIME_FORMAT)

            data = ts.pro_bar(tscode, asset=astype, start_date=start_raw, end_date=end_raw, freq='1min')
            data = data.rename(columns={'vol': 'volume'})
            out[dtkey] = data.reindex(columns=EQUITY_MINUTE_PRICE_META['columns'])
            db.save(dtkey, out[dtkey])

        all_out = pd.concat(out)
        all_out = all_out.set_index('trade_time', drop=True)
        all_out = all_out.sort_index(ascending=True)
        return all_out

    def get_stock_xdxr(self, code, refresh=False):
        """
        股票除权除息信息，如需更新，则更新股票历史所有数据。
        :param code:
        :param refresh:
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_XDXR.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, STOCK_XDXR_META)

        if not refresh:
            val = db.load(code)
            if val is not None:
                return val

        # log.info('update...')
        tscode = symbol_std_to_tus(code)
        # fields = ''
        # for ff in STOCK_XDXR_META['columns']:
        #     fields+=ff+','
        info = self.pro_api.dividend(ts_code=tscode)
        info_to_db = info.reindex(columns=STOCK_XDXR_META['columns'])
        db.save(code, info_to_db)
        return info_to_db

    def get_index_info(self):
        """"""
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_EQUITY_INFO.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, EQUITY_INFO_META)

        val = db.load(TusKeys.INDEX_INFO.value)
        if val is not None:
            return val

        def conv1(sym, subfix):
            stock, market = sym.split('.')
            code = stock + subfix
            return code

        log.info('update...')
        fields = 'ts_code,name,list_date'
        info1 = self.pro_api.index_basic(market='SSE', fields=fields)
        info1.loc[:, 'ts_code'] = info1.loc[:, 'ts_code'].apply(conv1, subfix='.XSHG')
        info1.loc[:, 'exchange'] = 'SSE'
        info2 = self.pro_api.index_basic(market='SZSE', fields=fields)
        info2.loc[:, 'ts_code'] = info2.loc[:, 'ts_code'].apply(conv1, subfix='.XSHE')
        info2.loc[:, 'exchange'] = 'SZSE'
        info = pd.concat([info1, info2], axis=0)
        info.loc[:, 'list_date'].fillna('20000101', inplace=True)

        info_to_db = pd.DataFrame({
            'ts_code': info['ts_code'],
            'exchange': info['exchange'],
            'name': info['name'],
            'start_date': info['list_date'],
            'end_date': '21000101'
        })

        db.save(TusKeys.INDEX_INFO.value, info_to_db)
        return info_to_db

    def get_stock_info(self):
        """"""
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_EQUITY_INFO.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, EQUITY_INFO_META)

        val = db.load(TusKeys.STOCK_INFO.value)
        if val is not None:
            return val

        log.info('update...')
        fields = 'ts_code,symbol,name,exchange,area,industry,list_date,delist_date'
        info1 = self.pro_api.stock_basic(list_status='L', fields=fields)  # 上市状态： L上市 D退市 P暂停上市
        info2 = self.pro_api.stock_basic(list_status='D', fields=fields)
        info3 = self.pro_api.stock_basic(list_status='P', fields=fields)
        info = pd.concat([info1, info2, info3], axis=0)
        info.loc[:, 'ts_code'] = info.loc[:, 'ts_code'].apply(symbol_tus_to_std)
        info.loc[:, 'delist_date'].fillna('21000101', inplace=True)

        info_to_db = pd.DataFrame({
            'ts_code': info['ts_code'],
            'exchange': info['exchange'],
            'name': info['name'],
            'start_date': info['list_date'],
            'end_date': info['delist_date'],
        })

        db.save(TusKeys.STOCK_INFO.value, info_to_db)
        return info_to_db

    def get_fund_info(self):
        """"""
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_EQUITY_INFO.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, EQUITY_INFO_META)

        val = db.load(TusKeys.FUND_INFO.value)
        if val is not None:
            return val

        log.info('update...')
        fields = 'ts_code,name,list_date,delist_date'
        info = self.pro_api.fund_basic(market='E', fields=fields)  # 交易市场: E场内 O场外（默认E）
        # info2 = self.pro_api.fund_basic(market='O', fields=fields)
        # info = pd.concat([info1, info2], axis=0)
        info.loc[:, 'ts_code'] = info.loc[:, 'ts_code'].apply(symbol_tus_to_std)
        info.loc[:, 'delist_date'].fillna('21000101', inplace=True)
        info.loc[:, 'exchange'] = info.loc[:, 'ts_code'].apply(lambda x: 'SSE' if x.endswith('.XSHG') else 'SZSE')

        info_to_db = pd.DataFrame({
            'ts_code': info['ts_code'],
            'exchange': info['exchange'],
            'name': info['name'],
            'start_date': info['list_date'],
            'end_date': info['delist_date'],
        })

        db.save(TusKeys.FUND_INFO.value, info_to_db)
        return info_to_db

    def get_index_weight(self, index_symbol, date, refresh=False):
        """
        tushare index_weight数据, 月初第一个交易日和月末最后一个交易日更新
        :param index_symbol:
        :param date:
        :param refresh:
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_INDEX_WEIGHT.value + index_symbol),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, INDEX_WEIGHT_META)

        # 找到所处月份的第一个交易日
        trdt = pd.Timestamp(date)
        m_start = pd.Timestamp(year=trdt.year, month=trdt.month, day=1)
        m_end = pd.Timestamp(year=trdt.year, month=trdt.month, day=trdt.days_in_month)

        trade_cal = self.trade_cal_index
        valid_dates = trade_cal[(trade_cal >= m_start) & (trade_cal <= m_end)]
        dtkey = valid_dates[0]
        dtkey = dtkey.strftime(DATE_FORMAT)

        if not refresh:
            val = db.load(dtkey)
            if val is not None:
                return val

        # log.info('update...')

        sym = symbol_std_to_tus(index_symbol)
        info = self.pro_api.index_weight(index_code=sym, strat_date=m_start.strftime(DATE_FORMAT),
                                         end_date=m_end.strftime(DATE_FORMAT))
        if not info.empty:
            # # t_dates = pd.to_datetime(info['trade_date'], format='%Y%m%d')
            # # info = info[t_dates >= m_start]
            # dtkey = info.loc[:, 'trade_date'].iloc[-1]

            info = info[info['trade_date'] == dtkey]
            info.loc[:, 'con_code'] = info['con_code'].apply(symbol_tus_to_std)

            info = info.reindex(columns=INDEX_WEIGHT_META['columns'])
            db.save(dtkey, info)
            return info
        return None


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
    # df = reader.get_stock_xdxr('002465.XSHE', refresh=False)

    # df = reader.get_price_daily('002465.XSHE', '20190101', '20200303', refresh=2)
    df = reader.get_price_minute('002465.XSHE', '20191201', '20200303', refresh=2)
    print(df)

    # print(timeit.Timer(lambda: reader.get_stock_xdxr('002465.XSHE', refresh=True)).timeit(1))
    # print(timeit.Timer(lambda: reader.get_index_weight('399300.XSHE', '20200318', refresh=True)).timeit(1))
    # print(timeit.Timer(lambda: reader.get_stock_xdxr('002465.XSHE', refresh=False)).timeit(1))
    # print(timeit.Timer(lambda: reader.get_index_weight('399300.XSHE', '20200318', refresh=False)).timeit(1))

    # print(timeit.Timer(lambda: reader.get_price_daily('002465.XSHE', '20190101', '20200303', refresh=2)).timeit(3))
    # print(timeit.Timer(lambda: reader.get_price_daily('002465.XSHE', '20190101', '20200303', refresh=0)).timeit(3))
