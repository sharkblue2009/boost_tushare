import tushare as ts

from ._passwd import TUS_TOKEN
from .layout import *
from .utils.qos import ThreadingTokenBucket
from .utils.xcutils import *


class XcNLBasic(object):
    pro_api = None

    def set_trade_cal(self):
        info = self.pro_api.trade_cal()
        info_to_db = info[info['is_open'] == 1].loc[:, 'cal_date']
        return info_to_db

    def set_index_info(self):
        """

        :return:
        """

        # def conv1(sym, subfix):
        #     stock, market = sym.split('.')
        #     code = stock + subfix
        #     return code

        fields = 'ts_code,name,list_date'
        info1 = self.pro_api.index_basic(market='SSE', fields=fields)
        # info1.loc[:, 'ts_code'] = info1.loc[:, 'ts_code'].apply(conv1, subfix='.XSHG')
        info1.loc[:, 'exchange'] = 'SSE'
        info2 = self.pro_api.index_basic(market='SZSE', fields=fields)
        # info2.loc[:, 'ts_code'] = info2.loc[:, 'ts_code'].apply(conv1, subfix='.XSHE')
        info2.loc[:, 'exchange'] = 'SZSE'
        info = pd.concat([info1, info2], axis=0)
        if not info.empty:
            info.loc[:, 'list_date'].fillna('20000101', inplace=True)

            info_to_db = pd.DataFrame({
                'ts_code': info['ts_code'],
                'exchange': info['exchange'],
                'name': info['name'],
                'start_date': info['list_date'],
                'end_date': '21000101'
            })

            return info_to_db
        return None

    def set_stock_info(self):
        """"""
        fields = 'ts_code,symbol,name,exchange,area,industry,list_date,delist_date'
        info1 = self.pro_api.stock_basic(list_status='L', fields=fields)  # 上市状态： L上市 D退市 P暂停上市
        info2 = self.pro_api.stock_basic(list_status='D', fields=fields)
        info3 = self.pro_api.stock_basic(list_status='P', fields=fields)
        info = pd.concat([info1, info2, info3], axis=0)
        if not info.empty:
            # info.loc[:, 'ts_code'] = info.loc[:, 'ts_code'].apply(symbol_tus_to_std)
            info.loc[:, 'delist_date'].fillna('21000101', inplace=True)

            info_to_db = pd.DataFrame({
                'ts_code': info['ts_code'],
                'exchange': info['exchange'],
                'name': info['name'],
                'start_date': info['list_date'],
                'end_date': info['delist_date'],
            })
            return info_to_db
        return None

    def set_fund_info(self):
        """

        :return:
        """
        fields = 'ts_code,name,list_date,delist_date'
        info = self.pro_api.fund_basic(market='E', fields=fields)  # 交易市场: E场内 O场外（默认E）
        if not info.empty:
            # info2 = self.pro_api.fund_basic(market='O', fields=fields)
            # info = pd.concat([info1, info2], axis=0)
            # info.loc[:, 'ts_code'] = info.loc[:, 'ts_code'].apply(symbol_tus_to_std)
            info.loc[:, 'delist_date'].fillna('21000101', inplace=True)
            info.loc[:, 'exchange'] = info.loc[:, 'ts_code'].apply(lambda x: 'SSE' if x.endswith('.SH') else 'SZ')

            info_to_db = pd.DataFrame({
                'ts_code': info['ts_code'],
                'exchange': info['exchange'],
                'name': info['name'],
                'start_date': info['list_date'],
                'end_date': info['delist_date'],
            })
            return info_to_db
        return None

    def set_index_weight(self, index_symbol, date):
        """
        tushare index_weight数据, 月初第一个交易日和月末最后一个交易日更新(20200318: 只有月末更新数据？)
        :param index_symbol:
        :param date:
        :return:
        """
        # 找到所处月份的最后一个交易日
        if not isinstance(date, pd.Timestamp):
            date = pd.Timestamp(date)

        valid_day = date.strftime(DATE_FORMAT)
        info = self.pro_api.index_weight(index_code=index_symbol, trade_date=valid_day)
        if not info.empty:
            # # t_dates = pd.to_datetime(info['trade_date'], format='%Y%m%d')
            # # info = info[t_dates >= m_start]
            # dtkey = info.loc[:, 'trade_date'].iloc[-1]

            # info.loc[:, 'con_code'] = info['con_code'].apply(symbol_tus_to_std)
            info = info[info['trade_date'] == valid_day]
            return info
        return info

    def set_index_classify(self, level, src='SW'):
        """
        申万行业分类

        接口：index_classify
        描述：获取申万行业分类，包括申万28个一级分类，104个二级分类，227个三级分类的列表信息
        :return:
        """
        lkey = level.upper()
        info = self.pro_api.index_classify(level=lkey, src=src)
        return info

    def set_index_member(self, index_code):
        """

        :param index_code:
        :return:
        """

        info = self.pro_api.index_member(index_code=index_code, fields=INDEX_MEMBER_META['columns'])
        if not info.empty:
            # info.loc[:, 'con_code'] = info['con_code'].apply(symbol_tus_to_std)
            return info
        return info


class XcNLPrice(object):
    pro_api = None
    master_db = None
    ts_token = None

    def set_price_daily(self, code, start, end, astype='E'):
        """
        :param code:
        :param start:
        :param end:
        :return:
        """
        if not isinstance(start, pd.Timestamp):
            start, end = pd.Timestamp(start), pd.Timestamp(end)

        start_raw = start.strftime(DATE_FORMAT)
        end_raw = end.strftime(DATE_FORMAT)

        data = ts.pro_bar(code, asset=astype, start_date=start_raw, end_date=end_raw, freq='D')
        if data is not None:
            data = data.rename(columns={'vol': 'volume'})
        return data

    # from ratelimit import limits, sleep_and_retry
    # @sleep_and_retry
    # @limits(30, period=120)
    def set_price_minute(self, code, start, end, freq='1min', astype='E', merge_first=True):
        """
        Note: 停牌时，pro_bar对于分钟K线，仍然能取到数据，返回的OHLC是pre_close值， vol值为0.
                但对于停牌时的日线， 则没有数据。
        :param code:
        :param start:
        :param end:
        :param freq:
        :param merge_first: True, merge first 9:30 Kbar to follow KBar.
        :return:
        """
        if not isinstance(start, pd.Timestamp):
            start, end = pd.Timestamp(start), pd.Timestamp(end)

        start_raw = start.strftime(DATETIME_FORMAT)
        end_raw = (end + pd.Timedelta(hours=17)).strftime(DATETIME_FORMAT)

        self.ts_token.block_consume(10)
        data = ts.pro_bar(code, asset=astype, start_date=start_raw, end_date=end_raw, freq=freq)
        if data is not None:
            data = data.rename(columns={'vol': 'volume'})
            # convert %Y-%m-%d %H:%M:%S to %Y%m%d %H:%M:%S
            data['trade_time'] = data['trade_time'].apply(lambda x: x.replace('-', ''))

            nbars = XTUS_FREQ_BARS[freq] + 1
            if len(data) % nbars != 0:
                """
                 002478.SZ, 2020-07-20 00:00:00-2020-09-24 00:00:00, 2372-49
                 002481.SZ, 2020-07-20 00:00:00-2020-09-24 00:00:00, 2381-49
                """
                log.error('min kbar length incorrect: {}, {}-{}, {}-{}'.format(code, start, end, len(data), nbars))
                return None

            if merge_first:
                # Handle the first row of every day. (the Kbar at 9:30)
                # Note : Data from tushare is in reverse order
                for k in range(len(data) - 1, 0, -nbars):
                    v = data
                    if True:
                        # open KBar check.
                        # assert (v.loc[k - 1, 'pre_close'] == v.loc[k, 'close'])  # Only work for Stocks
                        tt = pd.Timestamp(v.trade_time[k])
                        assert ((tt.hour == 9) & (tt.minute == 30))

                    v.loc[k - 1, 'open'] = v.loc[k, 'open']  # Open
                    v.loc[k - 1, 'high'] = v.loc[(k - 1):(k + 1), 'high'].max()  # High
                    v.loc[k - 1, 'low'] = v.loc[(k - 1):(k + 1), 'low'].min()  # low
                    v.loc[k - 1, 'volume'] = v.loc[(k - 1):(k + 1), 'volume'].sum()  # volume
                    v.loc[k - 1, 'amount'] = v.loc[(k - 1):(k + 1), 'amount'].sum()  # amount

                mask = (np.arange(len(data)) % nbars) != (nbars - 1)
                data = data[mask]

        else:
            log.error('min data empty: {}, {}-{}'.format(code, start_raw, end_raw))
        return data

    def set_stock_daily_info(self, code, start, end):
        """
        write stock daily information.
        :param code:
        :param start:
        :param end:
        :return:
        """
        if not isinstance(start, pd.Timestamp):
            start, end = pd.Timestamp(start), pd.Timestamp(end)

        fcols = STOCK_DAILY_INFO_META['columns']

        start_raw = start.strftime(DATE_FORMAT)
        end_raw = end.strftime(DATE_FORMAT)
        self.ts_token.block_consume(1)
        data = self.pro_api.daily_basic(ts_code=code, start_date=start_raw, end_date=end_raw,
                                        fields=fcols + ['trade_date'])

        return data

    def set_stock_adjfactor(self, code, start, end):
        """
        :param code:
        :param start:
        :param end:
        :return:
        """
        if not isinstance(start, pd.Timestamp):
            start, end = pd.Timestamp(start), pd.Timestamp(end)

        fcols = STOCK_ADJFACTOR_META['columns']
        start_raw = start.strftime(DATE_FORMAT)
        end_raw = end.strftime(DATE_FORMAT)
        self.ts_token.block_consume(1)
        data = self.pro_api.adj_factor(ts_code=code, start_date=start_raw, end_date=end_raw,
                                       fields=fcols + ['trade_date'])

        return data

    def set_stock_moneyflow(self, code, start, end):
        """
        :param code:
        :param start:
        :param end:
        :return:
        """
        if not isinstance(start, pd.Timestamp):
            start, end = pd.Timestamp(start), pd.Timestamp(end)

        # fcols = STOCK_ADJFACTOR_META['columns']
        start_raw = start.strftime(DATE_FORMAT)
        end_raw = end.strftime(DATE_FORMAT)
        self.ts_token.block_consume(1)
        data = self.pro_api.moneyflow(ts_code=code, start_date=start_raw, end_date=end_raw)

        return data

    def set_stock_bakdaily(self, code, start, end):
        """
        :param code:
        :param start:
        :param end:
        :return:
        """
        if not isinstance(start, pd.Timestamp):
            start, end = pd.Timestamp(start), pd.Timestamp(end)

        start_raw = start.strftime(DATE_FORMAT)
        end_raw = end.strftime(DATE_FORMAT)
        self.ts_token.block_consume(1)
        data = self.pro_api.bak_daily(ts_code=code, start_date=start_raw, end_date=end_raw)

        return data

    def set_stock_margindetail(self, code, start, end):
        """
        :param code:
        :param start:
        :param end:
        :return:
        """
        if not isinstance(start, pd.Timestamp):
            start, end = pd.Timestamp(start), pd.Timestamp(end)

        start_raw = start.strftime(DATE_FORMAT)
        end_raw = end.strftime(DATE_FORMAT)
        self.ts_token.block_consume(1)
        data = self.pro_api.margin_detail(ts_code=code, start_date=start_raw, end_date=end_raw)

        return data

    def set_stock_suspend_d(self, code, start, end):
        """
        股票停复牌信息
        注： 股票存在停牌半天的情况。但也会在suspend列表中体现
        :param code:
        :return:
        """
        if not isinstance(start, pd.Timestamp):
            start, end = pd.Timestamp(start), pd.Timestamp(end)

        fcols = STOCK_SUSPEND_D_META['columns']

        start_raw = start.strftime(DATE_FORMAT)
        end_raw = end.strftime(DATE_FORMAT)
        self.ts_token.block_consume(1)
        data = self.pro_api.suspend_d(ts_code=code, start_date=start_raw, end_date=end_raw, fields=fcols)

        return data

    def set_suspend_d(self, date):
        """
        股票停复牌信息
        注： 股票存在停牌半天的情况。但也会在suspend列表中体现
        :param code:
        :return:
        """
        fcols = SUSPEND_D_META['columns']
        if not isinstance(date, pd.Timestamp):
            date = pd.Timestamp(date)
        start_raw = date.strftime(DATE_FORMAT)

        self.ts_token.block_consume(1)
        data = self.pro_api.suspend_d(trade_date=start_raw, fields=fcols)

        return data

    def set_stock_xdxr(self, code):
        """
        股票除权除息信息，如需更新，则更新股票历史所有数据。
        :param code:
        :return:
        """
        self.ts_token.block_consume(10)
        info = self.pro_api.dividend(ts_code=code)
        # fcols = STOCK_XDXR_META['columns']
        # info_to_db = info_to_db.iloc[::-1]
        return info

    def set_stock_suspend(self, code):
        """
        股票停复牌信息
        注： 股票存在停牌半天的情况。但也会在suspend列表中体现
        :param code:
        :return:
        """
        info = self.pro_api.suspend(ts_code=code)
        return info


class XcNLFinance(object):
    pro_api = None

    def set_income(self, code, period):
        fcols = STOCK_FIN_INCOME_META['columns']
        data = self.pro_api.income(ts_code=code, period=period, fields=fcols)

        return data

    def set_balancesheet(self, code, period):
        fcols = STOCK_FIN_BALANCE_META['columns']
        data = self.pro_api.balancesheet(ts_code=code, period=period, fields=fcols)

        return data

    def set_cashflow(self, code, period):
        fcols = STOCK_FIN_CASHFLOW_META['columns']
        data = self.pro_api.cashflow(ts_code=code, period=period,
                                     fields=fcols)
        return data

    def set_fina_indicator(self, code, period):
        fcols = STOCK_FIN_INDICATOR_META['columns']
        data = self.pro_api.fina_indicator(ts_code=code, period=period,
                                           fields=fcols)
        return data


class TusNetLoader(XcNLBasic, XcNLFinance, XcNLPrice):

    def __init__(self):
        """
        :param last_day: Tushare last date with data available,
                            we assume yesterday's data is available in today.
        """
        # self.calendar = get_calendar('XSHG')
        ts.set_token(TUS_TOKEN)
        self.pro_api = ts.pro_api()

        # 每分钟不超过500次，每秒8次，同时api调用不超过300个。
        self.ts_token = ThreadingTokenBucket(80, 300)

        super(TusNetLoader, self).__init__()


gnetloader: TusNetLoader = None


def netloader_init() -> TusNetLoader:
    global gnetloader
    if gnetloader is None:
        gnetloader = TusNetLoader()
    return gnetloader
