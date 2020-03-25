from cntus.utils.xctus_utils import symbol_std_to_tus, symbol_tus_to_std, session_day_to_min_tus
from cntus.xcachedb import *
from cntus.dbschema import *
import pandas as pd
from cntus.utils.memoize import lazyval

class TusBasicInfo(object):
    """

    """
    master_db = None
    pro_api = None
    tus_last_date = None

    def get_trade_cal(self, refresh=False):
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_TRADE_CALENDAR.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_SER_COL, None)
        if not refresh:
            val = db.load('trade_cal')
            if val is not None:
                return val

        info = self.pro_api.trade_cal()
        info_to_db = info[info['is_open'] == 1].loc[:, 'cal_date']
        db.save('trade_cal', info_to_db)
        return info_to_db

    @lazyval
    def trade_cal(self):
        return self.get_trade_cal()
    @lazyval
    def trade_cal_index(self):
        return pd.to_datetime(self.trade_cal.tolist(), format='%Y%m%d')

    @lazyval
    def trade_cal_index_minutes(self):
        return session_day_to_min_tus(self.trade_cal_index, freq='1Min')

    @lazyval
    def index_info(self):
        info = self.get_index_info()
        info = info.set_index('ts_code', drop=True)
        return info

    @lazyval
    def stock_info(self):
        info = self.get_stock_info()
        info = info.set_index('ts_code', drop=True)
        return info

    @lazyval
    def fund_info(self):
        info = self.get_fund_info()
        info = info.set_index('ts_code', drop=True)
        return info

    def asset_lifetime(self, code):
        if code in self.stock_info.index.values:
            info = self.stock_info
            astype = 'E'
        if code in self.index_info.index.values:
            info = self.index_info
            astype = 'I'
        if code in self.fund_info.index.values:
            info = self.fund_info
            astype = 'FD'

        start_date = info.loc[code, 'start_date']
        end_date = info.loc[code, 'end_date']
        start_date, end_date = pd.Timestamp(start_date), pd.Timestamp(end_date)
        return astype, start_date, end_date

    def gen_keys_monthly(self, start_dt, end_dt, limit_start, limit_end):

        # 当前交易品种的有效交易日历
        today = self.tus_last_date
        tstart = max([limit_start, start_dt])
        tend = min([limit_end, end_dt, today])

        m_start = pd.Timestamp(year=tstart.year, month=tstart.month, day=1)
        m_end = pd.Timestamp(year=tend.year, month=tend.month, day=tend.days_in_month)

        vdates = pd.date_range(m_start, m_end, freq='MS')
        return vdates

    def gen_keys_daily(self, start_dt, end_dt, limit_start, limit_end):

        # 当前交易品种的有效交易日历
        today = self.tus_last_date
        tstart = max([limit_start, start_dt])
        tend = min([limit_end, end_dt, today])

        trade_cal = self.trade_cal_index
        vdates = trade_cal[(trade_cal >= tstart) & (trade_cal <= tend)]
        return vdates

    def get_index_info(self, refresh=False):
        """

        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_ASSET_INFO.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, ASSET_INFO_META)

        if not refresh:
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
        if not info.empty:
            info.loc[:, 'list_date'].fillna('20000101', inplace=True)

            info_to_db = pd.DataFrame({
                'ts_code': info['ts_code'],
                'exchange': info['exchange'],
                'name': info['name'],
                'start_date': info['list_date'],
                'end_date': '21000101'
            })

            out = db.save(TusKeys.INDEX_INFO.value, info_to_db)
            return out
        return None

    def get_stock_info(self, refresh=False):
        """"""
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_ASSET_INFO.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, ASSET_INFO_META)

        if not refresh:
            val = db.load(TusKeys.STOCK_INFO.value)
            if val is not None:
                return val

        log.info('update...')
        fields = 'ts_code,symbol,name,exchange,area,industry,list_date,delist_date'
        info1 = self.pro_api.stock_basic(list_status='L', fields=fields)  # 上市状态： L上市 D退市 P暂停上市
        info2 = self.pro_api.stock_basic(list_status='D', fields=fields)
        info3 = self.pro_api.stock_basic(list_status='P', fields=fields)
        info = pd.concat([info1, info2, info3], axis=0)
        if not info.empty:
            info.loc[:, 'ts_code'] = info.loc[:, 'ts_code'].apply(symbol_tus_to_std)
            info.loc[:, 'delist_date'].fillna('21000101', inplace=True)

            info_to_db = pd.DataFrame({
                'ts_code': info['ts_code'],
                'exchange': info['exchange'],
                'name': info['name'],
                'start_date': info['list_date'],
                'end_date': info['delist_date'],
            })

            out = db.save(TusKeys.STOCK_INFO.value, info_to_db)
            return out
        return None

    def get_fund_info(self, refresh=False):
        """

        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_ASSET_INFO.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, ASSET_INFO_META)

        if not refresh:
            val = db.load(TusKeys.FUND_INFO.value)
            if val is not None:
                return val

        fields = 'ts_code,name,list_date,delist_date'
        info = self.pro_api.fund_basic(market='E', fields=fields)  # 交易市场: E场内 O场外（默认E）
        if not info.empty:
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

            out = db.save(TusKeys.FUND_INFO.value, info_to_db)
            return out
        return None

    def get_index_weight(self, index_symbol, date, refresh=False):
        """
        tushare index_weight数据, 月初第一个交易日和月末最后一个交易日更新(20200318: 只有月末更新数据？)
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
        vdates = trade_cal[(trade_cal >= m_start) & (trade_cal <= m_end)]
        dtkey = vdates[-1]
        dtkey = dtkey.strftime(DATE_FORMAT)

        if not refresh:
            val = db.load(dtkey)
            if val is not None:
                return val

        sym = symbol_std_to_tus(index_symbol)
        info = self.pro_api.index_weight(index_code=sym, strat_date=m_start.strftime(DATE_FORMAT),
                                         end_date=m_end.strftime(DATE_FORMAT))
        if not info.empty:
            # # t_dates = pd.to_datetime(info['trade_date'], format='%Y%m%d')
            # # info = info[t_dates >= m_start]
            # dtkey = info.loc[:, 'trade_date'].iloc[-1]

            info = info[info['trade_date'] == dtkey]
            info.loc[:, 'con_code'] = info['con_code'].apply(symbol_tus_to_std)

            out = db.save(dtkey, info)
            return out
        return None

    def get_index_classify(self, level, src='SW', refresh=False):
        """
        申万行业分类

        接口：index_classify
        描述：获取申万行业分类，包括申万28个一级分类，104个二级分类，227个三级分类的列表信息
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_INDEX_CLASSIFY.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, INDEX_CLASSIFY_META)

        lkey = level.upper()

        if not refresh:
            val = db.load(lkey)
            if val is not None:
                return val

        info = self.pro_api.index_classify(level=lkey, src=src)
        if not info.empty:
            out = db.save(lkey, info)
            return out
        return None

    def get_index_member(self, index_code, refresh=False):
        """

        :param index_code:
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_INDEX_MEMBER.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, INDEX_MEMBER_META)

        lkey = index_code.upper()

        if not refresh:
            val = db.load(lkey)
            if val is not None:
                return val

        info = self.pro_api.index_member(index_code=index_code, fields=INDEX_MEMBER_META['columns'])
        if not info.empty:
            info.loc[:, 'con_code'] = info['con_code'].apply(symbol_tus_to_std)
            out = db.save(lkey, info)
            return out
        return None