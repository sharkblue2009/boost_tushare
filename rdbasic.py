"""
基础数据，不定期更新
"""

from .apiwrapper import api_call
from .proloader import TusNetLoader
from .schema import *
from .utils.memoize import lazyval
from .utils.xcutils import session_day_to_min_tus, MONTH_END, MONTH_START
from .xcdb.xcdb import *


class XcReaderBasic(object):
    """
    Basic Information
    """
    facc = None
    xctus_current_day = None
    xctus_first_date = pd.Timestamp('20080101')
    netloader: TusNetLoader = None

    _trade_cal_raw = None
    _trade_cal_day = None
    _trade_cal_1min = None
    _trade_cal_5min = None

    @property
    def trade_cal_raw(self):
        if self._trade_cal_raw is None:
            db = self.facc(TusSdbs.SDB_CALENDAR.value, CALENDAR_RAW_META)
            self._trade_cal_raw = db.load(TusKeys.CAL_RAW.value)
        return self._trade_cal_raw

    @trade_cal_raw.setter
    def trade_cal_raw(self, value):
        self._trade_cal_raw = None
        self._trade_cal_day = None
        self._trade_cal_1min = None
        self._trade_cal_5min = None

    @property
    def trade_cal(self):
        """
        当前有效的交易日历
        :return:
        """
        if self._trade_cal_day is None:
            db = self.facc(TusSdbs.SDB_CALENDAR.value, CALENDAR_IDX_META)
            self._trade_cal_day = db.load(TusKeys.CAL_INDEX_DAY.value)

        return self._trade_cal_day

    @property
    def trade_cal_1min(self):
        if self._trade_cal_1min is None:
            db = self.facc(TusSdbs.SDB_CALENDAR.value, CALENDAR_IDX_META)
            self._trade_cal_1min = db.load(TusKeys.CAL_INDEX_1MIN.value)
        return self._trade_cal_1min

    @property
    def trade_cal_5min(self):
        if self._trade_cal_5min is None:
            db = self.facc(TusSdbs.SDB_CALENDAR.value, CALENDAR_IDX_META)
            self._trade_cal_5min = db.load(TusKeys.CAL_INDEX_5MIN.value)
        return self._trade_cal_5min

    def freq_to_cal(self, freq):
        if freq == '1min':
            return self.trade_cal_1min
        if freq == '5min':
            return self.trade_cal_5min

    def cal_m2day_map(self):
        pass

    def cal_d2min_map(self):
        pass

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

    def asset_type(self, code):
        if code in self.stock_info.index.values:
            astype = 'E'
        elif code in self.index_info.index.values:
            astype = 'I'
        elif code in self.fund_info.index.values:
            astype = 'FD'
        else:
            raise KeyError
        return astype

    def asset_lifetime(self, code, astype='E'):
        """

        :param code:
        :param astype: 'E', 'I', 'FD'
        :return:
        """
        if astype is None:
            astype = self.asset_type(code)

        if astype == 'E':
            info = self.stock_info
        elif astype == 'I':
            info = self.index_info
        elif astype == 'FD':
            info = self.fund_info
        else:
            return

        start_date = info.loc[code, 'start_date']
        end_date = info.loc[code, 'end_date']
        start_date, end_date = pd.Timestamp(start_date), pd.Timestamp(end_date)
        return start_date, end_date

    ##########################################################
    # Reader API
    ##########################################################
    @api_call
    def tusbooster_lookup_calendar(self):

        db = self.facc(TusSdbs.SDB_CALENDAR.value, GENERAL_OBJ_META)

        b_need_update = True
        first_date = db.load(TusKeys.CAL_FIRST_DATE.value)
        current_date = db.load(TusKeys.CAL_CURRENT_DATE.value)

        if first_date is not None and current_date is not None:
            if first_date == self.xctus_first_date and current_date == self.xctus_current_day:
                b_need_update = False

        if b_need_update:
            print('Update calendar:{}, {}'.format(first_date, current_date))

            info = self.netloader.set_trade_cal()
            db.metadata = CALENDAR_RAW_META
            tcal = db.save(TusKeys.CAL_RAW.value, info)

            tcal_day = pd.to_datetime(tcal.tolist(), format='%Y%m%d')
            tcal_day = tcal_day[(self.xctus_first_date <= tcal_day) & (tcal_day <= self.xctus_current_day)]
            _tcal_1min = session_day_to_min_tus(tcal_day, '1min', market_open=False)
            _tcal_5min = session_day_to_min_tus(tcal_day, '5min', market_open=False)

            db.metadata = CALENDAR_IDX_META
            db.save(TusKeys.CAL_INDEX_DAY.value, tcal_day)
            db.save(TusKeys.CAL_INDEX_1MIN.value, _tcal_1min)
            db.save(TusKeys.CAL_INDEX_5MIN.value, _tcal_5min)

            db.metadata = GENERAL_OBJ_META
            db.save(TusKeys.CAL_FIRST_DATE.value, self.xctus_first_date)
            db.save(TusKeys.CAL_CURRENT_DATE.value, self.xctus_current_day)
            self.trade_cal_raw = None
            return tcal

        return

    # @api_call
    # def get_trade_cal_index(self, flag=IOFLAG.READ_XC):
    #     trade_cal = self.lookup_trade_cal()
    #     all_trade_cal = pd.to_datetime(trade_cal.tolist(), format='%Y%m%d')
    #     valid_trade_cal = all_trade_cal[all_trade_cal < self.xctus_current_day]
    #     return valid_trade_cal

    @api_call
    def get_index_info(self, flag=IOFLAG.READ_XC):
        """

        :return:
        """
        db = self.facc(TusSdbs.SDB_ASSET_INFO.value, ASSET_INFO_META)
        kk = TusKeys.INDEX_INFO.value

        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            if val is not None:
                return val
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_index_info()
            return db.save(kk, info)
        return

    @api_call
    def get_stock_info(self, flag=IOFLAG.READ_XC):
        """"""
        db = self.facc(TusSdbs.SDB_ASSET_INFO.value, ASSET_INFO_META)
        kk = TusKeys.STOCK_INFO.value

        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            if val is not None:
                return val
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_stock_info()
            return db.save(kk, info)

        return

    @api_call
    def get_fund_info(self, flag=IOFLAG.READ_XC):
        """

        :return:
        """
        db = self.facc(TusSdbs.SDB_ASSET_INFO.value, ASSET_INFO_META)
        kk = TusKeys.FUND_INFO.value

        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            if val is not None:
                return val
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_fund_info()
            return db.save(kk, info)

        return

    @api_call
    def get_index_weight(self, index_symbol, date, month_start=False, flag=IOFLAG.READ_XC):
        """
        tushare index_weight数据, 月初第一个交易日和月末最后一个交易日更新(20200318: 只有月末最后一个交易日更新数据？)
        :param index_symbol:
        :param date:
        :param month_start: use month start or month end data.
        :return:
        """
        # 找到所处月份的第一个交易日
        trdt = pd.Timestamp(date)
        if month_start:
            tdday = MONTH_START(trdt, self.trade_cal)
        else:
            tdday = MONTH_END(trdt, self.trade_cal)
        if tdday is None:
            return
        dtkey = tdday.strftime(DATE_FORMAT)

        db = self.facc((TusSdbs.SDB_INDEX_WEIGHT.value + index_symbol), INDEX_WEIGHT_META)
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
            val = db.load(dtkey)
            if val is not None:
                return val
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_index_weight(index_symbol, tdday)
            return db.save(dtkey, info)

        return

    @api_call
    def get_index_classify(self, level, src='SW', flag=IOFLAG.READ_XC):
        """
        申万行业分类

        接口：index_classify
        描述：获取申万行业分类，包括申万28个一级分类，104个二级分类，227个三级分类的列表信息
        :return:
        """
        db = self.facc(TusSdbs.SDB_INDEX_CLASSIFY.value, INDEX_CLASSIFY_META)

        kk = level.upper()

        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            if val is not None:
                return val
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_index_classify(level, src)
            return db.save(kk, info)

        return

    @api_call
    def get_index_member(self, index_code, flag=IOFLAG.READ_XC):
        """

        :param index_code:
        :return:
        """
        db = self.facc(TusSdbs.SDB_INDEX_MEMBER.value, INDEX_MEMBER_META)

        kk = index_code.upper()

        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            if val is not None:
                return val
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_index_member(index_code)
            return db.save(kk, info)

        return
