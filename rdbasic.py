"""
基础数据，不定期更新
"""
from .utils.xcutils import session_day_to_min_tus, MONTH_END
from cntus.xcdb.xcdb import *
from .schema import *
import pandas as pd
from .utils.memoize import lazyval
from .proloader import TusNetLoader


class XcReaderBasic(object):
    """
    Basic Information
    """
    master_db = None
    xctus_last_date = None
    netloader: TusNetLoader = None

    @lazyval
    def trade_cal(self):
        return self.get_trade_cal()

    @lazyval
    def trade_cal_index(self):
        """
        当前有效的交易日历
        :return:
        """
        all_trade_cal = pd.to_datetime(self.trade_cal.tolist(), format='%Y%m%d')
        valid_trade_cal = all_trade_cal[all_trade_cal < self.xctus_last_date]
        return valid_trade_cal

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

        start_date = info.loc[code, 'start_date']
        end_date = info.loc[code, 'end_date']
        start_date, end_date = pd.Timestamp(start_date), pd.Timestamp(end_date)
        return start_date, end_date

    ##########################################################
    # Reader API
    ##########################################################
    def get_trade_cal(self, flag=IOFLAG.READ_XC):
        db = self.facc(TusSdbs.SDB_TRADE_CALENDAR.value,
                       TRD_CAL_META)
        kk = 'trade_cal'
        if flag == IOFLAG.READ_XC:
            val = db.load(kk)
            if val is not None:
                return val
            info = self.netloader.set_trade_cal()
            return db.save(kk, info)
        elif flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            return val
        elif flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_trade_cal()
            return db.save(kk, info)
        return

    def get_index_info(self, flag=IOFLAG.READ_XC):
        """

        :return:
        """
        db = self.facc(TusSdbs.SDB_ASSET_INFO.value, ASSET_INFO_META)
        kk = TusKeys.INDEX_INFO.value

        if flag == IOFLAG.READ_XC:
            val = db.load(kk)
            if val is not None:
                return val
            info = self.netloader.set_index_info()
            return db.save(kk, info)
        elif flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            return val
        elif flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_index_info()
            return db.save(kk, info)
        return

    def get_stock_info(self, flag=IOFLAG.READ_XC):
        """"""
        db = self.facc(TusSdbs.SDB_ASSET_INFO.value, ASSET_INFO_META)
        kk = TusKeys.STOCK_INFO.value

        if flag == IOFLAG.READ_XC:
            val = db.load(kk)
            if val is not None:
                return val
            info = self.netloader.set_stock_info()
            return db.save(kk, info)
        elif flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            return val
        elif flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_stock_info()
            return db.save(kk, info)
        return

    def get_fund_info(self, flag=IOFLAG.READ_XC):
        """

        :return:
        """
        db = self.facc(TusSdbs.SDB_ASSET_INFO.value, ASSET_INFO_META)
        kk = TusKeys.FUND_INFO.value

        if flag == IOFLAG.READ_XC:
            val = db.load(kk)
            if val is not None:
                return val
            info = self.netloader.set_fund_info()
            return db.save(kk, info)
        elif flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            return val
        elif flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_fund_info()
            return db.save(kk, info)
        return

    def get_index_weight(self, index_symbol, date, flag=IOFLAG.READ_XC):
        """
        tushare index_weight数据, 月初第一个交易日和月末最后一个交易日更新(20200318: 只有月末最后一个交易日更新数据？)
        :param index_symbol:
        :param date:
        :return:
        """
        # 找到所处月份的第一个交易日
        trdt = pd.Timestamp(date)
        last_tdday = MONTH_END(trdt, self.trade_cal_index)
        if last_tdday is None:
            return
        dtkey = last_tdday.strftime(DATE_FORMAT)

        db = self.facc((TusSdbs.SDB_INDEX_WEIGHT.value + index_symbol), INDEX_WEIGHT_META)
        if flag == IOFLAG.READ_XC:
            val = db.load(dtkey)
            if val is not None:
                return val
            info = self.netloader.set_index_weight(index_symbol, last_tdday)
            return db.save(dtkey, info)
        elif flag == IOFLAG.READ_DBONLY:
            val = db.load(dtkey)
            return val
        elif flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_index_weight(index_symbol, last_tdday)
            return db.save(dtkey, info)
        return

    def get_index_classify(self, level, src='SW', flag=IOFLAG.READ_XC):
        """
        申万行业分类

        接口：index_classify
        描述：获取申万行业分类，包括申万28个一级分类，104个二级分类，227个三级分类的列表信息
        :return:
        """
        db = self.facc(TusSdbs.SDB_INDEX_CLASSIFY.value, INDEX_CLASSIFY_META, readonly=True)

        kk = level.upper()

        if flag == IOFLAG.READ_XC:
            val = db.load(kk)
            if val is not None:
                return val
            info = self.netloader.set_index_classify(level, src)
            return db.save(kk, info)
        elif flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            return val
        elif flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_index_classify(level, src)
            return db.save(kk, info)
        return

    def get_index_member(self, index_code, flag=IOFLAG.READ_XC):
        """

        :param index_code:
        :return:
        """
        db = self.facc(TusSdbs.SDB_INDEX_MEMBER.value, INDEX_MEMBER_META)

        kk = index_code.upper()

        if flag == IOFLAG.READ_XC:
            val = db.load(kk)
            if val is not None:
                return val
            info = self.netloader.set_index_member(index_code)
            return db.save(kk, info)
        elif flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            return val
        elif flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_index_member(index_code)
            return db.save(kk, info)
        return
