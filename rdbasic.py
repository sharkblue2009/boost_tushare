"""
基础数据，不定期更新
"""

from collections import OrderedDict

from .domain import XcDomain

from .apiwrapper import api_call
from .proloader import TusNetLoader
from .schema import *
from .utils.memoize import lazyval
from .utils.xcutils import *
from .xcdb.xcdb import *


class XcReaderBasic(object):
    """
    Basic Information
    """
    facc = None
    domain: XcDomain = None
    xctus_current_day = None
    xctus_first_day = None

    ##########################################################
    # Reader API
    ##########################################################
    @api_call
    def tusbooster_domain_update(self):

        db = self.facc(TusSdbs.SDB_CALENDAR.value, GENERAL_OBJ_META)

        b_need_update = True
        first_date = db.load(TusKeys.CAL_FIRST_DATE.value)
        current_date = db.load(TusKeys.CAL_CURRENT_DATE.value)
        del db

        if first_date is not None and current_date is not None:
            if first_date == self.xctus_first_day and current_date == self.xctus_current_day:
                b_need_update = False

        if b_need_update:
            print('Update calendar:{}, {}'.format(first_date, current_date))
            db = self.facc(TusSdbs.SDB_CALENDAR.value, GENERAL_OBJ_META)
            info = self.netloader.set_trade_cal()
            db.metadata = CALENDAR_RAW_META
            tcal = db.save(TusKeys.CAL_RAW.value, info)

            tcal_day = pd.to_datetime(tcal.tolist(), format='%Y%m%d')
            tcal_day = tcal_day[(self.xctus_first_day <= tcal_day) & (tcal_day <= self.xctus_current_day)]
            _tcal_1min = session_day_to_min_tus(tcal_day, '1min', market_open=False)
            _tcal_5min = session_day_to_min_tus(tcal_day, '5min', market_open=False)

            db.metadata = CALENDAR_D_IDX_META
            db.save(TusKeys.CAL_INDEX_DAY.value, tcal_day)
            db.save(TusKeys.CAL_INDEX_1MIN.value, _tcal_1min)
            db.save(TusKeys.CAL_INDEX_5MIN.value, _tcal_5min)

            db.metadata = GENERAL_OBJ_META
            db.save(TusKeys.CAL_FIRST_DATE.value, self.xctus_first_day)
            db.save(TusKeys.CAL_CURRENT_DATE.value, self.xctus_current_day)
            print('Done')
            del db

        print('Update asset info...')
        self.get_index_info(IOFLAG.READ_NETDB)
        self.get_stock_info(IOFLAG.READ_NETDB)
        self.get_fund_info(IOFLAG.READ_NETDB)
        self.get_index_classify(level='L1', flag=IOFLAG.READ_NETDB)
        self.get_index_classify(level='L2', flag=IOFLAG.READ_NETDB)
        print('Done')

        return

    @api_call
    def tusbooster_domain_load(self):
        """
        load domain data from DB.
        :return:
        """
        domain = self.domain
        db = self.facc(TusSdbs.SDB_CALENDAR.value, GENERAL_OBJ_META)
        first_date = db.load(TusKeys.CAL_FIRST_DATE.value)
        current_date = db.load(TusKeys.CAL_CURRENT_DATE.value)
        print('DB_Domain calendar:{}, {}'.format(first_date, current_date))
        domain.xctus_first_day = first_date
        domain.xctus_current_day = current_date
        del db

        db = self.facc(TusSdbs.SDB_CALENDAR.value, CALENDAR_D_IDX_META)
        domain._trade_cal_day = db.load(TusKeys.CAL_INDEX_DAY.value)
        # db = self.facc(TusSdbs.SDB_CALENDAR.value, CALENDAR_D_IDX_META)
        domain._trade_cal_1min = db.load(TusKeys.CAL_INDEX_1MIN.value)
        domain._trade_cal_5min = db.load(TusKeys.CAL_INDEX_5MIN.value)
        del db

        info = self.get_index_info()
        domain.index_info = info.set_index('ts_code', drop=True)

        info = self.get_stock_info()
        domain.stock_info = info.set_index('ts_code', drop=True)

        info = self.get_fund_info()
        domain.fund_info = info.set_index('ts_code', drop=True)

        # domain.suspend_info = self.get_suspend_d(self.xctus_first_day, self.xctus_current_day)
        return

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
        mmidx = self.domain.gen_dindex_monthly(MONTH_START(trdt), MONTH_END(trdt))
        if len(mmidx) == 0:
            return

        if month_start:
            tdday = mmidx[0]
        else:
            tdday = mmidx[-1]

        dtkey = dt64_to_strdt(tdday)

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

    @api_call
    def get_suspend_d(self, start='20100101', end='21000101', flag=IOFLAG.READ_XC):
        """
        每日所有股票停复牌信息
        注： 股票存在停牌半天的情况。但也会在suspend列表中体现
        :param code:
        :return:
        """
        domain = self.domain
        mmdts = domain.gen_keys_daily(start, end, None, None)
        if mmdts is None:
            return

        db = self.facc(TusSdbs.SDB_SUSPEND_D.value, SUSPEND_D_META)
        out = {}
        for dd in mmdts:
            dtkey = dt64_to_strdt(dd)
            if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
                val = db.load(dtkey, raw_mode=True)
                if val is not None:
                    out[dtkey] = val
                    continue
            if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
                ii = self.netloader.set_suspend_d(dd)
                out[dtkey] = db.save(dtkey, ii, raw_mode=True)

        out = list(out.values())
        out = np.vstack(out)
        all_out = pd.DataFrame(data=out, columns=SUSPEND_D_META['columns'])
        all_out['suspend_type'] = all_out['suspend_type'].astype(str)
        # all_out['suspend_timing'] = all_out['suspend_timing'].astype(str)

        all_out = all_out.set_index(['trade_date', 'ts_code'], drop=True)
        all_out.index.set_levels(pd.to_datetime(all_out.index.levels[0], format=DATE_FORMAT), level=0, inplace=True)
        all_out = all_out.sort_index(axis=0, level=0, ascending=True)
        # all_out = all_out.loc[pd.IndexSlice[tstart:tend, :]]
        # mask = all_out.index.map(lambda x: (x[0]>=tstart) & (x[0]<=tend))
        # all_out = all_out.loc[mask]

        return all_out
