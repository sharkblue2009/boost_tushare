"""
基础数据，不定期更新
"""

from collections import OrderedDict

from .domain import XcDomain

from .apiwrapper import api_call
from .proloader import TusNetLoader
from .layout import *
from .utils.memoize import lazyval
from .utils.xcutils import *
from .xcdb.xcdb import *
from logbook import Logger

log = Logger('xtus')


class XcReaderBasic(XcDomain):
    """
    Basic Information
    """
    facc = None
    netloader: TusNetLoader = None

    def __init__(self):
        super(XcReaderBasic, self).__init__()

    def init_domain(self):
        """
        load domain data from DB.
        :return:
        """
        self.update_domain(force_mode=False)
        first_date = self.xctus_first_day
        current_date = self.xctus_last_day

        log.info('Domain calendar:{}, {}'.format(first_date, current_date))

        db = self.facc(TusSdbs.SDB_CALENDAR.value, CALENDAR_DTIDX_META)
        self._cal_day = db.load(TusKeys.CAL_INDEX_DAY.value)
        self._cal_1min = db.load(TusKeys.CAL_INDEX_1MIN.value)
        self._cal_5min = db.load(TusKeys.CAL_INDEX_5MIN.value)
        db.commit()

        return

    def update_domain(self, force_mode=False):
        """
        update domain members
        :param force_mode:
        :return:
        """
        tcal_old = self.get_trade_cal(IOFLAG.READ_DBONLY)
        if force_mode:
            log.info('Update trading calendar...')
            tcal = self.get_trade_cal(IOFLAG.READ_NETDB)
        else:
            tcal = self.get_trade_cal(IOFLAG.READ_XC)

        if tcal_old is not None and tcal is not None:
            min_size = min(len(tcal_old), len(tcal))
            if (tcal_old.values[:min_size] != tcal.values[:min_size]).any():
                log.info("trade cal mismatch!!!")
                # return

        # Align the last/first day to trade calendar
        tcday = pd.to_datetime(tcal.tolist(), format='%Y%m%d').sort_values(ascending=True)
        # Note: need to align the month boarder
        tcday = tcday[(MONTH_START(self.xctus_first_day) <= tcday) & (tcday <= MONTH_END(self.xctus_last_day))]
        db_first_day = tcday[0]
        db_last_day = tcday[-1]
        self.xctus_first_day = db_first_day
        self.xctus_last_day = db_last_day

        db = self.facc(TusSdbs.SDB_CALENDAR.value, GENERAL_OBJ_META)
        b_need_update = True
        first_date = db.load(TusKeys.CAL_FIRST_DATE.value)
        current_date = db.load(TusKeys.CAL_CURRENT_DATE.value)
        db.commit()

        if first_date is not None and current_date is not None:
            if first_date == db_first_day and current_date == db_last_day:
                b_need_update = False

        if b_need_update or force_mode:
            log.info('Update calendar index...')
            tc1min = session_day_to_min_tus(tcday, '1min', market_open=False)
            tc5min = session_day_to_min_tus(tcday, '5min', market_open=False)

            db = self.facc(TusSdbs.SDB_CALENDAR.value, CALENDAR_DTIDX_META)
            db.metadata = CALENDAR_DTIDX_META
            db.save(TusKeys.CAL_INDEX_DAY.value, tcday)
            db.save(TusKeys.CAL_INDEX_1MIN.value, tc1min)
            db.save(TusKeys.CAL_INDEX_5MIN.value, tc5min)

            db.metadata = GENERAL_OBJ_META
            db.save(TusKeys.CAL_FIRST_DATE.value, db_first_day)
            db.save(TusKeys.CAL_CURRENT_DATE.value, db_last_day)
            db.commit()

            log.info('Update asset info...')
            self.get_index_info(IOFLAG.READ_NETDB)
            self.get_stock_info(IOFLAG.READ_NETDB)
            self.get_fund_info(IOFLAG.READ_NETDB)
            self.get_index_classify(level='L1', flag=IOFLAG.READ_NETDB)
            self.get_index_classify(level='L2', flag=IOFLAG.READ_NETDB)

        # log.info('Domain update done.')

        return

    ##########################################################
    # Reader API
    ##########################################################

    @api_call
    def get_trade_cal(self, flag=IOFLAG.READ_XC):

        db = self.facc(TusSdbs.SDB_CALENDAR.value, CALENDAR_RAW_META)

        kk = TusKeys.CAL_RAW.value

        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            if val is not None:
                return val
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_trade_cal()
            return db.save(kk, info)
        return

    @api_call
    def get_index_info(self, flag=IOFLAG.READ_XC):
        """
        get index information
        :return:
        """
        db = self.facc(TusSdbs.SDB_ASSET_INFO.value, INDEX_INFO_META)
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
        db = self.facc(TusSdbs.SDB_ASSET_INFO.value, STOCK_INFO_META)
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
        db = self.facc(TusSdbs.SDB_ASSET_INFO.value, FUND_INFO_META)
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
        mmdts = self.gen_keys_monthly(date, date, None, None)
        mmidx = self.gen_dindex_monthly(mmdts[0], mmdts[-1])
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
        mmdts = self.gen_keys_daily(start, end, None, None)
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
        db.commit()

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
