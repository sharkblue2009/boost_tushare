"""
行情数据，每日更新
"""

from .apiwrapper import api_call
from .proloader import TusNetLoader
from .schema import *
from .utils.xcutils import *
# from .utils.memoize import lazyval
from .xcdb.xcdb import *


class XcReaderPrice(object):
    """
    行情数据
    """
    master_db = None
    trade_cal = None
    trade_cal_1min = None
    trade_cal_5min = None

    netloader: TusNetLoader = None

    @api_call
    def get_price_daily(self, code, start: str, end: str, astype=None, flag=IOFLAG.READ_XC):
        """
        按月存取股票的日线数据
        1. 如当月停牌无交易，则存入空数据(或0)
        2. 股票未上市，或已退市，则对应月份键值不存在
        3. 当月有交易，则存储交易日的价格数据
        4. 如交易月键值不存在，但股票状态是正常上市，则该月数据需要下载
        5. refresh两种模式，0: 不更新数据，1: 数据做完整性检查，如不完整则更新，2: 更新start-end所有数据
        如何判断某一月度键值对应的价格数据是否完整：
            1. 对于Index, Fund, 由于不存在停牌情况，因此价格数据的trade_date和当月的trade_calendar能匹配，则数据完整
            2. 对于股票，由于可能停牌，因此，月度价格数据的trade_date加上suspend_date, 和当月trade_calendar匹配，则数据完整
        :param code:
        :param start:
        :param end:
        :return:
        """
        if astype is None:
            astype = self.asset_type(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_monthly(tstart, tend, self.asset_lifetime(code, astype), self.trade_cal)
        if len(vdates) == 0:
            return None
        dayindex, alldays = gen_dayindex_monthly(vdates, self.trade_cal)

        db = self.facc(TusSdbs.SDB_DAILY_PRICE.value + code, EQUITY_DAILY_PRICE_META)
        out = {}
        for dd in vdates:
            dtkey = dd.strftime(DATE_FORMAT)
            if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
                val = db.load(dtkey, raw_mode=True)
                if val is not None:
                    out[dtkey] = val
                    continue
            if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
                ii = self.netloader.set_price_daily(code, MONTH_START(dd), MONTH_END(dd), astype)
                if ii is None:
                    ii = pd.DataFrame(index=dayindex[dd], columns=EQUITY_DAILY_PRICE_META['columns'], dtype='f8')
                else:
                    ii = ii.set_index('trade_date', drop=True)
                    ii.index = pd.to_datetime(ii.index, format=DATE_FORMAT)
                    ii = ii.reindex(index=dayindex[dd])
                out[dtkey] = db.save(dtkey, ii, raw_mode=True)

        out = list(out.values())
        out = np.vstack(out)
        all_out = pd.DataFrame(data=out, columns=EQUITY_DAILY_PRICE_META['columns'])
        all_out = all_out.set_index(alldays)
        all_out = all_out[(all_out.index >= tstart) & (all_out.index <= tend)]
        return all_out

    @api_call
    def get_price_minute(self, code, start, end, freq='5min', astype='E', resample=False,
                         flag=IOFLAG.READ_XC):
        """
        按日存取股票的分钟线数据
        Note: 停牌时，pro_bar对于分钟K线，仍然能取到数据，返回的OHLC是pre_close值， vol值为0.
        但对于停牌时的日线， 则没有数据。
        1. 如当日停牌无交易，则存入空数据(tushare停牌时分钟数据返回ffill值，vol=0)
        2. 股票未上市，或已退市，则对应日键值不存在
        3. 当日有交易，则存储交易日的数据
        4. 如交易日键值不存在，但股票状态是正常上市，则该月数据需要下载
        5. refresh两种模式，1: 一种是只刷新末月数据，2: 另一种是刷新start-end所有数据
        注： tushare每天有241个分钟数据，包含9:30集合竞价数据(集合竞价成交+第一笔成交)
        交易日键值对应的分钟价格数据完整性检查：
            1. 股票， 要么数据完整241条数据，要么为空
            2. 指数和基金，无停牌，因此数据完整。
        :param code:
        :param start:
        :param end:
        :param freq:
        :param astype: asset type. 'E' for stock, 'I' for index, 'FD' for fund.
        :param resample: if use 1Min data resample to others
        :param flag:
        :return:
        """
        if freq not in XTUS_FREQS:
            return None

        if resample:
            curfreq = '1min'
        else:
            curfreq = freq

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_daily(tstart, tend, self.asset_lifetime(code, astype), self.trade_cal)
        if len(vdates) == 0:
            return None
        cc = {'1min': self.trade_cal_5min, '5min': self.trade_cal_5min, '15min': None, '30min': None, '60min': None}
        minindex, allmins = gen_minindex_daily(vdates, cc[freq])

        db = self.facc((TusSdbs.SDB_MINUTE_PRICE.value + code + curfreq), EQUITY_MINUTE_PRICE_META)
        out = {}
        for dd in vdates:
            dtkey = dd.strftime(DATE_FORMAT)
            if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
                val = db.load(dtkey, raw_mode=True)
                if val is not None:
                    out[dtkey] = val
                    continue
            if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
                ii = self.netloader.set_price_minute(code, dd, dd, curfreq, astype)
                if ii is None:
                    ii = pd.DataFrame(index=minindex[dd], columns=EQUITY_MINUTE_PRICE_META['columns'], dtype='f8')
                else:
                    ii = ii.set_index('trade_time', drop=True)
                    ii.index = pd.to_datetime(ii.index, format=DATETIME_FORMAT)
                    ii = ii.reindex(index=minindex[dd])
                    if (ii.volume == 0.0).all():
                        # 如果全天无交易，vol == 0, 则清空df.
                        ii.loc[:, :] = np.nan
                out[dtkey] = db.save(dtkey, ii, raw_mode=True)

        out = list(out.values())
        out = np.concatenate(out)
        all_out = pd.DataFrame(data=out, columns=EQUITY_MINUTE_PRICE_META['columns'])
        all_out = all_out.set_index(allmins)

        if resample:
            periods = cc[freq]
            all_out = price1m_resample(all_out, periods, market_open=True)

        # if (len(all_out) % 241) != 0:
        #     # Very slow
        #     print('unaligned:{}:-{}'.format(code, len(all_out)))
        #     # all_min_idx = self.trade_cal_index_minutes
        #     # tt_idx = all_min_idx[(all_min_idx >= tstart) & (all_min_idx <= (tend + pd.Timedelta(days=1)))]
        #     tt_idx = session_day_to_min_tus([dd], freq)
        #     all_out = all_out.reindex(index=tt_idx)
        #     all_out.index.name = 'trade_time'
        #     # print('::{}'.format(len(all_out)))

        return all_out

    @api_call
    def get_stock_daily_info(self, code, start, end, flag=IOFLAG.READ_XC):
        """
        Get stock daily information.
        :param code:
        :param start:
        :param end:
        :return:
        """
        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_monthly(tstart, tend, self.asset_lifetime(code, 'E'), self.trade_cal)
        if len(vdates) == 0:
            return
        dayindex, alldays = gen_dayindex_monthly(vdates, self.trade_cal)

        db = self.facc(TusSdbs.SDB_STOCK_DAILY_INFO.value + code, STOCK_DAILY_INFO_META)
        out = {}
        for dd in vdates:
            dtkey = dd.strftime(DATE_FORMAT)
            if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
                val = db.load(dtkey, raw_mode=True)
                if val is not None:
                    out[dtkey] = val
                    continue
            if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
                ii = self.netloader.set_stock_daily_info(code, MONTH_START(dd), MONTH_END(dd))
                if ii is None:
                    ii = pd.DataFrame(index=dayindex[dd], columns=STOCK_DAILY_INFO_META['columns'], dtype='f8')
                else:
                    ii = ii.set_index('trade_date', drop=True)
                    ii.index = pd.to_datetime(ii.index, format=DATE_FORMAT)
                    ii = ii.reindex(index=dayindex[dd])
                out[dtkey] = db.save(dtkey, ii, raw_mode=True)

        out = list(out.values())
        out = np.concatenate(out)
        all_out = pd.DataFrame(data=out, columns=STOCK_DAILY_INFO_META['columns'])
        all_out = all_out.set_index(alldays)
        all_out = all_out[(all_out.index >= tstart) & (all_out.index <= tend)]
        return all_out

    @api_call
    def get_stock_adjfactor(self, code, start: str, end: str, flag=IOFLAG.READ_XC):
        """
        按月存取股票的日线数据
        前复权:
            当日收盘价 × 当日复权因子 / 最新复权因子
        后复权:
            当日收盘价 × 当日复权因子
        :param code:
        :param start:
        :param end:
        :return:
        """
        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_monthly(tstart, tend, self.asset_lifetime(code, 'E'), self.trade_cal)
        if len(vdates) == 0:
            return
        dayindex, alldays = gen_dayindex_monthly(vdates, self.trade_cal)

        db = self.facc(TusSdbs.SDB_STOCK_ADJFACTOR.value + code, STOCK_ADJFACTOR_META)
        out = {}
        for dd in vdates:
            dtkey = dd.strftime(DATE_FORMAT)
            if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
                val = db.load(dtkey, raw_mode=True)
                if val is not None:
                    out[dtkey] = val
                    continue
            if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
                ii = self.netloader.set_stock_adjfactor(code, MONTH_START(dd), MONTH_END(dd))
                if ii is None:
                    ii = pd.DataFrame(index=dayindex[dd], columns=STOCK_ADJFACTOR_META['columns'], dtype='f8')
                else:
                    ii = ii.set_index('trade_date', drop=True)
                    ii.index = pd.to_datetime(ii.index, format=DATE_FORMAT)
                    ii = ii.reindex(index=dayindex[dd])
                out[dtkey] = db.save(dtkey, ii, raw_mode=True)

        out = list(out.values())
        out = np.concatenate(out)
        all_out = pd.DataFrame(data=out, columns=STOCK_ADJFACTOR_META['columns'])
        all_out = all_out.set_index(alldays)
        all_out = all_out[(all_out.index >= tstart) & (all_out.index <= tend)]
        return all_out

    @api_call
    def get_stock_xdxr(self, code, flag=IOFLAG.READ_XC):
        """
        股票除权除息信息，如需更新，则更新股票历史所有数据。
        :param code:
        :return:
        """
        db = self.facc(TusSdbs.SDB_STOCK_XDXR.value, STOCK_XDXR_META)

        kk = code
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            if val is not None:
                return val
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_stock_xdxr(code)
            return db.save(kk, info)

        return

    @api_call
    def get_stock_suspend(self, code, flag=IOFLAG.READ_XC):
        """
        每只股票的停复牌信息
        注： 股票存在停牌半天的情况。但也会在suspend列表中体现
        :param code:
        :return:
        """
        db = self.facc(TusSdbs.SDB_STOCK_SUSPEND.value, STOCK_SUSPEND_META)

        kk = code
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_DBONLY:
            val = db.load(kk)
            if val is not None:
                return val
        if flag == IOFLAG.READ_XC or flag == IOFLAG.READ_NETDB:
            info = self.netloader.set_stock_suspend(code)
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
        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_daily(tstart, tend, None, self.trade_cal)
        if len(vdates) == 0:
            return None

        db = self.facc(TusSdbs.SDB_SUSPEND_D.value, SUSPEND_D_META)
        out = {}
        for dd in vdates:
            dtkey = dd.strftime(DATE_FORMAT)
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
        all_out = all_out.loc[pd.IndexSlice[tstart:tend, :]]
        # mask = all_out.index.map(lambda x: (x[0]>=tstart) & (x[0]<=tend))
        # all_out = all_out.loc[mask]

        self.suspend_info = all_out  # read the suspend info into cache
        return all_out

    def stock_suspend(self, code):
        try:
            info = self.suspend_info.loc[pd.IndexSlice[:, code], :]
            info = info.droplevel(1)  # Drop the tscode index
            return info
        except:
            return None
