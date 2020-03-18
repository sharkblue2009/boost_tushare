from cntus.utils.xctus_utils import symbol_std_to_tus, session_day_to_min_tus
from cntus.xcachedb import *
from cntus.dbschema import *
import tushare as ts
import pandas as pd


class TusPriceInfo(object):
    """

    """
    master_db = None
    pro_api = None
    basic_info = None

    def integrity_check_monthly(self, code, month_start, val, astype='E'):
        """
        数据完整性检查
        :param code:
        :param month_start: Timestamp, Month start
        :param val:
        :return:
        """
        try:
            suspend = self.get_stock_suspend(code)
            sus_dates = pd.to_datetime(suspend['suspend_date'])
        except Exception as e:
            # create empty index
            sus_dates = pd.DatetimeIndex([], freq='D')

        trd_dates = self.trade_cal_index
        m_end = pd.Timestamp(year=month_start.year, month=month_start.month, day=month_start.days_in_month)
        m_end = min(self.tus_last_date, m_end)
        days_tcal = (trd_dates[(trd_dates >= month_start) & (trd_dates <= m_end)])
        days_susp = (sus_dates[(sus_dates >= month_start) & (sus_dates <= m_end)])

        if len(days_tcal) <= len(val) + len(days_susp):
            # 股票存在停牌半天的情况，也会被计入suspend列表
            return True

        log.info('incomplete-{}-{}, {}, '.format(code, month_start, val))

        return False

    def get_price_daily(self, code, start: str, end: str, refresh=0):
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
        :param refresh:
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_DAILY_PRICE.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, EQUITY_DAILY_PRICE_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = self.gen_keys_monthly(tstart, tend, list_date, delist_date)

        out = {}
        for dd in vdates:
            dtkey = dd.strftime(DATE_FORMAT)
            if refresh == 0 or refresh == 1:
                val = db.load(dtkey)
                if val is not None:
                    if refresh == 1:
                        # price_data integrity check.
                        if self.integrity_check_monthly(code, dd, val, astype):
                            out[dtkey] = val
                            continue
                    else:
                        out[dtkey] = val
                        continue

            start_raw = dd.strftime(DATE_FORMAT)
            end_raw = pd.Timestamp(year=dd.year, month=dd.month, day=dd.days_in_month).strftime(DATE_FORMAT)
            data = ts.pro_bar(tscode, asset=astype, start_date=start_raw, end_date=end_raw, freq='D')
            if data is not None:
                data = data.rename(columns={'vol': 'volume'})
            out[dtkey] = db.save(dtkey, data)

        all_out = pd.concat(out)
        all_out = all_out.set_index('trade_date', drop=True)
        all_out.index = pd.to_datetime(all_out.index, format=DATE_FORMAT)
        all_out = all_out.sort_index(ascending=True)
        all_out = all_out[(all_out.index >= tstart) & (all_out.index <= tend)]
        return all_out

    def get_stock_daily_info(self, code, start, end, refresh=0):
        """
        Get stock daily information.
        :param code:
        :param start:
        :param end:
        :param refresh:
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_DAILY_INFO.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_DAILY_INFO_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = self.gen_keys_monthly(tstart, tend, list_date, delist_date)

        fcols = STOCK_DAILY_INFO_META['columns']
        out = {}
        for dd in vdates:
            dtkey = dd.strftime(DATE_FORMAT)
            if refresh == 0 or refresh == 1:
                val = db.load(dtkey)
                if val is not None:
                    if refresh == 1:
                        # price_data integrity check.
                        if self.integrity_check_monthly(code, dd, val, astype):
                            out[dtkey] = val
                            continue
                    else:
                        out[dtkey] = val
                        continue

            start_raw = dd.strftime(DATE_FORMAT)
            end_raw = pd.Timestamp(year=dd.year, month=dd.month, day=dd.days_in_month).strftime(DATE_FORMAT)
            data = self.pro_api.daily_basic(ts_code=tscode, start_date=start_raw, end_date=end_raw,
                                            fields=fcols)
            out[dtkey] = db.save(dtkey, data)

        all_out = pd.concat(out)
        all_out = all_out.set_index('trade_date', drop=True)
        all_out.index = pd.to_datetime(all_out.index, format=DATE_FORMAT)
        all_out = all_out.sort_index(ascending=True)
        all_out = all_out[(all_out.index >= tstart) & (all_out.index <= tend)]
        return all_out

    def get_stock_adjfactor(self, code, start: str, end: str, refresh=0):
        """
        按月存取股票的日线数据
        前复权:
            当日收盘价 × 当日复权因子 / 最新复权因子
        后复权:
            当日收盘价 × 当日复权因子
        :param code:
        :param start:
        :param end:
        :param refresh:
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_ADJFACTOR.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_ADJFACTOR_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = self.gen_keys_monthly(tstart, tend, list_date, delist_date)

        fcols = STOCK_ADJFACTOR_META['columns']
        out = {}
        for dd in vdates:
            dtkey = dd.strftime(DATE_FORMAT)
            if refresh == 0 or refresh == 1:
                val = db.load(dtkey)
                if val is not None:
                    if refresh == 1:
                        # price_data integrity check.
                        if self.integrity_check_monthly(code, dd, val, astype):
                            out[dtkey] = val
                            continue
                    else:
                        out[dtkey] = val
                        continue

            start_raw = dd.strftime(DATE_FORMAT)
            end_raw = pd.Timestamp(year=dd.year, month=dd.month, day=dd.days_in_month).strftime(DATE_FORMAT)
            data = self.pro_api.adj_factor(ts_code=tscode, start_date=start_raw, end_date=end_raw, fields=fcols)
            out[dtkey] = db.save(dtkey, data)

        all_out = pd.concat(out)
        all_out = all_out.set_index('trade_date', drop=True)
        all_out.index = pd.to_datetime(all_out.index, format=DATE_FORMAT)
        all_out = all_out.sort_index(ascending=True)
        all_out = all_out[(all_out.index >= tstart) & (all_out.index <= tend)]
        return all_out

    def get_price_minute(self, code, start, end, refresh=0):
        """
        按日存取股票的分钟线数据
        1. 如当日停牌无交易，则存入空数据
        2. 股票未上市，或已退市，则对应日键值不存在
        3. 当日有交易，则存储交易日的数据
        4. 如交易日键值不存在，但股票状态是正常上市，则该月数据需要下载
        5. refresh两种模式，1: 一种是只刷新末月数据，2: 另一种是刷新start-end所有数据
        注： tushare每天有241个分钟数据，包含9:30集合竞价数据
        交易日键值对应的分钟价格数据完整性检查：
            1. 股票， 要么数据完整241条数据，要么为空
            2. 指数和基金，无停牌，因此数据完整。
        :param code:
        :param start:
        :param end:
        :param refresh:
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_DAILY_PRICE.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, EQUITY_MINUTE_PRICE_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = self.gen_keys_daily(tstart, tend, list_date, delist_date)

        out = {}
        for dd in vdates:
            dtkey = dd.strftime(DATE_FORMAT)
            if refresh == 0 or refresh == 1:
                # print(dd)
                val = db.load(dtkey)
                if val is not None:
                    if len(val) == 241:  # 数据完整
                        out[dtkey] = val
                        continue

            start_raw = dd.strftime(DATETIME_FORMAT)
            end_raw = (dd + pd.Timedelta(hours=17)).strftime(DATETIME_FORMAT)
            # fcols = EQUITY_MINUTE_PRICE_META['columns']
            data = ts.pro_bar(tscode, asset=astype, start_date=start_raw, end_date=end_raw, freq='1min')
            if data is not None:
                data = data.rename(columns={'vol': 'volume'})
            out[dtkey] = db.save(dtkey, data)

        all_out = pd.concat(out)
        all_out = all_out.set_index('trade_time', drop=True)
        all_out.index = pd.to_datetime(all_out.index, format=DATETIME_FORMAT)
        all_out = all_out.sort_index(ascending=True)

        if (len(all_out) % 241) != 0:
            # Very slow
            # print('unaligned:{}:-{}'.format(code,  len(all_out)))
            # all_min_idx = self.trade_cal_index_minutes
            # tt_idx = all_min_idx[(all_min_idx >= tstart) & (all_min_idx <= (tend + pd.Timedelta(days=1)))]
            tt_idx = session_day_to_min_tus([dd], '1Min')
            all_out = all_out.reindex(index=tt_idx)
            all_out.index.name = 'trade_time'
            # print('::{}'.format(len(all_out)))

        return all_out

    def get_stock_suspend(self, code, refresh=False):
        """
        股票停复牌信息
        注： 股票存在停牌半天的情况。但也会在suspend列表中体现
        :param code:
        :param refresh:
        :return:
        """
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_SUSPEND.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, STOCK_SUSPEND_META)

        if not refresh:
            val = db.load(code)
            if val is not None:
                return val

        tscode = symbol_std_to_tus(code)

        info = self.pro_api.suspend(ts_code=tscode)
        out = db.save(code, info)
        return out

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
        info = self.pro_api.dividend(ts_code=tscode)
        # fcols = STOCK_XDXR_META['columns']
        # info_to_db = info_to_db.iloc[::-1]
        out = db.save(code, info)
        return out
