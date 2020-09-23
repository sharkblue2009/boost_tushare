from .utils.xcutils import *
from logbook import Logger

log = Logger('xtus')


class XcDomain(object):
    xctus_last_day = None
    xctus_first_day = None

    _cal_raw = None  # Raw trading calendar from Tushare. string format
    _cal_day = None
    _cal_1min = None
    _cal_5min = None
    _cal_map_month = None
    _cal_map_day = None

    index_info = None
    stock_info = None
    fund_info = None
    suspend_info = None

    def __init__(self):
        log.info('Domain init...')

    @property
    def trade_cal_raw(self):
        return self._cal_raw

    @trade_cal_raw.setter
    def trade_cal_raw(self, value):
        self._cal_raw = None
        self._cal_day = None
        self._cal_1min = None
        self._cal_5min = None
        self._cal_map_month = None
        self._cal_map_day = None

    @property
    def trade_cal(self):
        """
        当前有效的交易日历
        :return:
        """
        return self._cal_day

    @property
    def trade_cal_1min(self):
        return self._cal_1min

    @property
    def trade_cal_5min(self):
        return self._cal_5min

    def freq_to_cal(self, freq):
        if freq == '1min':
            return self.trade_cal_1min
        if freq == '5min':
            return self.trade_cal_5min
        return None

    @property
    def tcalmap_mon(self):
        if self._cal_map_month is None:
            tstart = self.xctus_first_day
            tend = self.xctus_last_day
            m_start = pd.Timestamp(year=tstart.year, month=tstart.month, day=1)
            m_end = pd.Timestamp(year=tend.year, month=tend.month, day=tend.days_in_month)
            mdates = pd.date_range(m_start, m_end, freq='MS').values
            self._cal_map_month = pd.DataFrame(index=mdates, columns=['start', 'end'], dtype=np.int64)
            for dd in mdates:
                stt = np.sum(self.trade_cal < dd)
                ett = np.sum(self.trade_cal <= MONTH_END(dd))
                self._cal_map_month.loc[dd, 'start'] = stt
                self._cal_map_month.loc[dd, 'end'] = ett
            self._cal_map_month = self._cal_map_month.astype(np.int64)
        return self._cal_map_month

    @property
    def tcalmap_day(self):
        if self._cal_map_day is None:
            self._cal_map_day = pd.Series(data=np.arange(len(self.trade_cal)), index=self.trade_cal, dtype=np.int64)
        return self._cal_map_day

    def gen_keys_monthly(self, start_dt, end_dt, code=None, astype='E'):
        """
        根据当前交易品种的有效交易日历， 产生月度keys
        :param start_dt:
        :param end_dt:
        :return:
        """
        start_dt = strdt_to_dt64(start_dt)
        end_dt = strdt_to_dt64(end_dt)

        tstart = start_dt
        tend = end_dt

        if code is not None:
            asset_life = self.asset_lifetime(code, astype)
            if asset_life is not None:
                l_ss, l_ee = asset_life
                tstart = max([l_ss, start_dt])
                tend = min([l_ee, end_dt])

        mm_index = self.tcalmap_mon.index.values
        mmdts = mm_index[(mm_index >= MONTH_START(tstart)) & (mm_index <= MONTH_END(tend))]
        if len(mmdts) == 0:
            return None
        return mmdts

    def gen_dindex_monthly(self, start_m, end_m):
        """
        generate dayindex by month
        :param start_m: start month, datetime64
        :param end_m: end month
        :return:
        """
        ssc1, ssc2 = self.tcalmap_mon.loc[start_m], self.tcalmap_mon.loc[end_m]
        alldays = self.trade_cal[ssc1.start: ssc2.end]
        return alldays

    def gen_keys_daily(self, start_dt, end_dt, code, astype='E'):
        """
        根据当前交易品种的有效交易日历， 产生日度keys
        :param start_dt:
        :param end_dt:
        :param asset_life: tuple(born_date, dead_date)
        :param trade_cal: trade_cal index
        :return:
        """
        start_dt = strdt_to_dt64(start_dt)
        end_dt = strdt_to_dt64(end_dt)

        tstart = start_dt
        tend = end_dt

        if code is not None:
            asset_life = self.asset_lifetime(code, astype)
            if asset_life is not None:
                l_ss, l_ee = asset_life
                tstart = max([l_ss, start_dt])
                tend = min([l_ee, end_dt])

        tt_index = self.trade_cal
        ttdates = tt_index[(tt_index >= tstart) & (tt_index <= tend)]
        if len(ttdates) == 0:
            return None
        return ttdates

    def gen_mindex_daily(self, start_d, end_d, freq):
        """
        generate minute index by day
        :param start_d: start day
        :param end_d: end day
        :param freq:
        :return:
        """
        periods = XTUS_FREQ_BARS[freq]
        calmins = self.freq_to_cal(freq)
        ssc1, ssc2 = self.tcalmap_day.loc[start_d], self.tcalmap_day.loc[end_d]
        alldays = calmins[ssc1 * periods: (ssc2 + 1) * periods]
        return alldays

    def gen_keys_quarterly(self, start_dt, end_dt, code=None, astype=None):
        """

        :param start_dt:
        :param end_dt:
        :param asset_life: tuple(born_date, dead_date)
        :param trade_cal:
        :return:
        """
        start_dt = strdt_to_dt64(start_dt)
        end_dt = strdt_to_dt64(end_dt)

        tstart = start_dt
        tend = end_dt

        if code is not None:
            asset_life = self.asset_lifetime(code, astype)
            if asset_life is not None:
                l_ss, l_ee = asset_life
                tstart = max([l_ss, start_dt])
                tend = min([l_ee, end_dt])

        tstart, tend = pd.Timestamp(tstart), pd.Timestamp(tend)
        m_start = pd.Timestamp(year=tstart.year, month=1, day=1)
        m_end = pd.Timestamp(year=tend.year, month=tend.month, day=tend.days_in_month)

        key_index = pd.date_range(m_start, m_end, freq='QS')
        return key_index

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
        if code is None:
            return
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
        start_date = strdt_to_dt64(start_date)
        end_date = strdt_to_dt64(end_date)
        return start_date, end_date

    def integrity_check_km_vday(self, dt, dtval, code=None):
        """
        monthly key with daily data. use suspend information to check if the data is integrate
        :param dt: date keys
        :param dtval:  data values, 1d ndarray, [volume]
        :param trade_days:
        :return:
        """
        if dtval is None:
            return False

        trdays = self.gen_dindex_monthly(dt, dt)
        susp_info = self.stock_suspend(code)
        if susp_info is None:
            expect_size = len(trdays)
            susp = None
        else:
            susp = susp_info[(susp_info.index >= MONTH_START(dt)) & (susp_info.index <= MONTH_END(dt))]
            susp = susp.loc[(susp['suspend_type'] == 'S') & (susp['suspend_timing'].isna()), :]
            expect_size = len(trdays) - len(susp)

        if expect_size <= np.sum(~np.isnan(dtval)):
            # 股票存在停牌半天的情况，也会被计入suspend列表
            bvalid = True
        else:
            bvalid = False
            if susp_info is not None:
                log.info('[!KMVDAY]:{}-{}:: t{}-v{}-s{} '.format(code, dt, len(trdays), len(susp), len(dtval)))

        return bvalid

    def integrity_check_kd_vmin(self, dt, dtval, freq='1min', code=None):
        """
        daily key with minute data, use suspend information to judge if the data should exist,
        :param dt: date keys
        :param dtval:  data values, 1d-ndarray
        :param freq:
        :return:
        """
        if dtval is None:
            return False

        nbars = XTUS_FREQ_BARS[freq]
        susp_info = self.stock_suspend(code)

        bvalid = False
        if True:  # dt in self.trade_cal:
            vldlen = np.sum(~np.isnan(dtval))
            if susp_info is None:
                if vldlen == nbars:
                    bvalid = True
            else:
                susp = susp_info.loc[(susp_info['suspend_type'] == 'S') & (susp_info.index == dt), :]
                if not susp.empty:
                    if susp['suspend_timing'].isna():  # .iloc[-1]
                        # 当日全天停牌
                        if vldlen == 0:
                            bvalid = True
                    else:
                        # 部分时间停牌
                        if nbars > vldlen > 0:
                            bvalid = True
                else:
                    if 0 < vldlen <= nbars:
                        # Fixme, 如果部分时间没有交易，vol=0, vldlen可能小于nbars
                        bvalid = True

            if not bvalid and susp_info is not None:
                log.info('[!KDVMIN]-: {}-{}:: {}-{} '.format(code, dt, vldlen))

        return bvalid

    def stock_suspend(self, code):
        try:
            info = self.suspend_info.loc[pd.IndexSlice[:, code], :]
            info = info.droplevel(1)  # Drop the tscode index
            return info
        except:
            return None
