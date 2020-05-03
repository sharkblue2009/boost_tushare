from .proloader import TusNetLoader
from cntus.xcdb.xcdb import *
from .schema import *
from .utils.xcutils import *
import numpy as np
import pandas as pd
import logbook

log = logbook.Logger('tupd')


class XcUpdaterPrice(object):
    """
    rollback: rollback data units to do integrity check when updating
    """
    netloader: TusNetLoader = None
    master_db = None
    suspend_info = None
    trade_cal_index: pd.DatetimeIndex = None

    rollback = 3

    @staticmethod
    def _integrity_check_km_vday(dt, dtval, trade_days, susp_info=None, code=None):
        """
        monthly key with daily data. use suspend information to check if the data is integrate
        :param dt: date keys
        :param dtval: values
        :return:
        """
        if dtval is None:
            return False

        # trade_days = self.trade_cal_index
        # susp_info = self.suspend_info.loc[pd.IndexSlice[:, code]]

        trdays = trade_days[(trade_days >= MONTH_START(dt)) & (trade_days <= MONTH_END(dt))]
        if susp_info is None:
            expect_size = len(trdays)
        else:
            susp = susp_info[(susp_info.index >= MONTH_START(dt)) & (susp_info.index <= MONTH_END(dt))]
            susp = susp.loc[(susp['suspend_type'] == 'S') & (susp['suspend_timing'].isna()), :]
            expect_size = len(trdays) - len(susp)

        if expect_size <= len(dtval):
            # 股票存在停牌半天的情况，也会被计入suspend列表
            bvalid = True
        else:
            bvalid = False
            if susp_info is not None:
                log.info('[!KMVDAY]:{}-{}:: t{}-v{}-s{} '.format(code, dt, len(trdays), len(susp), len(dtval)))

        return bvalid

    @staticmethod
    def _integrity_check_kd_vday(dt, dtval, trade_days):
        """
        daily key with daily data.
        :param code: ts code
        :param dtkeys: list of date keys
        :param dtvals: list of values
        :return:
        """
        if dtval is None:
            return False

        if dt in trade_days:
            return True

        log.info('[!KDVDAY]: {}'.format(dt))
        return False

    @staticmethod
    def _integrity_check_kd_vmin(dt, dtval, trade_days, susp_info=None, freq='1min', code=None):
        """
        daily key with minute data, use suspend information to judge if the data should exist,
        :param dt: date keys
        :param dtval:  values
        :return:
        """
        if dtval is None:
            return False

        # trdays = self.trade_cal_index
        # susp_info = self.suspend_info.loc[pd.IndexSlice[:, code]]

        cc = {'1min': 241, '5min': 49, '15min': 17, '30min': 9, '60min': 5, '120min': 3}
        nbars = cc[freq]

        b_vld = False

        if dt in trade_days:
            data = dtval
            if susp_info is None:
                if len(data) == nbars:
                    b_vld = True
            else:
                susp = susp_info.loc[(susp_info['suspend_type'] == 'S') & (susp_info.index == dt), :]
                if not susp.empty:
                    if susp['suspend_timing'].isna():  # .iloc[-1]
                        # 当日全天停牌
                        if len(data) == nbars or len(data) == 0:
                            b_vld = True
                    else:
                        # 部分时间停牌
                        if len(data) < nbars:
                            b_vld = True
                else:
                    if len(data) == nbars:
                        b_vld = True

            if not b_vld and susp_info is not None:
                log.info('[!KDVMIN]-: {}-{}:: {}-{} '.format(code, dt, len(susp), len(data)))

        return b_vld

    def update_suspend_d(self, start, end, flag=IOFLAG.UPDATE_MISS):
        """
        :param code:
        :param start:
        :param end:
        :return:
        """
        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = self.trade_cal_index[(self.trade_cal_index >= tstart) & (self.trade_cal_index <= tend)]
        if len(vdates) == 0:
            return 0

        bvalid = np.full((len(vdates),), True, dtype=np.bool)
        db = self.facc(TusSdbs.SDB_SUSPEND_D.value, SUSPEND_D_META)

        if flag == IOFLAG.UPDATE_MISS:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                if val is not None:
                    bvalid[n] = True  # update missed month data.
                else:
                    bvalid[n] = False
        elif flag == IOFLAG.UPDATE_INVALID:
            for n, dd in enumerate(vdates):
                dd = vdates[n]
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                if val is None:
                    bvalid[n] = True
                else:
                    bvalid[n] = False
        elif flag == IOFLAG.UPDATE_ALL:
            bvalid[:] = False

        for n, dd in enumerate(vdates):
            if not bvalid[n]:
                data = self.netloader.set_suspend_d(dd)
                dtkey = dd.strftime(DATE_FORMAT)
                db.save(dtkey, data)
        return np.sum(~bvalid)

    def update_price_daily(self, code, start, end, astype, flag=IOFLAG.UPDATE_MISS):
        """

        :param code:
        :param start:
        :param end:
        :param flag:
        :return:
        """
        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_monthly(tstart, tend, self.asset_lifetime(code, astype), self.trade_cal_index)
        if len(vdates) == 0:
            return 0

        # Dummy read here.
        # trd_cal = self.trade_cal_index
        # susp_info = self.suspend_info.loc[pd.IndexSlice[:, code]]

        db = self.facc(TusSdbs.SDB_DAILY_PRICE.value + code, EQUITY_DAILY_PRICE_META)
        bvalid = np.full((len(vdates),), True, dtype=np.bool)

        if flag == IOFLAG.UPDATE_MISS:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                if val is not None:
                    bvalid[n] = True
                    if n >= len(vdates) - self.rollback:
                        bvalid[n] = self._integrity_check_km_vday(dd, val, self.trade_cal_index,
                                                                  self.stock_suspend(code), code)
                else:
                    bvalid[n] = False

        elif flag == IOFLAG.UPDATE_INVALID:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                bvalid[n] = self._integrity_check_km_vday(dd, val, self.trade_cal_index,
                                                          self.stock_suspend(code))
        elif flag == IOFLAG.UPDATE_ALL:
            bvalid[:] = False

        count = np.sum(~bvalid)

        db.commit()
        # Reopen Accessor
        db = self.facc(TusSdbs.SDB_DAILY_PRICE.value + code, EQUITY_DAILY_PRICE_META)
        need_update = nadata_iter(bvalid, 50)
        while True:
            tstart, tend = next(need_update)
            if tstart is None:
                break
            dts_upd = vdates[tstart: tend + 1]
            data = self.netloader.set_price_daily(code, MONTH_START(dts_upd[0]), MONTH_END(dts_upd[-1]))
            for xx in dts_upd:
                dtkey = xx.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                db.save(dtkey, xxd)
        return count

    def update_price_minute(self, code, start, end, freq='1min', astype='E', flag=IOFLAG.UPDATE_MISS):
        """
        :param code:
        :param start:
        :param end:
        :param freq:
        :param flag:
        :return:
        """
        if freq not in ['1min', '5min', '15min', '30min', '60min', '120m']:
            return None

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_daily(tstart, tend, self.asset_lifetime(code, astype),
                                self.trade_cal_index)

        bvalid = np.full((len(vdates),), True, dtype=np.bool)
        db = self.facc((TusSdbs.SDB_MINUTE_PRICE.value + code + freq),
                       EQUITY_MINUTE_PRICE_META, readonly=True)

        if flag == IOFLAG.UPDATE_MISS:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                if val is not None:
                    bvalid[n] = True
                    if n >= len(vdates) - self.rollback:
                        bvalid[n] = self._integrity_check_kd_vmin(dd, val, self.trade_cal_index,
                                                                  self.stock_suspend(code), freq=freq, code=code)
                else:
                    bvalid[n] = False
        elif flag == IOFLAG.UPDATE_INVALID:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                bvalid[n] = self._integrity_check_kd_vmin(dd, val, self.stock_suspend(code), freq=freq, code=code)
        elif flag == IOFLAG.UPDATE_ALL:
            bvalid[:] = False

        count = np.sum(~bvalid)
        db.commit()
        db = self.facc((TusSdbs.SDB_MINUTE_PRICE.value + code + freq),
                       EQUITY_MINUTE_PRICE_META)
        need_update = nadata_iter(bvalid, 12)
        while True:
            tstart, tend = next(need_update)
            if tstart is None:
                break
            dts_upd = vdates[tstart: tend + 1]
            data = self.netloader.set_price_minute(code, dts_upd[0], dts_upd[-1], freq)
            for xx in dts_upd:
                dtkey = xx.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_time'].map(lambda x: x[:8] == dtkey[:8]), :]
                db.save(dtkey, xxd)

        return count

    def update_stock_adjfactor(self, code, start, end, flag=IOFLAG.UPDATE_MISS):
        """

        :param code:
        :param start:
        :param end:
        :return:
        """
        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_monthly(tstart, tend, self.asset_lifetime(code, 'E'), self.trade_cal_index)
        if len(vdates) == 0:
            return 0

        bvalid = np.full((len(vdates),), True, dtype=np.bool)
        db = self.facc((TusSdbs.SDB_STOCK_ADJFACTOR.value + code),
                       STOCK_ADJFACTOR_META)

        if flag == IOFLAG.UPDATE_MISS:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                if val is not None:
                    bvalid[n] = True
                    if n == len(vdates) - self.rollback:
                        bvalid[n] = self._integrity_check_km_vday(dd, val, self.trade_cal_index,
                                                                  self.stock_suspend(code), code)
                else:
                    bvalid[n] = False
        elif flag == IOFLAG.UPDATE_INVALID:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                bvalid[n] = self._integrity_check_km_vday(dd, val, self.trade_cal_index,
                                                          self.stock_suspend(code))
        elif flag == IOFLAG.UPDATE_ALL:
            bvalid[:] = False

        count = np.sum(~bvalid)
        db.commit()
        db = self.facc((TusSdbs.SDB_STOCK_ADJFACTOR.value + code),
                       STOCK_ADJFACTOR_META)
        need_update = nadata_iter(bvalid, 50)
        while True:
            tstart, tend = next(need_update)
            if tstart is None:
                break
            dts_upd = vdates[tstart: tend + 1]
            data = self.netloader.set_stock_adjfactor(code, MONTH_START(dts_upd[0]), MONTH_END(dts_upd[-1]))
            for xx in dts_upd:
                dtkey = xx.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                db.save(dtkey, xxd)
        return count

    def update_stock_dayinfo(self, code, start, end, flag=IOFLAG.UPDATE_MISS):
        """

        :param code:
        :param start:
        :param end:
        :return:
        """
        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_monthly(tstart, tend, self.asset_lifetime(code, 'E'), self.trade_cal_index)
        if len(vdates) == 0:
            return 0

        db = self.facc((TusSdbs.SDB_STOCK_DAILY_INFO.value + code),
                       STOCK_DAILY_INFO_META)
        bvalid = np.full((len(vdates),), True, dtype=np.bool)

        if flag == IOFLAG.UPDATE_MISS:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                if val is not None:
                    bvalid[n] = True
                    if n == len(vdates) - self.rollback:
                        bvalid[n] = self._integrity_check_km_vday(dd, val, self.trade_cal_index,
                                                                  self.stock_suspend(code), code)
                else:
                    bvalid[n] = False
        elif flag == IOFLAG.UPDATE_INVALID:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                bvalid[n] = self._integrity_check_km_vday(dd, val, self.trade_cal_index,
                                                          self.stock_suspend(code))
        elif flag == IOFLAG.UPDATE_ALL:
            bvalid[:] = False

        count = np.sum(~bvalid)
        db.commit()
        db = self.facc((TusSdbs.SDB_STOCK_DAILY_INFO.value + code),
                       STOCK_DAILY_INFO_META)
        need_update = nadata_iter(bvalid, 50)
        while True:
            tstart, tend = next(need_update)
            if tstart is None:
                break
            dts_upd = vdates[tstart: tend + 1]
            data = self.netloader.set_stock_daily_info(code, MONTH_START(dts_upd[0]), MONTH_END(dts_upd[-1]))
            for xx in dts_upd:
                dtkey = xx.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                db.save(dtkey, xxd)
        return count
