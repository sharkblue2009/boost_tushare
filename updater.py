from .proloader import TusNetLoader
from cntus.xcdb.xcdb import *
from .schema import *
from .utils.xcutils import *
import numpy as np
import pandas as pd
import logbook

log = logbook.Logger('tupd')


class XcUpdaterPrice(object):
    netloader: TusNetLoader = None
    master_db = None
    suspend_info = None
    trade_cal_index: pd.DatetimeIndex = None
    xctus_last_date: None

    def _integrity_check_km_vday(self, code, dt, dtval):
        """
        monthly key with daily data. use suspend information to check if the data is integrate
        :param code: ts code
        :param dt: list of date keys
        :param dtval: list of values
        :return:
        """
        if dtval is None:
            return False

        trd_cal = self.trade_cal_index
        susp_info = self.suspend_info.loc[pd.IndexSlice[:, code]]

        trd_days = trd_cal[(trd_cal >= MONTH_START(dt)) & (trd_cal <= MONTH_END(dt))]
        susp = susp_info[(susp_info >= MONTH_START(dt)) & (susp_info <= MONTH_END(dt))]
        susp = susp.loc[(susp['suspend_type'] == 'S') & (susp['suspend_timing'].isna()), :]
        if len(trd_days) <= len(dtval) + len(susp):
            # 股票存在停牌半天的情况，也会被计入suspend列表
            bvalid = True
        else:
            bvalid = False
            log.info('[!KMVDAY]: {}-{}:: {}-{}-{} '.format(code, dt, len(trd_days), len(susp), len(dtval)))

        return bvalid

    def _integrity_check_kd_vday(self, dt, dtval):
        """
        daily key with daily data.
        :param code: ts code
        :param dtkeys: list of date keys
        :param dtvals: list of values
        :return:
        """
        if dtval is None:
            return False

        tcal_idx = self.trade_cal_index
        if dt in tcal_idx:
            return True

        log.info('[!KDVDAY]: {}'.format(dt))
        return False

    def _integrity_check_kd_vmin(self, code, dt, dtval, freq='1min'):
        """
        daily key with minute data, use suspend information to judge if the data should exist,
        :param code: ts code
        :param dt: date keys
        :param dtval:  values
        :return:
        """
        if dtval is None:
            return False

        trd_cal = self.trade_cal_index
        susp_info = self.suspend_info.loc[pd.IndexSlice[:, code]]

        cc = {'1min': 241, '5min': 49, '15min': 17, '30min': 9, '60min': 5, '120min': 3}
        nbars = cc[freq]

        b_vld = False

        if dt in trd_cal:
            susp = susp_info.loc[(susp_info['suspend_type'] == 'S') & (susp_info.index == dt), :]
            data = dtval
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
        else:
            log.warning('[!KDVMIN]-Wrong data:{}-{}'.format(code, dt))

        if not b_vld:
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
        db = self.facc( TusSdbs.SDB_SUSPEND_D.value, SUSPEND_D_META)

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
        :param mode: -1: erase, 0: update from last valid, 1: update all
        :return:
        """
        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_monthly(tstart, tend, self.asset_lifetime(code, astype), self.xctus_last_date)
        if len(vdates) == 0:
            return 0

        # Dummy read here.
        # trd_cal = self.trade_cal_index
        # susp_info = self.suspend_info.loc[pd.IndexSlice[:, code]]

        db = self.facc( TusSdbs.SDB_DAILY_PRICE.value + code, EQUITY_DAILY_PRICE_META)
        bvalid = np.full((len(vdates),), True, dtype=np.bool)

        if flag == IOFLAG.UPDATE_MISS:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                if val is not None:
                    bvalid[n] = True
                else:
                    bvalid[n] = False
        elif flag == IOFLAG.UPDATE_INVALID:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                bvalid[n] = self._integrity_check_km_vday(code, dd, val)
        elif flag == IOFLAG.UPDATE_ALL:
            bvalid[:] = False

        count = np.sum(~bvalid)

        db.commit()
        # Reopen Accessor
        db = self.facc( TusSdbs.SDB_DAILY_PRICE.value + code, EQUITY_DAILY_PRICE_META)
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
        :param mode: -1: erase, 0: update from last valid, 1: update all
        :return:
        """
        if freq not in ['1min', '5min', '15min', '30min', '60min', '120m']:
            return None

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_daily(tstart, tend, self.asset_lifetime(code, astype),
                                self.trade_cal_index, self.xctus_last_date)

        bvalid = np.full((len(vdates),), True, dtype=np.bool)
        db = self.facc( (TusSdbs.SDB_MINUTE_PRICE.value + code + freq),
                        EQUITY_MINUTE_PRICE_META, readonly=True)

        if flag == IOFLAG.UPDATE_MISS:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                if val is not None:
                    bvalid[n] = True
                else:
                    bvalid[n] = False
        elif flag == IOFLAG.UPDATE_INVALID:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                bvalid[n] = self._integrity_check_kd_vmin(code, dd, val)
        elif flag == IOFLAG.UPDATE_ALL:
            bvalid[:] = False

        count = np.sum(~bvalid)
        db.commit()
        db = self.facc( (TusSdbs.SDB_MINUTE_PRICE.value + code + freq),
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
        vdates = gen_keys_monthly(tstart, tend, self.asset_lifetime(code, 'E'), self.xctus_last_date)
        if len(vdates) == 0:
            return 0

        bvalid = np.full((len(vdates),), True, dtype=np.bool)
        db = self.facc( (TusSdbs.SDB_STOCK_ADJFACTOR.value + code),
                        STOCK_ADJFACTOR_META)

        if flag == IOFLAG.UPDATE_MISS:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                if val is not None:
                    bvalid[n] = True
                else:
                    bvalid[n] = False
        elif flag == IOFLAG.UPDATE_INVALID:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                bvalid[n] = self._integrity_check_km_vday(code, dd, val)
        elif flag == IOFLAG.UPDATE_ALL:
            bvalid[:] = False

        count = np.sum(~bvalid)
        db.commit()
        db = self.facc( (TusSdbs.SDB_STOCK_ADJFACTOR.value + code),
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
        vdates = gen_keys_monthly(tstart, tend, self.asset_lifetime(code, 'E'), self.xctus_last_date)
        if len(vdates) == 0:
            return 0

        db = self.facc( (TusSdbs.SDB_STOCK_DAILY_INFO.value + code),
                        STOCK_DAILY_INFO_META)
        bvalid = np.full((len(vdates),), True, dtype=np.bool)

        if flag == IOFLAG.UPDATE_MISS:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                if val is not None:
                    bvalid[n] = True
                else:
                    bvalid[n] = False
        elif flag == IOFLAG.UPDATE_INVALID:
            for n, dd in enumerate(vdates):
                dtkey = dd.strftime(DATE_FORMAT)
                val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
                bvalid[n] = self._integrity_check_km_vday(code, dd, val)
        elif flag == IOFLAG.UPDATE_ALL:
            bvalid[:] = False

        count = np.sum(~bvalid)
        db.commit()
        db = self.facc( (TusSdbs.SDB_STOCK_DAILY_INFO.value + code),
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
