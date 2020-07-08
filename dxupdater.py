from .proloader import TusNetLoader
from .xcdb.xcdb import *
from .schema import *
from .utils.xcbstutils import *
from .apiwrapper import api_call
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

    @api_call
    def update_suspend_d(self, start, end):
        """
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

        for n, dd in enumerate(vdates):
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
            if val is not None:
                bvalid[n] = True  # update missed month data.
            else:
                bvalid[n] = False

        for n, dd in enumerate(vdates):
            if not bvalid[n]:
                data = self.netloader.set_suspend_d(dd)
                dtkey = dd.strftime(DATE_FORMAT)
                db.save(dtkey, data)

        return np.sum(~bvalid)

    @api_call
    def update_price_daily(self, code, start, end, astype, rollback=3):
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

        for n, dd in enumerate(vdates):
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
            if val is not None:
                bvalid[n] = True
                if n >= len(vdates) - rollback:
                    bvalid[n] = integrity_check_km_vday(dd, val, self.trade_cal_index,
                                                        self.stock_suspend(code), code)
            else:
                bvalid[n] = False
            #TODO: 数据缓存中最后一个数据，也应进行完整性检查。

        count = np.sum(~bvalid)

        db.commit()
        # Reopen Accessor
        db = self.facc(TusSdbs.SDB_DAILY_PRICE.value + code, EQUITY_DAILY_PRICE_META)

        # 每次最大获取5000条记录
        max_units = 4700 // 23
        need_update = nadata_iter(bvalid, max_units)
        while True:
            tstart, tend = next(need_update)
            if tstart is None:
                break
            dts_upd = vdates[tstart: tend + 1]
            data = self.netloader.set_price_daily(code, MONTH_START(dts_upd[0]), MONTH_END(dts_upd[-1]), astype)
            for xx in dts_upd:
                dtkey = xx.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                db.save(dtkey, xxd)

        return count

    @api_call
    def update_price_minute(self, code, start, end, freq='1min', astype='E', rollback=5):
        """
        :param code:
        :param start:
        :param end:
        :param freq:
        :param flag:
        :return:
        """
        if freq not in ['1min', '5min', '15min', '30min', '60min', '120min']:
            return None

        tstart = pd.Timestamp(start)
        tend = pd.Timestamp(end)
        vdates = gen_keys_daily(tstart, tend, self.asset_lifetime(code, astype),
                                self.trade_cal_index)

        bvalid = np.full((len(vdates),), True, dtype=np.bool)
        db = self.facc((TusSdbs.SDB_MINUTE_PRICE.value + code + freq),
                       EQUITY_MINUTE_PRICE_META, readonly=True)

        for n, dd in enumerate(vdates):
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
            if val is not None:
                bvalid[n] = True
                if n >= len(vdates) - rollback:
                    bvalid[n] = integrity_check_kd_vmin(dd, val, self.trade_cal_index,
                                                        self.stock_suspend(code), freq=freq, code=code)
            else:
                bvalid[n] = False

        count = np.sum(~bvalid)
        db.commit()
        db = self.facc((TusSdbs.SDB_MINUTE_PRICE.value + code + freq),
                       EQUITY_MINUTE_PRICE_META)

        # 每次最大获取8000条记录
        cc = {'1min': 1, '5min': 5, '15min': 15, '30min': 30, '60min': 60, '120min': 120}
        max_units = 6000 // (240 // cc[freq] + 1)
        need_update = nadata_iter(bvalid, max_units)
        while True:
            tstart, tend = next(need_update)
            if tstart is None:
                break
            dts_upd = vdates[tstart: tend + 1]
            data = self.netloader.set_price_minute(code, dts_upd[0], dts_upd[-1], freq)
            if data is None:
                continue
            for xx in dts_upd:
                dtkey = xx.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_time'].map(lambda x: x[:8] == dtkey[:8]), :]
                db.save(dtkey, xxd)

        return count

    @api_call
    def update_stock_adjfactor(self, code, start, end, rollback=3):
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

        for n, dd in enumerate(vdates):
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
            if val is not None:
                bvalid[n] = True
                if n >= len(vdates) - rollback:
                    bvalid[n] = integrity_check_km_vday(dd, val, self.trade_cal_index,
                                                        self.stock_suspend(code), code)
            else:
                bvalid[n] = False

        count = np.sum(~bvalid)
        db.commit()
        db = self.facc((TusSdbs.SDB_STOCK_ADJFACTOR.value + code),
                       STOCK_ADJFACTOR_META)
        # 每次最大获取5000条记录
        max_units = 4000 // 23
        need_update = nadata_iter(bvalid, max_units)
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

    @api_call
    def update_stock_dayinfo(self, code, start, end, rollback=3):
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

        for n, dd in enumerate(vdates):
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey, KVTYPE.TPV_NARR_2D)
            if val is not None:
                bvalid[n] = True
                if n >= len(vdates) - rollback:
                    bvalid[n] = integrity_check_km_vday(dd, val, self.trade_cal_index,
                                                        self.stock_suspend(code), code)
            else:
                bvalid[n] = False

        count = np.sum(~bvalid)
        db.commit()
        db = self.facc((TusSdbs.SDB_STOCK_DAILY_INFO.value + code),
                       STOCK_DAILY_INFO_META)
        # 每次最大获取5000条记录
        max_units = 4700 // 23
        need_update = nadata_iter(bvalid, max_units)
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
