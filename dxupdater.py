from .apiwrapper import api_call
from .proloader import TusNetLoader
from .schema import *
from .utils.xcutils import *
from .xcdb.xcdb import *
from .domain import XcDomain

log = logbook.Logger('tupd')


class XcUpdaterPrice(object):
    """
    rollback: rollback data units to do integrity check when updating
    """
    netloader: TusNetLoader = None
    master_db = None
    domain: XcDomain = None

    @api_call
    def tusbooster_updater_init(self):
        domain = self.domain
        domain.suspend_info = self.get_suspend_d(self.xctus_first_day, self.xctus_current_day)

    @api_call
    def update_suspend_d(self, start, end):
        """
        :param start:
        :param end:
        :return:
        """
        domain = self.domain
        mmdts = domain.gen_keys_daily(start, end, None, None)
        if mmdts is None:
            return

        bvalid = np.full((len(mmdts),), True, dtype=np.bool)
        db = self.facc(TusSdbs.SDB_SUSPEND_D.value, SUSPEND_D_META)

        for n, dd in enumerate(mmdts):
            dtkey = dt64_to_strdt(dd)
            val = db.load(dtkey, raw_mode=True)
            if val is not None:
                bvalid[n] = True  # update missed month data.
            else:
                bvalid[n] = False

        for n, dd in enumerate(mmdts):
            if not bvalid[n]:
                data = self.netloader.set_suspend_d(dd)
                dtkey = dt64_to_strdt(dd)
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
        domain = self.domain
        if astype is None:
            astype = domain.asset_type(code)
        mmdts = domain.gen_keys_monthly(start, end, code, astype)
        if mmdts is None:
            return

        db = self.facc(TusSdbs.SDB_DAILY_PRICE.value + code, EQUITY_DAILY_PRICE_META)
        bvalid = np.full((len(mmdts),), True, dtype=np.bool)

        for n, dd in enumerate(mmdts):
            dtkey = dt64_to_strdt(dd)
            val = db.load(dtkey, raw_mode=True)
            if val is not None:
                bvalid[n] = True
                if n >= len(mmdts) - rollback:
                    bvalid[n] = domain.integrity_check_km_vday(dd, val[:, 4], code)
            else:
                bvalid[n] = False
            # TODO: 数据缓存中最后一个数据，也应进行完整性检查。

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
            dts_upd = mmdts[tstart: tend + 1]
            data = self.netloader.set_price_daily(code, MONTH_START(dts_upd[0]), MONTH_END(dts_upd[-1]), astype)
            for tt in dts_upd:
                dtkey = dt64_to_strdt(tt)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                xxd = xxd.set_index('trade_date', drop=True)
                xxd.index = pd.to_datetime(xxd.index, format=DATE_FORMAT)
                dayindex = domain.gen_dindex_monthly(tt, tt)
                xxd = xxd.reindex(index=dayindex)
                db.save(dtkey, xxd)

        return count

    @api_call
    def update_price_minute(self, code, start, end, freq='1min', astype='E', rollback=10):
        """
        :param code:
        :param start:
        :param end:
        :param freq:
        :param astype:
        :param rollback:
        :return:
        """
        if freq not in XTUS_FREQS:
            return None

        domain = self.domain
        if astype is None:
            astype = domain.asset_type(code)
        mmdts = domain.gen_keys_daily(start, end, code, astype)
        if mmdts is None:
            return

        bvalid = np.full((len(mmdts),), True, dtype=np.bool)
        db = self.facc((TusSdbs.SDB_MINUTE_PRICE.value + code + freq), EQUITY_MINUTE_PRICE_META)

        for n, dd in enumerate(mmdts):
            dtkey = dt64_to_strdt(dd)
            val = db.load(dtkey, raw_mode=True)
            if val is not None:
                bvalid[n] = True
                if n >= len(mmdts) - rollback:
                    bvalid[n] = domain.integrity_check_kd_vmin(dd, val[:, 4], freq=freq, code=code)
            else:
                bvalid[n] = False

        count = np.sum(~bvalid)
        db.commit()
        db = self.facc((TusSdbs.SDB_MINUTE_PRICE.value + code + freq), EQUITY_MINUTE_PRICE_META)

        # 每次最大获取8000条记录
        cc = {'1min': 1, '5min': 5, '15min': 15, '30min': 30, '60min': 60}
        max_units = 6000 // (240 // cc[freq] + 1)
        need_update = nadata_iter(bvalid, max_units)
        while True:
            tstart, tend = next(need_update)
            if tstart is None:
                break
            dts_upd = mmdts[tstart: tend + 1]
            data = self.netloader.set_price_minute(code, dts_upd[0], dts_upd[-1], freq)
            if data is None:
                continue
            for tt in dts_upd:
                dtkey = dt64_to_strdt(tt)
                xxd = data.loc[data['trade_time'].map(lambda x: x[:8] == dtkey[:8]), :]
                xxd = xxd.set_index('trade_time', drop=True)
                xxd.index = pd.to_datetime(xxd.index, format=DATETIME_FORMAT)
                minindex = domain.gen_mindex_daily(tt, tt, freq)
                xxd = xxd.reindex(index=minindex)
                if (xxd.volume == 0.0).all():
                    # 如果全天无交易，vol == 0, 则清空df.
                    xxd.loc[:, :] = np.nan
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
        vdates = gen_keys_monthly(tstart, tend, self.asset_lifetime(code, 'E'), self.trade_cal)
        if len(vdates) == 0:
            return 0

        bvalid = np.full((len(vdates),), True, dtype=np.bool)
        db = self.facc((TusSdbs.SDB_STOCK_ADJFACTOR.value + code), STOCK_ADJFACTOR_META)

        for n, dd in enumerate(vdates):
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey, raw_mode=True)
            if val is not None:
                bvalid[n] = True
                if n >= len(vdates) - rollback:
                    bvalid[n] = integrity_check_km_vday(dd, val[:, 0], self.trade_cal,
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
            for tt in dts_upd:
                dtkey = tt.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                xxd = xxd.set_index('trade_date', drop=True)
                xxd.index = pd.to_datetime(xxd.index, format=DATE_FORMAT)
                dayindex = gen_dayindex_monthly(tt, self.trade_cal)
                xxd = xxd.reindex(index=dayindex)
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
        vdates = gen_keys_monthly(tstart, tend, self.asset_lifetime(code, 'E'), self.trade_cal)
        if len(vdates) == 0:
            return 0

        db = self.facc((TusSdbs.SDB_STOCK_DAILY_INFO.value + code), STOCK_DAILY_INFO_META)
        bvalid = np.full((len(vdates),), True, dtype=np.bool)

        for n, dd in enumerate(vdates):
            dtkey = dd.strftime(DATE_FORMAT)
            val = db.load(dtkey, raw_mode=True)
            if val is not None:
                bvalid[n] = True
                if n >= len(vdates) - rollback:
                    bvalid[n] = integrity_check_km_vday(dd, val[:, 0], self.trade_cal,
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
            for tt in dts_upd:
                dtkey = tt.strftime(DATE_FORMAT)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                xxd = xxd.set_index('trade_date', drop=True)
                xxd.index = pd.to_datetime(xxd.index, format=DATE_FORMAT)
                dayindex = gen_dayindex_monthly(tt, self.trade_cal)
                xxd = xxd.reindex(index=dayindex)
                db.save(dtkey, xxd)
        return count
