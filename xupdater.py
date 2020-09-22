from .apiwrapper import api_call
from .proloader import TusNetLoader
from .layout import *
from .utils.xcutils import *
# from .xcdb.xcdb import *
# from .domain import XcDomain
from .xcdb.zlmdb import *
from functools import partial
from .rdbasic import XcReaderBasic
from .rdprice import XcReaderPrice
from .utils.memoize import lazyval
from .proloader import netloader_init, TusNetLoader

log = logbook.Logger('tupd')


class XcDBUpdater(XcReaderBasic, XcReaderPrice):
    """
    rollback: rollback data units to do integrity check when updating
    """

    master_db = None

    @lazyval
    def netloader(self) -> TusNetLoader:
        return netloader_init()

    def __init__(self, last_day=None, dbtype=DBTYPE.DB_LMDB):
        """
        :param last_day: Tushare last date with data available,
                            we assume yesterday's data is available in today.
        """
        if dbtype == DBTYPE.DB_LMDB:
            self.master_db = XcLMDB(LMDB_NAME, readonly=False)
            self.acc = XcLMDBAccessor
            self.facc = partial(XcLMDBAccessor, self.master_db)

            # , write_buffer_size = 0x400000, block_size = 0x4000,
            # max_file_size = 0x1000000, lru_cache_size = 0x100000, bloom_filter_bits = 0
        else:
            # self.master_db = XcLevelDB(LEVELDB_NAME, readonly=False)
            # self.acc = XcLevelDBAccessor
            # self.facc = partial(XcLevelDBAccessor, self.master_db)
            pass

        if last_day is None:
            """
            Last date always point to the end of Today. but tushare data may not exist at this time.
            """
            self.xctus_last_day = pd.Timestamp.today().normalize() + pd.Timedelta(days=1)
        else:
            self.xctus_last_day = last_day

        self.xctus_first_day = pd.Timestamp('20080101')

        log.info('Updater: date range:{}-{}'.format(self.xctus_first_day, self.xctus_last_day))

        super(XcDBUpdater, self).__init__()

    def init_domain(self):
        super(XcDBUpdater, self).init_domain()
        self.suspend_info = self.get_suspend_d(self.xctus_first_day, self.xctus_last_day)

    def update_domain(self, force_mode=False):
        super(XcDBUpdater, self).update_domain(force_mode)
        self.suspend_info = self.get_suspend_d(self.xctus_first_day, self.xctus_last_day)

    def update_suspend_d(self, start, end):
        """
        :param start:
        :param end:
        :return:
        """
        mmdts = self.gen_keys_daily(start, end, None, None)
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

    def update_price_daily(self, code, start, end, astype, rollback=3):
        """

        :param code:
        :param start:
        :param end:
        :param flag:
        :return:
        """
        if astype is None:
            astype = self.asset_type(code)
        mmdts = self.gen_keys_monthly(start, end, code, astype)
        if mmdts is None:
            return 0

        db = self.facc(TusSdbs.SDB_DAILY_PRICE.value + code, EQUITY_DAILY_PRICE_META)
        bvalid = np.full((len(mmdts),), True, dtype=np.bool)

        for n, dd in enumerate(mmdts):
            dtkey = dt64_to_strdt(dd)
            val = db.load(dtkey, raw_mode=True)
            if val is not None:
                bvalid[n] = True
                if n >= len(mmdts) - rollback:
                    bvalid[n] = self.integrity_check_km_vday(dd, val[:, 4], code)
            else:
                bvalid[n] = False
            # TODO: 数据缓存中最后一个数据，也应进行完整性检查。
        db.commit()
        count = np.sum(~bvalid)

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
                dayindex = self.gen_dindex_monthly(tt, tt)
                xxd = xxd.reindex(index=dayindex)
                db.save(dtkey, xxd)

        return count

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

        if astype is None:
            astype = self.asset_type(code)
        mmdts = self.gen_keys_daily(start, end, code, astype)
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
                    bvalid[n] = self.integrity_check_kd_vmin(dd, val[:, 4], freq=freq, code=code)
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
                minindex = self.gen_mindex_daily(tt, tt, freq)
                xxd = xxd.reindex(index=minindex)
                if (xxd.volume == 0.0).all():
                    # 如果全天无交易，vol == 0, 则清空df.
                    xxd.loc[:, :] = np.nan
                db.save(dtkey, xxd)

        return count

    def update_stock_adjfactor(self, code, start, end, rollback=3):
        """

        :param code:
        :param start:
        :param end:
        :return:
        """
        mmdts = self.gen_keys_monthly(start, end, code, 'E')
        if mmdts is None:
            return 0

        bvalid = np.full((len(mmdts),), True, dtype=np.bool)
        db = self.facc((TusSdbs.SDB_STOCK_ADJFACTOR.value + code), STOCK_ADJFACTOR_META)

        for n, dd in enumerate(mmdts):
            dtkey = dt64_to_strdt(dd)
            val = db.load(dtkey, raw_mode=True)
            if val is not None:
                bvalid[n] = True
                if n >= len(mmdts) - rollback:
                    bvalid[n] = self.integrity_check_km_vday(dd, val[:, 0], code)
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
            dts_upd = mmdts[tstart: tend + 1]
            data = self.netloader.set_stock_adjfactor(code, MONTH_START(dts_upd[0]), MONTH_END(dts_upd[-1]))
            for tt in dts_upd:
                dtkey = dt64_to_strdt(tt)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                xxd = xxd.set_index('trade_date', drop=True)
                xxd.index = pd.to_datetime(xxd.index, format=DATE_FORMAT)
                dayindex = self.gen_dindex_monthly(tt, tt)
                xxd = xxd.reindex(index=dayindex)
                db.save(dtkey, xxd)
        return count

    def update_stock_dayinfo(self, code, start, end, rollback=3):
        """

        :param code:
        :param start:
        :param end:
        :return:
        """
        mmdts = self.gen_keys_monthly(start, end, code, 'E')
        if mmdts is None:
            return 0

        db = self.facc((TusSdbs.SDB_STOCK_DAILY_INFO.value + code), STOCK_DAILY_INFO_META)
        bvalid = np.full((len(mmdts),), True, dtype=np.bool)

        for n, dd in enumerate(mmdts):
            dtkey = dt64_to_strdt(dd)
            val = db.load(dtkey, raw_mode=True)
            if val is not None:
                bvalid[n] = True
                if n >= len(mmdts) - rollback:
                    bvalid[n] = self.integrity_check_km_vday(dd, val[:, 0], code)
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
            dts_upd = mmdts[tstart: tend + 1]
            data = self.netloader.set_stock_daily_info(code, MONTH_START(dts_upd[0]), MONTH_END(dts_upd[-1]))
            for tt in dts_upd:
                dtkey = dt64_to_strdt(tt)
                xxd = data.loc[data['trade_date'].map(lambda x: x[:6] == dtkey[:6]), :]
                xxd = xxd.set_index('trade_date', drop=True)
                xxd.index = pd.to_datetime(xxd.index, format=DATE_FORMAT)
                dayindex = self.gen_dindex_monthly(tt, tt)
                xxd = xxd.reindex(index=dayindex)
                db.save(dtkey, xxd)
        return count


g_updater: XcDBUpdater = None


def tusupdater_init() -> XcDBUpdater:
    global g_updater
    if g_updater is None:
        g_updater = XcDBUpdater()
    return g_updater
