from .layout import *
from .utils.xcutils import *
from .xcdb.xcdb import *
from .rdbasic import XcReaderBasic
from .rdprice import XcReaderPrice
from .layout import *
from .utils.xcutils import *
from .xcdb.zlmdb import *
from functools import partial


class XcDBChecker(XcReaderBasic, XcReaderPrice):
    master_db = None

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

        self.xctus_first_day = pd.Timestamp('20000101')

        log.info('Updater: date range:{}-{}'.format(self.xctus_first_day, self.xctus_last_day))

        super(XcDBChecker, self).__init__()

    def init_domain(self):
        """
        because updater may working at multi thread env, so need to load info first.
        """
        super(XcDBChecker, self).init_domain()
        # dummy read here to create the tcalmap, or it may fail when parallel thread case.
        aa = self.suspend_info
        aa = self.stock_info
        aa = self.index_info
        aa = self.fund_info
        aa = self.tcalmap_day
        aa = self.tcalmap_mon

    def check_price_daily(self, code, start, end, astype, flag=IOFLAG.ERASE_INVALID):
        if astype is None:
            astype = self.asset_type(code)
        mmdts = self.gen_keys_monthly(start, end, code, astype)
        if mmdts is None:
            return 0

        db = self.facc(TusSdbs.SDB_DAILY_PRICE.value + code, EQUITY_DAILY_PRICE_META)
        bvalid = np.full((len(mmdts),), True, dtype=np.bool)

        if flag == IOFLAG.ERASE_ALL:
            for n, dd in enumerate(mmdts):
                dtkey = dt64_to_strdt(dd)
                db.remove(dtkey)
                bvalid[n] = False
        elif flag == IOFLAG.ERASE_INVALID:
            for n, dd in enumerate(mmdts):
                dtkey = dt64_to_strdt(dd)
                val = db.load(dtkey, raw_mode=True)
                if val is not None:
                    vld1 = self.integrity_check_km_vday(dd, val[:, 4], code)
                    if not vld1:
                        db.remove(dtkey)
                        bvalid[n] = False
        db.commit()
        return np.sum(~bvalid)

    def check_price_minute(self, code, start, end, freq='1min', astype=None, flag=IOFLAG.ERASE_INVALID):
        if freq not in XTUS_FREQS:
            return 0

        if astype is None:
            astype = self.asset_type(code)
        mmdts = self.gen_keys_daily(start, end, code, astype)
        if mmdts is None:
            return 0

        db = self.facc((TusSdbs.SDB_MINUTE_PRICE.value + code + freq),
                       EQUITY_MINUTE_PRICE_META)
        bvalid = np.full((len(mmdts),), True, dtype=np.bool)
        if flag == IOFLAG.ERASE_ALL:
            for n, dd in enumerate(mmdts):
                dtkey = dt64_to_strdt(dd)
                db.remove(dtkey)
                bvalid[n] = False
        else:
            for n, dd in enumerate(mmdts):
                dtkey = dt64_to_strdt(dd)
                val = db.load(dtkey, raw_mode=True)
                if val is not None:
                    bvalid = self.integrity_check_kd_vmin(dd, val[:, 4], freq=freq, code=code)
                    if not bvalid:
                        db.remove(dtkey)
                        bvalid[n] = False
        db.commit()
        return np.sum(~bvalid)

    def check_stock_dayinfo(self, code, start, end, flag=IOFLAG.ERASE_INVALID):
        """

        :param code:
        :param start:
        :param end:
        :return:
        """
        astype = 'E'
        mmdts = self.gen_keys_monthly(start, end, code, astype)
        if mmdts is None:
            return 0

        db = self.facc((TusSdbs.SDB_STOCK_DAILY_INFO.value + code), STOCK_DAILY_INFO_META)
        bvalid = np.full((len(mmdts),), True, dtype=np.bool)
        if flag == IOFLAG.ERASE_ALL:
            for n, dd in enumerate(mmdts):
                dtkey = dt64_to_strdt(dd)
                db.remove(dtkey)
                bvalid[n] = False
        else:
            for n, dd in enumerate(mmdts):
                dtkey = dt64_to_strdt(dd)
                val = db.load(dtkey, raw_mode=True)
                if val is not None:
                    bvalid = self.integrity_check_km_vday(dd, val[:, 0], code)
                    if not bvalid:
                        db.remove(dtkey)
                        bvalid[n] = False
        db.commit()
        return np.sum(~bvalid)

    def check_stock_adjfactor(self, code, start, end, flag=IOFLAG.ERASE_INVALID):
        """

        :param code:
        :param start:
        :param end:
        :return:
        """
        astype = 'E'
        mmdts = self.gen_keys_monthly(start, end, code, astype)
        if mmdts is None:
            return 0

        db = self.facc((TusSdbs.SDB_STOCK_ADJFACTOR.value + code), STOCK_ADJFACTOR_META)
        bvalid = np.full((len(mmdts),), True, dtype=np.bool)

        if flag == IOFLAG.ERASE_ALL:
            for n, dd in enumerate(mmdts):
                dtkey = dt64_to_strdt(dd)
                db.remove(dtkey)
                bvalid[n] = False
        else:
            for n, dd in enumerate(mmdts):
                dtkey = dt64_to_strdt(dd)
                val = db.load(dtkey, raw_mode=True)
                if val is not None:
                    bvalid = self.integrity_check_km_vday(dd, val[:, 0], code)
                    if not bvalid:
                        db.remove(dtkey)
                        bvalid[n] = False
        db.commit()
        return np.sum(~bvalid)


g_checker: XcDBChecker = None


def tuschecker_init() -> XcDBChecker:
    global g_checker
    if g_checker is None:
        g_checker = XcDBChecker()
        # try:
        g_checker.init_domain()
        # except:
        #     log.info('Init domain fail.')
        #     pass
    return g_checker
