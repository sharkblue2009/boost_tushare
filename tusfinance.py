from cntus.utils.xctus_utils import symbol_std_to_tus, symbol_tus_to_std
from cntus.xcachedb import *
from cntus.dbschema import *


class TusFinanceInfo(object):
    """
    Finance information loader
    """

    master_db = None
    pro_api = None
    tus_last_date = None

    def get_income(self, code, period, refresh=False):
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_FIN_INCOME.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_FIN_INCOME_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tpp1 = pd.Timestamp(period)

        # 当前交易品种的有效交易日历
        today = pd.Timestamp.today()
        tpp1 = max([list_date, tpp1])
        tend = min([delist_date, tpp1, today])
        if tpp1 > tend:
            return None

        if not tpp1.is_quarter_end:
            return None

        dtkey = tpp1.strftime(DATE_FORMAT)
        if not refresh:
            val = db.load(dtkey)
            if val is not None:
                return val

        fcols = STOCK_FIN_INCOME_META['columns']
        data = self.pro_api.income(ts_code=tscode, period=period,
                                          fields=fcols)
        out = db.save(dtkey, data)

        return out

    def get_balancesheet(self, code, period, refresh=False):
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_FIN_BALANCE.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_FIN_BALANCE_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tpp1 = pd.Timestamp(period)

        # 当前交易品种的有效交易日历
        today = pd.Timestamp.today()
        tpp1 = max([list_date, tpp1])
        tend = min([delist_date, tpp1, today])
        if tpp1 > tend:
            return None

        if not tpp1.is_quarter_end:
            return None

        dtkey = tpp1.strftime(DATE_FORMAT)
        if not refresh:
            val = db.load(dtkey)
            if val is not None:
                return val

        fcols = STOCK_FIN_BALANCE_META['columns']
        data = self.pro_api.balancesheet(ts_code=tscode, period=period,
                                                fields=fcols)

        out = db.save(dtkey, data)

        return out

    def get_cashflow(self, code, period, refresh=False):
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_FIN_CASHFLOW.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_FIN_CASHFLOW_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tpp1 = pd.Timestamp(period)

        # 当前交易品种的有效交易日历
        today = pd.Timestamp.today()
        tpp1 = max([list_date, tpp1])
        tend = min([delist_date, tpp1, today])
        if tpp1 > tend:
            return None

        if not tpp1.is_quarter_end:
            return None

        dtkey = tpp1.strftime(DATE_FORMAT)
        if not refresh:
            val = db.load(dtkey)
            if val is not None:
                return val

        fcols = STOCK_FIN_CASHFLOW_META['columns']
        data = self.pro_api.cashflow(ts_code=tscode, period=period,
                                                fields=fcols)

        out = db.save(dtkey, data)

        return out

    def get_forcast(self, code, period):
        pass

    def get_express(self, code, period):
        pass

    def get_fina_indicator(self, code, period, refresh=False):
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_FIN_INDICATOR.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_FIN_INDICATOR_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.asset_lifetime(code)

        tpp1 = pd.Timestamp(period)

        # 当前交易品种的有效交易日历
        today = pd.Timestamp.today()
        tpp1 = max([list_date, tpp1])
        tend = min([delist_date, tpp1, today])
        if tpp1 > tend:
            return None

        if not tpp1.is_quarter_end:
            return None

        dtkey = tpp1.strftime(DATE_FORMAT)
        if not refresh:
            val = db.load(dtkey)
            if val is not None:
                return val

        fcols = STOCK_FIN_INDICATOR_META['columns']
        data = self.pro_api.fina_indicator(ts_code=tscode, period=period,
                                                fields=fcols)

        out = db.save(dtkey, data)

        return out

