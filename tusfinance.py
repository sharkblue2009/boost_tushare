from tusreader import TusReader, symbol_std_to_tus, symbol_tus_to_std, get_tusreader
from xcachedb import *
from dbschema import *


class TusFinance(object):
    """

    """

    def __init__(self, reader):
        self.reader = reader
        self.master_db = reader.master_db

    def get_income(self, code, period, refresh=False):
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_FIN_INCOME.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_FIN_INCOME_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.reader.asset_lifetime(code)

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
        data = self.reader.pro_api.income(ts_code=tscode, period=period,
                                          fields=fcols)
        if data is None:
            # create empyt dataframe for nan data.
            info_to_db = pd.DataFrame(columns=fcols)
        elif data.empty:
            info_to_db = pd.DataFrame(columns=fcols)
        else:
            info_to_db = data.reindex(columns=fcols)

        db.save(dtkey, info_to_db)

        return info_to_db

    def get_balancesheet(self, code, period, refresh=False):
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_FIN_BALANCE.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_FIN_BALANCE_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.reader.asset_lifetime(code)

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
        data = self.reader.pro_api.balancesheet(ts_code=tscode, period=period,
                                                fields=fcols)
        if data is None:
            # create empyt dataframe for nan data.
            info_to_db = pd.DataFrame(columns=fcols)
        elif data.empty:
            info_to_db = pd.DataFrame(columns=fcols)
        else:
            info_to_db = data.reindex(columns=fcols)

        db.save(dtkey, info_to_db)

        return info_to_db

    def get_cashflow(self, code, period, refresh=False):
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_FIN_CASHFLOW.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_FIN_CASHFLOW_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.reader.asset_lifetime(code)

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
        data = self.reader.pro_api.cashflow(ts_code=tscode, period=period,
                                                fields=fcols)
        if data is None:
            # create empyt dataframe for nan data.
            info_to_db = pd.DataFrame(columns=fcols)
        elif data.empty:
            info_to_db = pd.DataFrame(columns=fcols)
        else:
            info_to_db = data.reindex(columns=fcols)

        db.save(dtkey, info_to_db)

        return info_to_db

    def get_forcast(self, code, period):
        pass

    def get_express(self, code, period):
        pass

    def get_fina_indicator(self, code, period, refresh=False):
        db = XcAccessor(self.master_db.get_sdb(TusSdbs.SDB_STOCK_FIN_INDICATOR.value + code),
                        KVTYPE.TPK_DATE, KVTYPE.TPV_DFRAME, STOCK_FIN_INDICATOR_META)

        tscode = symbol_std_to_tus(code)
        astype, list_date, delist_date = self.reader.asset_lifetime(code)

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
        data = self.reader.pro_api.fina_indicator(ts_code=tscode, period=period,
                                                fields=fcols)
        if data is None:
            # create empyt dataframe for nan data.
            info_to_db = pd.DataFrame(columns=fcols)
        elif data.empty:
            info_to_db = pd.DataFrame(columns=fcols)
        else:
            info_to_db = data.reindex(columns=fcols)

        db.save(dtkey, info_to_db)

        return info_to_db

gfinreader = None

def get_tusfinreader():
    global gfinreader
    if gfinreader is None:
        gfinreader = TusFinance(reader=get_tusreader())
    return gfinreader

if __name__ == '__main__':
    import logbook, sys
    import timeit
    from tusreader import TusReader

    zipline_logging = logbook.NestedSetup([
        logbook.NullHandler(),
        logbook.StreamHandler(sys.stdout, level=logbook.INFO),
        logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
    ])
    zipline_logging.push_application()

    reader = TusReader()
    fin_reader = TusFinance(reader)

    df = fin_reader.get_income('002465.XSHE', '20150630', refresh=True)

    print(df)
    df = fin_reader.get_balancesheet('002465.XSHE', '20150630')
    print(df)
    df = fin_reader.get_cashflow('002465.XSHE', '20150630')
    print(df)
    df = fin_reader.get_fina_indicator('002465.XSHE', '20150630')
    print(df)
    # print(timeit.Timer(lambda: fin_reader.get_income('002465.XSHE', '20150630', refresh=True)).timeit(1))
    # print(timeit.Timer(lambda: fin_reader.get_income('002465.XSHE', '20150630')).timeit(1))
