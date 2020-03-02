from trading_calendars import get_calendar
# from zipline.cn_wrapper.pipeline.cnutils import symbol_tus_to_std

import tushare as ts
from cntus._passwd import TUS_TOKEN
import pandas as pd

from enum import Enum
from cntus.xcachedb import *


def symbol_tus_to_std(symbol: str):
    stock, market = symbol.split('.')

    if market == 'SH':
        code = stock + '.XSHG'
    elif market == 'SZ':
        code = stock + '.XSHE'
    else:
        raise ValueError('Symbol error{}'.format(symbol))

    return code


class TUS_KEY(Enum):
    INDEX_INFO = 'IndexInfo'
    STOCK_INFO = 'StockInfo'
    FUND_INFO = 'FundInfo'


class TUS_SDB(Enum):
    SDB_EQUITY_INFO = 'ts:equity_info'
    SDB_DAILY_PRICE = 'ts:daliy_price'
    SDB_MINUTE_PRICE = 'ts:minute_price'
    SDB_INDEX_WEIGHT = 'ts:index_weight:'


EQUITY_INFO_META = {
    'columns': ['ts_code', 'exchange', 'name', 'start_date', 'end_date'],
}

INDEX_WEIGHT_META = {
    'columns': ['con_code', 'weight'],
}


class TusReader(object):

    def __init__(self):
        self.calendar = get_calendar('XSHG')
        ts.set_token(TUS_TOKEN)
        self.pro_api = ts.pro_api()
        self.master_db = XCacheDB()

    def get_stock_price(self, code, start, end, freq, fields):
        """"""

    def get_stock_xdxr(self, code):
        """"""

    def get_index_info(self):
        """"""
        db = XcAccessor(self.master_db.get_sdb(TUS_SDB.SDB_EQUITY_INFO.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, EQUITY_INFO_META)

        val = db.load(TUS_KEY.INDEX_INFO.value)
        if val is not None:
            log.info('load...')
            return val

        def conv1(sym, subfix):
            stock, market = sym.split('.')
            code = stock + subfix
            return code

        log.info('update...')
        fields = 'ts_code,name,list_date'
        info1 = self.pro_api.index_basic(market='SSE', fields=fields)
        info1.loc[:, 'ts_code'] = info1.loc[:, 'ts_code'].apply(conv1, subfix='.XSHG')
        info1.loc[:, 'exchange'] = 'SSE'
        info2 = self.pro_api.index_basic(market='SZSE', fields=fields)
        info2.loc[:, 'ts_code'] = info2.loc[:, 'ts_code'].apply(conv1, subfix='.XSHE')
        info2.loc[:, 'exchange'] = 'SZSE'
        info = pd.concat([info1, info2], axis=0)
        info.loc[:, 'list_date'].fillna('20000101', inplace=True)

        info_to_db = pd.DataFrame({
            'ts_code': info['ts_code'],
            'exchange': info['exchange'],
            'name': info['name'],
            'start_date': info['list_date'],
            'end_date': '21000101'
        })

        db.save(TUS_KEY.INDEX_INFO.value, info_to_db)
        return info_to_db

    def get_stock_info(self):
        """"""
        db = XcAccessor(self.master_db.get_sdb(TUS_SDB.SDB_EQUITY_INFO.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, EQUITY_INFO_META)

        val = db.load(TUS_KEY.STOCK_INFO.value)
        if val is not None:
            log.info('load...')
            return val

        log.info('update...')
        fields = 'ts_code,symbol,name,exchange,area,industry,list_date,delist_date'
        info1 = self.pro_api.stock_basic(list_status='L', fields=fields)  # 上市状态： L上市 D退市 P暂停上市
        info2 = self.pro_api.stock_basic(list_status='D', fields=fields)
        info3 = self.pro_api.stock_basic(list_status='P', fields=fields)
        info = pd.concat([info1, info2, info3], axis=0)
        info.loc[:, 'ts_code'] = info.loc[:, 'ts_code'].apply(symbol_tus_to_std)
        info.loc[:, 'delist_date'].fillna('21000101', inplace=True)

        info_to_db = pd.DataFrame({
            'ts_code': info['ts_code'],
            'exchange': info['exchange'],
            'name': info['name'],
            'start_date': info['list_date'],
            'end_date': info['delist_date'],
        })

        db.save(TUS_KEY.STOCK_INFO.value, info_to_db)
        return info_to_db

    def get_fund_info(self):
        """"""
        db = XcAccessor(self.master_db.get_sdb(TUS_SDB.SDB_EQUITY_INFO.value),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, EQUITY_INFO_META)

        val = db.load(TUS_KEY.FUND_INFO.value)
        if val is not None:
            log.info('load...')
            return val

        log.info('update...')
        fields = 'ts_code,name,list_date,delist_date'
        info = self.pro_api.fund_basic(market='E', fields=fields)  # 交易市场: E场内 O场外（默认E）
        # info2 = self.pro_api.fund_basic(market='O', fields=fields)
        # info = pd.concat([info1, info2], axis=0)
        info.loc[:, 'ts_code'] = info.loc[:, 'ts_code'].apply(symbol_tus_to_std)
        info.loc[:, 'delist_date'].fillna('21000101', inplace=True)
        info.loc[:, 'exchange'] = info.loc[:, 'ts_code'].apply(lambda x: 'SSE' if x.endswith('.XSHG') else 'SZSE')

        info_to_db = pd.DataFrame({
            'ts_code': info['ts_code'],
            'exchange': info['exchange'],
            'name': info['name'],
            'start_date': info['list_date'],
            'end_date': info['delist_date'],
        })

        db.save(TUS_KEY.FUND_INFO.value, info_to_db)
        return info_to_db

    def get_index_weight(self, index_sym, date):
        """"""
        db = XcAccessor(self.master_db.get_sdb(TUS_SDB.SDB_EQUITY_INFO.value+index_sym),
                        KVTYPE.TPK_RAW, KVTYPE.TPV_DFRAME, EQUITY_INFO_META)

    def update_all_stock_info(self):
        # 上市状态： L上市 D退市 P暂停上市
        info1 = self.pro_api.stock_basic(list_status='L')
        info2 = self.pro_api.stock_basic(list_status='D')
        info3 = self.pro_api.stock_basic(list_status='P')
        info = pd.concat([info1, info2, info3], axis=0)
        info.loc[:, 'ts_code'] = info.loc[:, 'ts_code'].apply(symbol_tus_to_std)
        info = info.set_index('ts_code')
        info.index = info.index.astype(str)


if __name__ == '__main__':
    import logbook, sys

    zipline_logging = logbook.NestedSetup([
        logbook.NullHandler(),
        logbook.StreamHandler(sys.stdout, level=logbook.INFO),
        logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
    ])
    zipline_logging.push_application()

    reader = TusReader()

    df = reader.get_index_info()
    print(df)

    df = reader.get_stock_info()
    print(df)

    df = reader.get_fund_info()
    print(df)
