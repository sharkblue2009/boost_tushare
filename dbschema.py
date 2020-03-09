from enum import Enum


class TusKeys(Enum):
    INDEX_INFO = 'IndexInfo'
    STOCK_INFO = 'StockInfo'
    FUND_INFO = 'FundInfo'


class TusSdbs(Enum):
    SDB_TRADE_CALENDAR = 'ts'
    SDB_EQUITY_INFO = 'ts:equity_info'
    SDB_EQUITY_CALENDAR = 'ts:equity_calendar'
    SDB_STOCK_XDXR = 'ts:stock_xdxr'
    SDB_DAILY_PRICE = 'ts:daliy_price:'
    SDB_MINUTE_PRICE = 'ts:minute_price:'
    SDB_INDEX_WEIGHT = 'ts:index_weight:'


EQUITY_INFO_META = {
    'columns': ['ts_code', 'exchange', 'name', 'start_date', 'end_date'],
}

INDEX_WEIGHT_META = {
    'columns': ['trade_date', 'con_code', 'weight'],
}

STOCK_XDXR_META = {
    'columns': ['end_date',
                'ann_date',
                'div_proc',
                'stk_div',
                'stk_bo_rate',
                'stk_co_rate',
                'cash_div',
                'cash_div_tax',
                'record_date',
                'ex_date',
                'pay_date',
                'div_listdate',
                'imp_ann_date',
                # 'base_date',
                # 'base_share',
                ]
}

EQUITY_DAILY_PRICE_META = {
    'columns': [
        'trade_date',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'amount',
    ]
}

EQUITY_MINUTE_PRICE_META = {
    'columns': [
        'trade_time',
        'open',
        'high',
        'low',
        'close',
        'volume',
        'amount',
    ]
}