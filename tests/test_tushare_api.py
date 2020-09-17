import tushare as ts

from boost_tushare._passwd import TUS_TOKEN

ts.set_token(TUS_TOKEN)
pro = ts.pro_api()

trade_date = '20191010'
start_date = '20200301'
end_date = '20200317'

df = pro.index_weight(index_code='399300.SZ', trade_date='20191031')

df = ts.pro_bar('000785.SZ', asset='E', start_date=start_date, end_date=end_date, freq='D')
df = ts.pro_bar('000785.SZ', asset='E', start_date=start_date, end_date=end_date, freq='5min')

df = pro.daily_basic(ts_code='000785.SZ', start_date=start_date, end_date=end_date,
                     fields=[
                         'trade_date',
                         'close',
                         'turnover_rate',
                         'turnover_rate_f',
                         'volume_ratio',
                         'pe',
                         'pe_ttm',
                         'pb',
                         'ps',
                         'ps_ttm',
                         'dv_ratio',
                         'dv_ttm',
                         'total_share',
                         'float_share',
                         'free_share',
                         'total_mv',
                         'circ_mv',
                     ])
