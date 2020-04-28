import tushare as ts

TUS_TOKEN = '8231fd2b6d147cac3e170dce427a67f6b225476f924c7c976a6e84f2'
ts.set_token(TUS_TOKEN)
pro = ts.pro_api()

trade_date='20191010'
start_date='20190901'
end_date='20190930'

df = pro.index_weight(index_code='399300.SZ', trade_date=trade_date)
