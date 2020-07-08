import logbook, sys
from boost_tushare.xcbooster import *
from boost_tushare import tusbooster_init

if __name__ == '__main__':
    app_logging = logbook.NestedSetup([
        logbook.NullHandler(),
        logbook.StreamHandler(sys.stdout, level=logbook.INFO),
        logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
    ])
    app_logging.push_application()

    reader = tusbooster_init()

    cntus_update_basic()

    cntus_update_stock_day(start_date='20130101')
    cntus_update_index_day(start_date='20180101')

    cntus_update_stock_min(start_date='20170101')


    # reader = TusUpdater()

    # stks = ['000155.XSHE'] #'000002.XSHE',
    # for stk in stks:
    #     log.info('-->{}'.format(stk))
    #     df = reader.get_stock_suspend_d(stk)
    #     log.info('daily1')
    #     start_date = '20150101'
    #     end_date = '20191231'
    #     reader.price_daily_update(stk, start_date, end_date, mode=-1)
    #     reader.price_daily_update(stk, start_date, end_date)
    #     df_day1 = reader.get_price_daily(stk, start_date, end_date)
    #
    #     log.info('daily2')
    #     reader.price_daily_update(stk, '20150323', '20160101', mode=-1)
    #     reader.price_daily_update(stk, '20170523', '20170529', mode=-1)
    #     reader.price_daily_update(stk, start_date, end_date)
    #     df_day2 = reader.get_price_daily(stk, start_date, end_date)
    #
    #     res = (df_day1.values.ravel() == df_day2.values.ravel())
    #     if np.all(res):
    #         print('right')
    #
    # stks = ['000002.XSHE', '000155.XSHE']
    # for stk in stks:
    #     log.info('-->{}'.format(stk))
    #     # df = reader.get_stock_suspend_d(stk, refresh=True)
    #     log.info('min1')
    #     start_date = '20180101'
    #     end_date = '20191231'
    #     reader.price_minute_update(stk, start_date, end_date, mode=-1)
    #     reader.price_minute_update(stk, start_date, end_date)
    #     df_day1 = reader.get_price_minute(stk, start_date, end_date)
    #
    #     log.info('min2')
    #     reader.price_minute_update(stk, '20180323', '20190101', mode=-1)
    #     reader.price_minute_update(stk, '20190523', '20190529', mode=-1)
    #     reader.price_minute_update(stk, start_date, end_date)
    #     df_day2 = reader.get_price_minute(stk, start_date, end_date)
    #
    #     res = (df_day1.values.ravel() == df_day2.values.ravel())
    #     if np.all(res):
    #         print('right')

    # stks = ['000002.XSHE', '000155.XSHE']
    # for stk in stks:
    #     log.info('-->{}'.format(stk))
    #     # df = reader.get_stock_suspend_d(stk, refresh=True)
    #     log.info('min1')
    #     start_date = '20150101'
    #     end_date = '20191231'
    #     reader.stock_adjfactor_erase(stk, start_date, end_date)
    #     reader.stock_adjfactor_update(stk, start_date, end_date)
    #     df_day1 = reader.get_stock_adjfactor(stk, start_date, end_date)
    #
    #     log.info('min2')
    #     reader.stock_adjfactor_erase(stk, '20150323', '20160101')
    #     reader.stock_adjfactor_erase(stk, '20170523', '20170529')
    #     reader.stock_adjfactor_update(stk, start_date, end_date)
    #     df_day2 = reader.get_stock_adjfactor(stk, start_date, end_date)
    #
    #     res = (df_day1.values.ravel() == df_day2.values.ravel())
    #     if np.all(res):
    #         print('right')

    # stks = ['000002.XSHE', '000155.XSHE']
    # for stk in stks:
    #     log.info('-->{}'.format(stk))
    #     # df = reader.get_stock_suspend_d(stk, refresh=True)
    #     log.info('min1')
    #     start_date = '20150101'
    #     end_date = '20191231'
    #     reader.stock_dayinfo_update(stk, start_date, end_date, b_erase=True)
    #     reader.stock_dayinfo_update(stk, start_date, end_date)
    #     df_day1 = reader.get_stock_daily_info(stk, start_date, end_date)
    #
    #     log.info('min2')
    #     reader.stock_dayinfo_update(stk, '20150323', '20160101', b_erase=True)
    #     reader.stock_dayinfo_update(stk, '20170523', '20170529', b_erase=True)
    #     reader.stock_dayinfo_update(stk, start_date, end_date)
    #     df_day2 = reader.get_stock_daily_info(stk, start_date, end_date)
    #
    #     df_day1.fillna(0, inplace=True)
    #     df_day2.fillna(0, inplace=True)
    #     res = (df_day1.values.ravel() == df_day2.values.ravel())
    #     if np.all(res):
    #         print('right')
