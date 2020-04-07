import pandas as pd


def session_day_to_min_tus(day_sess, freq='1Min', market_open=True):
    """
    Convert day based session to high frequency session
    :param day_sess:
    :param freq:
    :return: DatetimeIndex
    """
    time_a = pd.Timedelta(hours=9, minutes=30)
    time_b = pd.Timedelta(hours=11, minutes=30)
    time_c = pd.Timedelta(hours=13, minutes=0)
    time_d = pd.Timedelta(hours=15, minutes=0)
    cc = {'1min': '1Min', '5min': '5Min', '15min': '15Min', '30min': '30Min', '60min': '60Min', '120min': '120Min'}
    freq_c = cc[freq]

    if market_open:
        sides = None
    else:
        sides = 'right'

    out_sess = None
    for x in day_sess:
        ss1 = pd.date_range(start=x + time_a, end=x + time_b, freq=freq_c, closed=sides)
        if out_sess is None:
            out_sess = ss1
        else:
            out_sess = out_sess.append(ss1)
        ss1 = pd.date_range(start=x + time_c, end=x + time_d, freq=freq_c, closed='right')
        out_sess = out_sess.append(ss1)

    return out_sess


def symbol_tus_to_std(symbol: str):
    stock, market = symbol.split('.')

    if market == 'SH':
        code = stock + '.XSHG'
    elif market == 'SZ':
        code = stock + '.XSHE'
    else:
        raise ValueError('Symbol error{}'.format(symbol))

    return code


def symbol_std_to_tus(symbol: str):
    """
    convert STD symbol string to Tushare format,
    Tushare symbol format is like 000001.SZ, 000001.SH
    :param symbol:
    :return: tdx code,
    """
    stock, market = symbol.split('.')
    if market == 'XSHG':
        code = stock + '.SH'
    elif market == 'XSHE':
        code = stock + '.SZ'
    else:
        raise ValueError('Symbol error{}'.format(symbol))

    return code


# def find_closest_date(all_dates, dt, mode='backward'):
#     """
#
#     :param all_dates:
#     :param dt:
#     :param mode:
#     :return:
#     """
#     tt_all_dates = pd.to_datetime(all_dates, format='%Y%m%d')
#     tt_dt = pd.Timestamp(dt)
#     if mode == 'backward':
#         valid = tt_all_dates[tt_all_dates <= tt_dt]
#         if len(valid) > 0:
#             return valid[-1].strftime('%Y%m%d')
#     else:
#         valid = tt_all_dates[tt_all_dates >= tt_dt]
#         if len(valid) > 0:
#             return valid[0].strftime('%Y%m%d')
#     return None


def price1m_resample(data1m, periods=5, market_open=True):
    """
    convert 1 minute data to other frequency
    :param data1m:
    :param periods:  5, 15, 30, 60, 120
    :param market_open: if has market_open(9:30) data
    :return:
    """
    if periods not in [5, 15, 30, 60, 120]:
        raise KeyError

    data1m = data1m.reset_index(drop=False)
    if market_open:
        # market_open only has one bar, so need to handle it specially.
        data1m.index = data1m.index + (data1m.index // 241 + 1) * (periods - 1)

    df_out = data1m.groupby(data1m.index // periods).last()
    df_out['open'] = data1m['open'].groupby(data1m.index // periods).first()
    df_out['high'] = data1m['high'].groupby(data1m.index // periods).max()
    df_out['low'] = data1m['low'].groupby(data1m.index // periods).min()
    df_out['volume'] = data1m['volume'].groupby(data1m.index // periods).sum()
    df_out['amount'] = data1m['amount'].groupby(data1m.index // periods).sum()
    df_out = df_out.set_index('trade_time')
    return df_out
