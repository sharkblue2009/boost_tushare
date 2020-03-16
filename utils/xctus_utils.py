import pandas as pd

def session_day_to_freq(day_sess, freq='1Min'):
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

    out_sess = None
    for x in day_sess:
        ss1 = pd.date_range(start=x + time_a, end=x + time_b, freq=freq, closed='right')
        if out_sess is None:
            out_sess = ss1
        else:
            out_sess = out_sess.append(ss1)
        ss1 = pd.date_range(start=x + time_c, end=x + time_d, freq=freq, closed='right')
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
