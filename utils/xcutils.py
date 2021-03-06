"""
Utils
"""
import logbook
import numpy as np
import pandas as pd

log = logbook.Logger('bstutl')

XTUS_FREQS = ['1min', '5min', '15min', '30min', '60min']
XTUS_FREQ_BARS = {'1min': 240, '5min': 48, '15min': 16, '30min': 8, '60min': 4}
DATE_FORMAT = '%Y%m%d'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'


def nadata_iter(ar_flags, max_length):
    """
    生成器，查找连续的False序列，返回位置 start, end
    :param ar_flags:
    :param max_length:
    :return:
    """
    pps = None
    ppe = None
    cnt = 0
    for n, dd in enumerate(ar_flags):
        if dd:
            if ppe is not None:
                tstart = pps
                tend = ppe
                yield tstart, tend
                # print('[1]', tstart, tend)
                ppe = None
                pps = None
            cnt = 0
        else:
            if pps is None:
                pps = n
            ppe = n
            cnt += 1
            if cnt > max_length or n == (len(ar_flags) - 1):
                tstart = pps
                tend = ppe
                yield tstart, tend
                # print('[2]', tstart, tend)
                ppe = None
                pps = None
                cnt = 0
    yield None, None


def dt64_to_strdt(dt):
    """
    Convert np.datetime64 to String format Datatime used for Tushare
    :param dt:
    :return:
    """
    # dd = pd.Timestamp(dt)
    # dtkey = dd.strftime(DATE_FORMAT)
    dtkey = np.datetime_as_string(dt, unit='D')
    return dtkey


def strdt_to_dt64(dt):
    """
    Convert  String format Datatime used for Tushare np.datetime64
    :param dt:
    :return:
    """
    if isinstance(dt, np.datetime64):
        return dt
    dt64 = pd.Timestamp(dt).to_datetime64()
    return dt64


def DAY_START(date):
    """
    :param date:
    :return:
    """
    dd = date
    mday = pd.Timestamp(year=dd.year, month=dd.month, day=dd.day) + pd.Timedelta(hours=9, minutes=30)
    mday = mday.to_datetime64().astype('M8[m]')
    return mday


def DAY_END(date):
    dd = date
    mday = pd.Timestamp(year=dd.year, month=dd.month, day=dd.day) + pd.Timedelta(hours=15, minutes=0)
    mday = mday.to_datetime64().astype('M8[m]')
    return mday


def MONTH_START(date, trade_days=None):
    """
    根据某个日期找到这个月的第一天，或者本月交易日的第一天。
    :param date:
    :param trade_days:
    :return:
    """
    dd = pd.Timestamp(date)
    mday = pd.Timestamp(year=dd.year, month=dd.month, day=1)
    mday = mday.to_datetime64().astype('M8[D]')
    return mday
    # if trade_days is None:
    #     return mday
    #
    # mdays = trade_days[trade_days >= mday]
    # if len(mdays) == 0:
    #     return None
    #
    # mday = mdays[0]
    # if (mday.year == dd.year) and (mday.month == dd.month):
    #     return mday
    #
    # return None


def MONTH_END(date, trade_days=None):
    dd = pd.Timestamp(date)
    mday = pd.Timestamp(year=dd.year, month=dd.month, day=dd.days_in_month)
    mday = mday.to_datetime64().astype('M8[D]')
    return mday
    # if trade_days is None:
    #     return mday.to_datetime64().astype('M8[D]')
    #
    # mdays = trade_days[trade_days <= mday]
    # if len(mdays) == 0:
    #     return None
    #
    # mday = mdays[-1]
    # if (mday.year == dd.year) and (mday.month == dd.month):
    #     return mday
    # return None


_quater_map = [0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]


def QUARTER_START(date, trade_days=None):
    """
    根据某个日期找到这个季度的第一天，或者本季度交易日的第一天。
    :param date:
    :param trade_days:
    :return:
    """
    dd = date
    mday = pd.Timestamp(year=dd.year, month=_quater_map[dd.month], day=1)
    if trade_days is None:
        return mday

    mday = trade_days[trade_days >= mday][0]
    if mday.year == dd.year and _quater_map[mday.month] == _quater_map[dd.month]:
        return mday
    return None


def QUARTER_END(date, trade_days=None):
    dd = date
    mday = pd.Timestamp(year=dd.year, month=_quater_map[dd.month] + 2, day=1)
    mday = pd.Timestamp(year=mday.year, month=mday.month, day=mday.days_in_month)
    if trade_days is None:
        return mday

    mday = trade_days[trade_days <= mday][-1]
    if mday.year == dd.year and _quater_map[mday.month] == _quater_map[dd.month]:
        return mday
    return None


# from pytz import UTC

def _compute_all_minutes(opens_in_ns, closes_in_ns, periods, sections):
    """
    Given arrays of opens and closes, both in nanoseconds,
    return an array of each minute between the opens and closes.
    """
    deltas = closes_in_ns - opens_in_ns

    # + 1 because we want 390 mins per standard day, not 389
    daily_sizes = (deltas // periods) + 1
    # num_minutes = daily_sizes.sum()

    # One allocation for the entire thing. This assumes that each day
    # represents a contiguous block of minutes.
    pieces = []

    for open_, size in zip(opens_in_ns, daily_sizes):
        day_mins = np.arange(open_, open_ + size * periods, periods)
        day_mins = np.append(day_mins[:sections[0]], day_mins[sections[1]:])
        pieces.append(day_mins)

    out = np.concatenate(pieces).view('datetime64[ns]')
    # assert len(out) == num_minutes
    return out


NANOSECONDS_PER_MINUTE = int(6e10)


def session_day_to_min_tus(day_sess, freq='1min', market_open=False, tz=None):
    """
    Exchange calendar for the Shanghai Stock Exchange (XSHG, XSSC, SSE).

    Open time: 9:30 Asia/Shanghai
    Close time: 15:00 Asia/Shanghai

    NOTE: For now, we are skipping the intra-day break from 11:30 to 13:00.

    Due to the complexity around the Shanghai exchange holidays, we are
    hardcoding a list of holidays covering 1999-2025, inclusive. There are
    no known early closes or late opens.
    """
    freq = freq.lower()
    n_min = {'1min': 1, '5min': 5, '15min': 15, '30min': 30, '60min': 60}
    morn_nonn = {'1min': (120, -120), '5min': (24, -24),
                 '15min': (8, -8), '30min': (4, -4), '60min': (2, -2)}

    if market_open:
        _opens = day_sess + pd.Timedelta(hours=9, minutes=30)
        _closes = day_sess + pd.Timedelta(hours=15, minutes=0)
        periods = n_min[freq] * NANOSECONDS_PER_MINUTE
        sections = morn_nonn[freq]
        sections = (sections[0] + 1, sections[1])
    else:
        _opens = day_sess + pd.Timedelta(hours=9, minutes=30 + n_min[freq])
        _closes = day_sess + pd.Timedelta(hours=15, minutes=0)
        periods = n_min[freq] * NANOSECONDS_PER_MINUTE
        sections = morn_nonn[freq]

    """
    Returns a DatetimeIndex representing all the minutes in this calendar.
    """
    opens_in_ns = _opens.values.astype(
        'datetime64[ns]',
    ).view('int64')

    closes_in_ns = _closes.values.astype(
        'datetime64[ns]',
    ).view('int64')

    return pd.DatetimeIndex(_compute_all_minutes(opens_in_ns, closes_in_ns, periods, sections), tz=tz, )


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


FORMAT = lambda x: '%.4f' % x


def price1m_resample(data1m, freq='5min', market_open=True):
    """
    convert 1 minute data to other frequency
    :param data1m:
    :param periods:  5, 15, 30, 60, 120
    :param market_open: if has market_open(9:30) data
    :return:
    """
    cc = {'1min': 1, '5min': 5, '15min': 15, '30min': 30, '60min': 60}
    periods = cc[freq]
    if periods == 1:
        return data1m

    data1m = data1m.reset_index(drop=False)
    if market_open:
        # market_open only has one bar, so need to handle it specially.
        data1m.index = data1m.index + (data1m.index // 241 + 1) * (periods - 1)

    df_out = data1m.groupby(data1m.index // periods).last()
    df_out['open'] = data1m['open'].groupby(data1m.index // periods).first()
    df_out['high'] = data1m['high'].groupby(data1m.index // periods).max()  # .map(lambda x: FORMAT(x)).astype(float)
    df_out['low'] = data1m['low'].groupby(data1m.index // periods).min()
    df_out['volume'] = data1m['volume'].groupby(data1m.index // periods).sum()
    df_out['amount'] = data1m['amount'].groupby(data1m.index // periods).sum()
    df_out = df_out.set_index('trade_time')
    return df_out


def df_to_sarray(df):
    """
    Convert a pandas DataFrame object to a numpy structured array.
    This is functionally equivalent to but more efficient than
    np.array(df.to_array())

    From: http://stackoverflow.com/questions/30773073/save-pandas-dataframe-using-h5py-for-interoperabilty-with-other-hdf5-readers
          https://stackoverflow.com/questions/13187778/convert-pandas-dataframe-to-numpy-array/35971974
    Parameters
    ----------
    df : dataframe
         the data frame to convert

    Returns
    -------
    z : ndarray
        a numpy structured array representation of df
    """
    v = df.values
    cols = df.columns

    # if six.PY2:  # python 2 needs .encode() but 3 does not
    #     types = [(cols[i].encode(), df[k].dtype.type) for (i, k) in enumerate(cols)]
    # else:
    types = [(cols[i], df[k].dtype.type) for (i, k) in enumerate(cols)]

    dtype = np.dtype(types)
    z = np.zeros(v.shape[0], dtype)
    for (i, k) in enumerate(z.dtype.names):
        z[k] = v[:, i]
    return z


def sarray_to_df(sarray, index_column='index'):
    """
    Convert from a structured array back to a Pandas Dataframe

    Parameters
    ----------
    sarray : array
             numpy structured array

    index_column : str
                   The name of the index column.  Default: 'index'

    Returns
    -------
     : dataframe
       A pandas dataframe
    """

    def remove_field_name(a, name):
        names = list(a.dtype.names)
        if name in names:
            names.remove(name)
        b = a[names]
        return b

    if index_column is not None:
        index = sarray[index_column]
        clean_array = remove_field_name(sarray, 'index')
    else:
        clean_array = sarray
        index = None
    columns = clean_array.dtype.names

    return pd.DataFrame(data=sarray, index=index, columns=columns)
