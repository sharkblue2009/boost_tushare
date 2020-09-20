"""
Utils
"""
import logbook
import numpy as np
import pandas as pd

log = logbook.Logger('bstutl')

XTUS_FREQS = ['1min', '5min', '15min', '30min', '60min']
XTUS_FREQ_BARS = {'1min': 240, '5min': 48, '15min': 16, '30min': 8, '60min': 4}


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


def DAY_START(date):
    """
    :param date:
    :return:
    """
    dd = date
    mday = pd.Timestamp(year=dd.year, month=dd.month, day=dd.day) + pd.Timedelta(hours=9, minutes=30)
    return mday


def DAY_END(date):
    dd = date
    mday = pd.Timestamp(year=dd.year, month=dd.month, day=dd.day) + pd.Timedelta(hours=15, minutes=0)
    return mday


def MONTH_START(date, trade_days=None):
    """
    根据某个日期找到这个月的第一天，或者本月交易日的第一天。
    :param date:
    :param trade_days:
    :return:
    """
    dd = date
    mday = pd.Timestamp(year=dd.year, month=dd.month, day=1)
    if trade_days is None:
        return mday

    mdays = trade_days[trade_days >= mday]
    if len(mdays) == 0:
        return None

    mday = mdays[0]
    if (mday.year == dd.year) and (mday.month == dd.month):
        return mday

    return None


def MONTH_END(date, trade_days=None):
    dd = date
    mday = pd.Timestamp(year=dd.year, month=dd.month, day=dd.days_in_month)
    if trade_days is None:
        return mday

    mdays = trade_days[trade_days <= mday]
    if len(mdays) == 0:
        return None

    mday = mdays[-1]
    if (mday.year == dd.year) and (mday.month == dd.month):
        return mday
    return None


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


def gen_keys_monthly(start_dt, end_dt, asset_life, trade_cal):
    """
    根据当前交易品种的有效交易日历， 产生月度keys
    :param start_dt:
    :param end_dt:
    :param asset_life: tuple(born_date, dead_date)
    :param trade_cal:
    :return:
    """
    if asset_life is not None:
        l_ss, l_ee = asset_life
        tstart = max([l_ss, start_dt])
        tend = min([l_ee, end_dt])
    else:
        tstart = start_dt
        tend = end_dt

    if trade_cal is not None:
        tend = min(tend, trade_cal[-1])

    m_start = pd.Timestamp(year=tstart.year, month=tstart.month, day=1)
    m_end = pd.Timestamp(year=tend.year, month=tend.month, day=tend.days_in_month)
    key_index = pd.date_range(m_start, m_end, freq='MS')

    return key_index


def gen_dayindex_monthly(month_k, trade_cal):
    """
    :param month_k: monthly key index
    :param trade_cal: day trade_cal
    :return: {month_key: day_sess in the month}
    """
    mst = MONTH_START(month_k)
    med = MONTH_END(month_k)
    dayindex = trade_cal[(trade_cal >= mst) & (trade_cal.year <= med)]

    return dayindex


def gen_dayindex_periods(start_day, n_periods, trade_cal):
    tmp_cal = trade_cal[trade_cal >= start_day]
    tmp_cal = tmp_cal[:n_periods]
    return tmp_cal


def gen_keys_daily(start_dt, end_dt, asset_life, trade_cal):
    """
    根据当前交易品种的有效交易日历， 产生日度keys
    :param start_dt:
    :param end_dt:
    :param asset_life: tuple(born_date, dead_date)
    :param trade_cal: trade_cal index
    :return:
    """
    if asset_life is not None:
        l_ss, l_ee = asset_life
        tstart = max([l_ss, start_dt])
        tend = min([l_ee, end_dt])
    else:
        tstart = start_dt
        tend = end_dt

    if trade_cal is None:
        key_index = pd.date_range(tstart, tend, freq='D')
    else:
        key_index = trade_cal[(trade_cal >= tstart) & (trade_cal <= tend)]
    return key_index


def gen_minindex_daily(day_time, trade_cal_min):
    """
    :param day_time: day session index
    :param trade_cal_min: minute trade_cal
    :return: dict, {month_key: day_sess in the month}
    """

    mst = DAY_START(day_time)
    med = DAY_END(day_time)
    minindex = trade_cal_min[(trade_cal_min >= mst) & (trade_cal_min.year <= med)]

    return minindex


def gen_minindex_periods(start_time, n_periods, trade_cal):
    tmp_cal = trade_cal[trade_cal >= start_time]
    tmp_cal = tmp_cal[:n_periods]
    return tmp_cal


def gen_keys_quarterly(start_dt, end_dt, asset_life=None, trade_cal=None):
    """

    :param start_dt:
    :param end_dt:
    :param asset_life: tuple(born_date, dead_date)
    :param trade_cal:
    :return:
    """

    limit_start, limit_end = asset_life

    # 当前交易品种的有效交易日历
    tstart = max([limit_start, start_dt])
    tend = min([limit_end, end_dt])

    if trade_cal is not None:
        tend = min(tend, trade_cal[-1])

    m_start = pd.Timestamp(year=tstart.year, month=1, day=1)
    m_end = pd.Timestamp(year=tend.year, month=tend.month, day=tend.days_in_month)

    key_index = pd.date_range(m_start, m_end, freq='QS')
    return key_index


def session_day_to_min_tus(day_sess, freq='1min', market_open=True):
    """
    Convert day based session to high frequency session
    :param day_sess:
    :param freq:
    :param market_open: if include 9:30
    :return: DatetimeIndex
    """
    time_a = pd.Timedelta(hours=9, minutes=30)
    time_b = pd.Timedelta(hours=11, minutes=30)
    time_c = pd.Timedelta(hours=13, minutes=0)
    time_d = pd.Timedelta(hours=15, minutes=0)

    freq_c = freq.replace('min', 'Min')

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


def integrity_check_km_vday(dt, dtval, trade_days, susp_info=None, code=None):
    """
    monthly key with daily data. use suspend information to check if the data is integrate
    :param dt: date keys
    :param dtval:  data values, ndarray, [volume]
    :param trade_days:
    :param susp_info:
    :return:
    """
    if dtval is None:
        return False

    trdays = trade_days[(trade_days >= MONTH_START(dt)) & (trade_days <= MONTH_END(dt))]
    if susp_info is None:
        expect_size = len(trdays)
        susp = None
    else:
        susp = susp_info[(susp_info.index >= MONTH_START(dt)) & (susp_info.index <= MONTH_END(dt))]
        susp = susp.loc[(susp['suspend_type'] == 'S') & (susp['suspend_timing'].isna()), :]
        expect_size = len(trdays) - len(susp)

    if expect_size <= np.sum(~np.isnan(dtval)):
        # 股票存在停牌半天的情况，也会被计入suspend列表
        bvalid = True
    else:
        bvalid = False
        if susp_info is not None:
            log.info('[!KMVDAY]:{}-{}:: t{}-v{}-s{} '.format(code, dt, len(trdays), len(susp), len(dtval)))

    return bvalid


def integrity_check_kd_vday(dt, dtval, trade_days):
    """
    daily key with daily data.
    :return:
    """
    if dtval is None:
        return False

    if dt in trade_days:
        return True

    log.info('[!KDVDAY]: {}'.format(dt))
    return False


def integrity_check_kd_vmin(dt, dtval, trade_days, susp_info=None, freq='1min', code=None):
    """
    daily key with minute data, use suspend information to judge if the data should exist,
    :param dt: date keys
    :param dtval:  data values
    :param trade_days:
    :param susp_info:
    :param freq:
    :return:
    """
    if dtval is None:
        return False

    nbars = XTUS_FREQ_BARS[freq]

    bvalid = False
    if dt in trade_days:
        vldlen = np.sum(~np.isnan(dtval))
        if susp_info is None:
            if vldlen == nbars:
                bvalid = True
        else:
            susp = susp_info.loc[(susp_info['suspend_type'] == 'S') & (susp_info.index == dt), :]
            if not susp.empty:
                if susp['suspend_timing'].isna():  # .iloc[-1]
                    # 当日全天停牌
                    if vldlen == 0:
                        bvalid = True
                else:
                    # 部分时间停牌
                    if nbars > vldlen > 0:
                        bvalid = True
            else:
                if 0 < vldlen <= nbars:
                    # Fixme, 如果部分时间没有交易，vol=0, vldlen可能小于nbars
                    bvalid = True

        if not bvalid and susp_info is not None:
            log.info('[!KDVMIN]-: {}-{}:: {}-{} '.format(code, dt, len(susp), vldlen))

    return bvalid


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
