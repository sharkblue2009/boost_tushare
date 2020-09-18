import os
import math, sys

import click
import logbook
import pandas as pd
import numpy as np
from boost_tushare.utils.parallelize import parallelize

from boost_tushare.api import *
from boost_tushare import *
from boost_tushare.xcbooster import XcTusBooster

log = logbook.Logger('cli')


def progress_bar(cur, total):
    percent = '{:.0%}'.format(cur / total)
    sys.stdout.write('\r')
    sys.stdout.write("[%-50s] %s" % ('=' * int(math.floor(cur * 50 / total)), percent))
    sys.stdout.flush()


def cntus_update_basic():
    """

    :param b_stock_day:
    :param b_stock_min:
    :param b_index_day:
    :param b_index_min:
    :return:
    """
    log.info('Download basic information...(Trading Calendar, Asset info)')
    tusbooster_init()
    get_trade_cal(IOFLAG.READ_NETDB)

    get_index_info(IOFLAG.READ_NETDB)
    get_stock_info(IOFLAG.READ_NETDB)
    get_fund_info(IOFLAG.READ_NETDB)
    get_index_classify(level='L1', flag=IOFLAG.READ_NETDB)
    get_index_classify(level='L2', flag=IOFLAG.READ_NETDB)
    log.info('Finished')

    booster = tusbooster_init()
    XcTusBooster.get_trade_cal(IOFLAG.READ_NETDB)

    start_date = '20100101'
    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    log.info('Downloading stocks suspend data: {}-{}'.format(start_date, end_date))
    update_suspend_d(start_date, end_date)

    return


def cntus_update_stock_day(start_date='20150101'):
    tusbooster_init()

    df_stock = get_stock_info()

    def _fetch_day(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = update_price_daily(stk, t_start, t_end, astype)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    # log.info('Downloading stocks suspend data: {}, {}-{}'.format(len(df_stock), start_date, end_date))
    # update_suspend_d(start_date, end_date)

    # dummy read suspend_info
    suspend_info = get_suspend_d(start_date, end_date)
    # booster = init_booster()
    # XcTusBooster.suspend_info.update(booster)

    all_symbols = []
    for k, stk in df_stock['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'E'})

    all_symbols = list(reversed(all_symbols))
    log.info('Downloading stocks data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

    batch_size = 60
    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))
        symbol_batch = all_symbols[idx:idx + batch_size]

        # result = _fetch_day(symbol_batch)
        result = parallelize(_fetch_day, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))


def cntus_update_stock_day_ext(start_date='20150101'):
    tusbooster_init()

    df_stock = get_stock_info()

    def _fetch_stock_ext(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']

            # Froce reload xdxr from net
            get_stock_xdxr(stk, IOFLAG.READ_NETDB)

            # reader.update_stock_adjfactor(stk, t_start, t_end)
            results[stk] = update_stock_dayinfo(stk, t_start, t_end)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    # dummy read suspend_info
    suspend_info = get_suspend_d(start_date, end_date)

    all_symbols = []
    for k, stk in df_stock['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'E'})

    all_symbols = list(reversed(all_symbols))

    batch_size = 60

    log.info('Downloading stocks extension data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))
        symbol_batch = all_symbols[idx:idx + batch_size]
        result = parallelize(_fetch_stock_ext, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))


def cntus_update_index_day(start_date):
    tusbooster_init()

    df_index = get_index_info()

    def _fetch_day(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = update_price_daily(stk, t_start, t_end, astype)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    log.info('Downloading index daily quotations: {}, {}-{}'.format(len(df_index), start_date, end_date))

    all_symbols = []
    for k, stk in df_index['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'I'})

    batch_size = 60
    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))
        symbol_batch = all_symbols[idx:idx + batch_size]
        result = parallelize(_fetch_day, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))


def cntus_update_stock_min(start_date='20190101'):
    tusbooster_init()

    df_stock = get_stock_info()

    def _fetch_min(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = update_price_minute(stk, t_start, t_end, freq='5min', astype=astype)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    log.info('Downloading stocks minute price: {}, {}-{}'.format(len(df_stock), start_date, end_date))

    all_symbols = []
    for k, stk in df_stock['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'E'})

    # all_symbols = all_symbols[::-1]

    batch_size = 60
    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))

        symbol_batch = all_symbols[idx:idx + batch_size]

        result = parallelize(_fetch_min, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
        # _fetch_min(symbol_batch)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))


def cntus_update_index_min(start_date='20190101'):
    tusbooster_init()

    df_index = get_index_info()

    def _fetch_min(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = update_price_minute(stk, t_start, t_end, freq='1min', astype=astype)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    log.info('Downloading stocks minute quotation: {}, {}-{}'.format(len(df_index), start_date, end_date))

    all_symbols = []
    for k, stk in df_index['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'I'})

    batch_size = 60
    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))

        symbol_batch = all_symbols[idx:idx + batch_size]

        result = parallelize(_fetch_min, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))
    return


def cntus_check_stock_day(start_date='20150101'):
    tusbooster_init()

    df_stock = get_stock_info()

    def _fetch_day(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = check_price_daily(stk, t_start, t_end, astype)

        return results

    def _fetch_stock_ext(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']

            get_stock_xdxr(stk, IOFLAG.READ_NETDB)
            # reader.update_stock_adjfactor(stk, t_start, t_end)
            results[stk] = check_stock_dayinfo(stk, t_start, t_end)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    # dummy read suspend_info
    suspend_info = get_suspend_d(start_date, end_date)
    # booster = init_booster()
    # XcTusBooster.suspend_info.update(booster)

    all_symbols = []
    for k, stk in df_stock['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'E'})

    all_symbols = list(reversed(all_symbols))
    log.info('Downloading stocks data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

    batch_size = 60
    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))
        symbol_batch = all_symbols[idx:idx + batch_size]

        # result = _fetch_day(symbol_batch)
        result = parallelize(_fetch_day, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))

    log.info('Downloading stocks extension data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))
        symbol_batch = all_symbols[idx:idx + batch_size]
        parallelize(_fetch_stock_ext, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))


####################################################################################
import cmd


class REPL(cmd.Cmd):
    def __init__(self, ctx):
        cmd.Cmd.__init__(self)
        self.ctx = ctx

        self.prompt = 'BST>>'

    def default(self, line):
        subcommand = first.commands.get(line)
        if subcommand:
            self.ctx.invoke(subcommand)
        else:
            return cmd.Cmd.default(self, line)


@click.group(invoke_without_command=True)
@click.pass_context
def first(ctx):
    if ctx.invoked_subcommand is None:
        repl = REPL(ctx)
        repl.cmdloop()


@click.command()
def update_basic():
    cntus_update_basic()
    click.echo('done')


@click.command()
@click.option("--start", default='20130101', )
@click.option("--end", default=None, )
def update_daily(start, end):
    cntus_update_stock_day(start_date=start)
    click.echo('done')


@click.command()
@click.option("--start", default='20130101', )
@click.option("--end", default=None, )
def update_daily_ext(start, end):
    cntus_update_stock_day_ext(start_date=start)
    click.echo('done')


@click.command()
@click.option("--start", default='20170101', )
@click.option("--end", default=None, )
def update_minute(start, end):
    cntus_update_stock_min(start_date=start)
    click.echo('done')


@click.command()
@click.option("--start", default='20170101', )
@click.option("--end", default=None, )
def update_index_daily(start, end):
    cntus_update_index_day(start_date=start)
    click.echo('done')


@click.command()
@click.option("--start", default='20170101', )
@click.option("--end", default=None, )
def update_index_minute(start, end):
    cntus_update_index_min(start_date=start)
    click.echo('done')


@click.command()
@click.option("--start", default='20130101', )
@click.option("--end", default=None, )
def check_daily(start, end):
    cntus_check_stock_day(start_date=start)
    click.echo('done')


first.add_command(update_basic)
first.add_command(update_daily)
first.add_command(update_daily_ext)
first.add_command(update_minute)
first.add_command(update_index_daily)
first.add_command(update_index_minute)
first.add_command(check_daily)

if __name__ == "__main__":
    import logbook, sys

    app_logging = logbook.NestedSetup([
        logbook.NullHandler(),
        logbook.StreamHandler(sys.stdout, level=logbook.INFO),
        logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
    ])
    app_logging.push_application()

    first()
