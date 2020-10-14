import os
import math, sys

import click
import cmd
import logbook
import pandas as pd
import numpy as np
from boost_tushare.utils.parallelize import parallelize

from boost_tushare import *
from boost_tushare.xupdater import *
from boost_tushare.xchecker import *

log = logbook.Logger('cli')


def progress_bar(cur, total):
    percent = '{:.0%}'.format(cur / total)
    sys.stdout.write('\r')
    sys.stdout.write("[%-50s] %s" % ('=' * int(math.floor(cur * 50 / total)), percent))
    sys.stdout.flush()


def cntus_update_basic():
    """
    :return:
    """
    log.info('Update basic information...(Trading Calendar, Asset info)')
    updater = tusupdater_init()
    updater.update_domain(force_mode=True)

    start_date = '20100101'
    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    log.info('Downloading stocks suspend data: {}-{}'.format(start_date, end_date))
    updater.update_suspend_d(start_date, end_date)

    log.info('Renew Domain Class...')
    updater.renew_domain()
    updater.init_domain()

    return


def cntus_update_stock_day(start_date='20150101', type='L'):
    updater = tusupdater_init()

    df_stock = updater.get_stock_info()
    df_stock = df_stock[df_stock.list_status == type]
    if len(df_stock) == 0:
        return

    def _fetch_day(symbols):
        results = {}
        for ss in symbols:
            sst = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[sst] = updater.update_price_daily(sst, t_start, t_end, astype)
            # updater.update_stock_xdxr(sst, t_start, t_end)  # Froce reload xdxr from net
            # updater.update_stock_adjfactor(stk, t_start, t_end)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    # log.info('Downloading stocks suspend data: {}, {}-{}'.format(len(df_stock), start_date, end_date))
    # update_suspend_d(start_date, end_date)

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


def cntus_update_stock_xdxr(start_date='20150101', type='L'):
    updater = tusupdater_init()

    df_stock = updater.get_stock_info()
    df_stock = df_stock[df_stock.list_status == type]
    if len(df_stock) == 0:
        return

    def _fetch_day(symbols):
        results = {}
        for ss in symbols:
            sst = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            # astype = ss['astype']
            updater.update_stock_xdxr(sst, t_start, t_end)  # Froce reload xdxr from net
            results[sst] = 1
            # updater.update_stock_adjfactor(stk, t_start, t_end)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    all_symbols = []
    for k, stk in df_stock['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'E'})

    all_symbols = list(reversed(all_symbols))
    log.info('Downloading stocks xdxr data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

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


def cntus_update_stock_day_ext(start_date='20150101', type='L'):
    updater = tusupdater_init()

    df_stock = updater.get_stock_info()
    df_stock = df_stock[df_stock.list_status == type]
    if len(df_stock) == 0:
        return

    def _fetch_stock_ext(symbols):
        results = {}
        for ss in symbols:
            sst = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            results[sst] = updater.update_stock_dayinfo(sst, t_start, t_end)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    # dummy read suspend_info
    # suspend_info = updater.get_suspend_d(start_date, end_date)

    all_symbols = []
    for k, stk in df_stock['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'E'})

    all_symbols = list(reversed(all_symbols))
    batch_size = 60
    log.info('Downloading stocks extension(xdxr, dayinfo) data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))
        symbol_batch = all_symbols[idx:idx + batch_size]
        result = parallelize(_fetch_stock_ext, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))


def cntus_update_stock_min(start_date='20190101', type='L'):
    updater = tusupdater_init()

    df_stock = updater.get_stock_info()
    df_stock = df_stock[df_stock.list_status == type]
    if len(df_stock) == 0:
        return

    def _fetch_min(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = updater.update_price_minute(stk, t_start, t_end, freq='5min', astype=astype)

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

        # result = _fetch_min(symbol_batch)
        result = parallelize(_fetch_min, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))


def cntus_update_index_day(start_date):
    updater = tusupdater_init()

    df_index = updater.get_index_info()

    def _fetch_day(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = updater.update_price_daily(stk, t_start, t_end, astype)

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


def cntus_update_index_min(start_date='20190101'):
    updater = tusupdater_init()

    df_index = updater.get_index_info()

    def _fetch_min(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = updater.update_price_minute(stk, t_start, t_end, freq='1min', astype=astype)

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


####################################################################
def cntus_check_stock_day(start_date='20150101'):
    checker = tuschecker_init()

    df_stock = checker.get_stock_info()
    df_stock = df_stock[df_stock.list_status == 'L']

    def _fetch_day(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = checker.check_price_daily(stk, t_start, t_end, astype)

        return results

    def _fetch_stock_ext(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            results[stk] = checker.check_stock_dayinfo(stk, t_start, t_end)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    all_symbols = []
    for k, stk in df_stock['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'E'})

    all_symbols = list(reversed(all_symbols))
    log.info('Checking stocks data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

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

    log.info('Checking stocks extension data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

    all_result = {}
    for idx in range(0, len(all_symbols), batch_size):
        progress_bar(idx, len(all_symbols))
        symbol_batch = all_symbols[idx:idx + batch_size]
        result = parallelize(_fetch_stock_ext, workers=20, splitlen=3)(symbol_batch)
        all_result.update(result)
    sys.stdout.write('\n')
    log.info('Total units: {}'.format(np.sum(list(all_result.values()))))


def cntus_erase_stock_min(freq='5min'):
    log.info('Drop all minutes data {}'.format(freq))

    checker = tuschecker_init()
    df_stock = checker.get_stock_info()

    for k, stk in df_stock['ts_code'].items():
        db = checker.facc((TusSdbs.SDB_MINUTE_PRICE.value + stk + freq), EQUITY_MINUTE_PRICE_META)
        db.drop()
        db.commit()

    log.info('Done.')


def cntus_check_index_day(start_date='20150101'):
    checker = tuschecker_init()

    df_stock = checker.get_index_info()

    def _fetch_day(symbols):
        results = {}
        for ss in symbols:
            stk = ss['code']
            t_start = ss['start_date']
            t_end = ss['end_date']
            astype = ss['astype']
            results[stk] = checker.check_price_daily(stk, t_start, t_end, astype)

        return results

    end_date = pd.Timestamp.today().strftime('%Y%m%d')

    all_symbols = []
    for k, stk in df_stock['ts_code'].items():
        all_symbols.append({'code': stk, 'start_date': start_date, 'end_date': end_date, 'astype': 'I'})

    all_symbols = list(reversed(all_symbols))
    log.info('Checking index data: {}, {}-{}'.format(len(df_stock), start_date, end_date))

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


####################################################################################


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
@click.option("--type", default='L', )
def update_daily(start, type):
    cntus_update_stock_day(start_date=start, type=type)
    click.echo('done')


@click.command()
@click.option("--start", default='20130101', )
@click.option("--type", default='L', )
def update_xdxr(start, type):
    cntus_update_stock_xdxr(start_date=start, type=type)
    click.echo('done')


@click.command()
@click.option("--start", default='20130101', )
@click.option("--type", default='L', )
def update_daily_ext(start, type):
    cntus_update_stock_day_ext(start_date=start, type=type)
    click.echo('done')


@click.command()
@click.option("--start", default='20170101', )
@click.option("--type", default='L', )
def update_minute(start, type):
    cntus_update_stock_min(start_date=start, type=type)
    click.echo('done')


@click.command()
@click.option("--start", default='20170101', )
def update_index_daily(start):
    cntus_update_index_day(start_date=start)
    click.echo('done')


@click.command()
@click.option("--start", default='20170101', )
def update_index_minute(start):
    cntus_update_index_min(start_date=start)
    click.echo('done')


@click.command()
@click.option("--start", default='20130101', )
def check_daily(start):
    cntus_check_stock_day(start_date=start)
    click.echo('done')


@click.command()
@click.option("--start", default='20130101', )
def check_index_daily(start):
    cntus_check_index_day(start_date=start)
    click.echo('done')



@click.command()
@click.option("--days", default=0, )
def tsshow(days):
    """"""
    netreader = netloader_init()
    today = pd.Timestamp.today()
    if days > 0:
        today = today - pd.Timedelta(days=days)
    today = today.strftime(DATE_FORMAT)

    print('Check tushare data status: {}'.format(today))
    print('-' * 50)
    df = netreader.set_price_daily('000001.SH', today, today, astype='I')
    if df.empty:
        print('Daily data unavailable')
    else:
        print(df)
        print('Daily data OK')

    print('-' * 50)
    df = netreader.set_stock_daily_info('000001.SZ', today, today)
    if df.empty:
        print('DailyInfo unavailable')
    else:
        print(df)
        print('DailyInfo OK')

    print('-' * 50)
    df = netreader.set_stock_bakdaily('000001.SZ', today, today)
    if df.empty:
        print('BakDailyInfo unavailable')
    else:
        print(df)
        print('BakDailyInfo OK')

    print('-' * 50)
    df = netreader.set_stock_moneyflow('000001.SZ', today, today)
    if df.empty:
        print('Moneyflow unavailable')
    else:
        print(df)
        print('Moneyflow OK')

    print('-' * 50)
    df = netreader.set_price_minute('000001.SH', today, today, freq='5min', astype='I')
    if df.empty:
        print('Minute unavailable')
    else:
        print(df.iloc[-3:])
        print('Minute OK')
    print('-' * 50)

    print('-' * 50)
    df = netreader.set_suspend_d(today)
    if df.empty:
        print('Suspend unavailable')
    else:
        print(df.iloc[-3:])
        print('Suspend OK')
    print('-' * 50)


@click.command()
def dbshow():
    reader = tusbooster_init()
    # reader.master_db.show()
    reader.master_db.detail()


first.add_command(update_basic)
first.add_command(update_daily)
first.add_command(update_xdxr)
first.add_command(update_daily_ext)
first.add_command(update_minute)
first.add_command(update_index_daily)
first.add_command(update_index_minute)
first.add_command(check_daily)
first.add_command(check_index_daily)
first.add_command(tsshow)
first.add_command(dbshow)

if __name__ == "__main__":
    import logbook, sys

    app_logging = logbook.NestedSetup([
        logbook.NullHandler(),
        logbook.StreamHandler(sys.stdout, level=logbook.INFO),
        logbook.StreamHandler(sys.stderr, level=logbook.ERROR),
    ])
    app_logging.push_application()

    first()
