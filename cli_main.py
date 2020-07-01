import os

import click
import logbook
import pandas as pd

log = logbook.Logger('cli')


from boost_tushare.xcbooster import cntus_update_basic, cntus_update_stock_day, \
    cntus_update_stock_min, cntus_update_index_day, cntus_update_index_min

@click.group()
def first():
    print("hello world")

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

first.add_command(update_basic)
first.add_command(update_daily)
first.add_command(update_minute)
first.add_command(update_index_daily)
first.add_command(update_index_minute)

first()