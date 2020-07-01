from .xcdb.xcdb import *


def get_trade_cal(flag=IOFLAG.READ_XC):
    """
    :param flag:
    :return:
    """


def get_index_info(flag=IOFLAG.READ_XC):
    """
    :param flag:
    :return:
    """


def get_stock_info(flag=IOFLAG.READ_XC):
    """
    :param flag:
    :return:
    """


def get_fund_info(flag=IOFLAG.READ_XC):
    """

    :param flag:
    :return:
    """


def get_index_classify(level, src='SW', flag=IOFLAG.READ_XC):
    """

    :param level:
    :param src:
    :param flag:
    :return:
    """


def get_index_weight(index_symbol, date, month_start=False, flag=IOFLAG.READ_XC):
    """
    """


def get_index_member(index_code, flag=IOFLAG.READ_XC):
    """

    :param index_code:
    :param flag:
    :return:
    """


###############################################################################

def get_stock_suspend(code, flag=IOFLAG.READ_XC):
    """"""


def get_suspend_d(start='20100101', end='21000101', flag=IOFLAG.READ_XC):
    """

    :param start:
    :param end:
    :param flag:
    :return:
    """


def get_price_daily(code, start: str, end: str, astype=None, flag=IOFLAG.READ_XC):
    """

    :param code:
    :param start:
    :param end:
    :param astype:
    :param flag:
    :return:
    """


def get_price_minute(code, start, end, freq='1min', astype='E', resample=False, flag=IOFLAG.READ_XC):
    """

    :param code:
    :param start:
    :param end:
    :param freq:
    :param astype:
    :param resample:
    :param flag:
    :return:
    """


def get_stock_daily_info(code, start, end, flag=IOFLAG.READ_XC):
    """"""


def get_stock_adjfactor(code, start: str, end: str, flag=IOFLAG.READ_XC):
    """"""


def get_stock_xdxr(code, flag=IOFLAG.READ_XC):
    """"""


###################################################################
def get_income(code, period, flag=IOFLAG.READ_XC):
    """"""


def get_balancesheet(code, period, flag=IOFLAG.READ_XC):
    """"""


def get_cashflow(code, period, flag=IOFLAG.READ_XC):
    """"""


def get_fina_indicator(code, period, flag=IOFLAG.READ_XC):
    """"""


###############################################################################
def update_suspend_d(start, end):
    """"""


def update_price_daily(code, start, end, astype, rollback=3):
    """"""


def update_price_minute(code, start, end, freq='1min', astype='E', rollback=5):
    """"""


def update_stock_adjfactor(code, start, end, rollback=3):
    """"""


def update_stock_dayinfo(code, start, end, rollback=3):
    """"""
