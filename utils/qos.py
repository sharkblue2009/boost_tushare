import time
from threading import Lock


class TokenBucket(object):
    def __init__(self, rate, capacity, full=True):
        """
        :param rate: token per second
        :param capacity: bucket capacity
        """
        self._rate = rate
        self._capacity = capacity
        if full:
            self._current_amount = capacity
        else:
            self._current_amount = 0
        self._last_consume_time = time.time()

    def consume(self, token_amount=1):
        """
        :param token_amount: token amount need take
        :return:
            True  : ok
            False : not ok
        """
        # 计算从上次发送到这次发送，新发放的令牌数量
        offset = time.time() - self._last_consume_time
        incr = int(offset * self._rate) if 1 <= offset else 0
        # 令牌数量不能超过桶的容量
        self._current_amount = min(incr + self._current_amount, self._capacity)
        self._last_consume_time = time.time()
        # 如果没有足够的令牌，则不能发送数据
        if token_amount > self._current_amount:
            return False
        else:
            self._current_amount -= token_amount
            return True

    def block_consume(self, token_amount=1, interval=5):
        """
        :param token_amount: token amount need take
        :param interval: sleep interval (second)
        """
        while not self.consume(token_amount):
            time.sleep(interval)


class ThreadingTokenBucket(object):
    def __init__(self, rate, capacity, full=True):
        """
                :param rate: token per second
                :param capacity: bucket capacity
                """
        self._lock = Lock()
        self._bucket = TokenBucket(rate, capacity, full)

    def consume(self, token_amount=1):
        self._lock.acquire()
        try:
            return self._bucket.consume(token_amount)
        finally:
            self._lock.release()

    def block_consume(self, token_amount=1, interval=5):
        while not self.consume(token_amount):
            time.sleep(interval)
