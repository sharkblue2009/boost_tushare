"""
Data fetch turbo accelerator
"""
import concurrent.futures
from decimal import Decimal, ROUND_HALF_UP
from logbook import Logger

logger = Logger('parallel')


def precise_round(num):
    return float(Decimal(str(num)).quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))


def parallelize(mapfunc, workers=10, splitlen=10):
    """
    并行化处理
    """
    def wrapper(symbols):
        result = {}
        with concurrent.futures.ThreadPoolExecutor(
                max_workers=workers) as executor:
            tasks = []
            for i in range(0, len(symbols), splitlen):
                part = symbols[i:i + splitlen]
                task = executor.submit(mapfunc, part)
                tasks.append(task)

            total_count = len(symbols)
            report_percent = 10
            processed = 0
            for task in concurrent.futures.as_completed(tasks):
                task_result = task.result()
                result.update(task_result)
                processed += len(task_result)
                percent = processed / total_count * 100
                if percent >= report_percent:
                    logger.debug('{}: {:.2f}% completed'.format(
                        mapfunc.__name__, percent))
                    report_percent = (percent + 10.0) // 10 * 10
        return result

    return wrapper


