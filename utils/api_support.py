import functools
import boost_tushare.boost
import threading

context = threading.local()
print('===', context)


def get_algo_instance():
    return getattr(context, 'tusbooster', None)


def set_algo_instance(algo):
    context.tusbooster = algo
    return




class BoostTushareAPI(object):
    """
    Context manager for making an algorithm instance available to zipline API
    functions within a scoped block.
    """

    def __init__(self, algo_instance):
        self.algo_instance = algo_instance

        set_algo_instance(self.algo_instance)

    def __enter__(self):
        """
        Set the given algo instance, storing any previously-existing instance.
        """
        self.old_algo_instance = get_algo_instance()
        set_algo_instance(self.algo_instance)

    def __exit__(self, _type, _value, _tb):
        """
        Restore the algo instance stored in __enter__.
        """
        set_algo_instance(self.old_algo_instance)


def api_method(f):
    # Decorator that adds the decorated class method as a callable
    # function (wrapped) to zipline.api
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        # Get the instance and call the method
        algo_instance = get_algo_instance()
        if algo_instance is None:
            raise RuntimeError(
                'api method %s must be called with instance initialed.'
                % f.__name__
            )
        return getattr(algo_instance, f.__name__)(*args, **kwargs)

    # Add functor to boost_tushare.boost
    setattr(boost_tushare.boost, f.__name__, wrapped)
    boost_tushare.boost.__all__.append(f.__name__)
    f.is_api_method = True
    return f
