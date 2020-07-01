import functools
import boost_tushare.api
import threading

# context = threading.local()
# print('===', context)

g_instance = None


def get_algo_instance():
    # return getattr(context, 'tusbooster', None)
    return g_instance


def set_algo_instance(algo):
    # context.tusbooster = algo
    global g_instance
    g_instance = algo
    return


def api_call(f):
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
    setattr(boost_tushare.api, f.__name__, wrapped)
    boost_tushare.api.__all__.append(f.__name__)
    f.is_api_method = True
    return f
