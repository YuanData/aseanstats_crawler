import datetime
import logging
import os
import random
import traceback
from functools import wraps

from config import log_dir


def change_logger_file(filename=None, b_abs_path=False):
    for handler in logger.handlers[:]:  # remove all old handlers
        logger.removeHandler(handler)

    dt_now = datetime.datetime.now()
    dt_now = dt_now.replace(hour=0, minute=0, microsecond=0)

    if b_abs_path:
        log_path = filename
    else:
        if filename is None:
            log_path = os.path.join(log_dir, 'ASEANSTATS_%s.log.txt' % (dt_now.strftime("%Y%m%d")))
        else:
            log_path = os.path.join(log_dir, filename)

    file_d = logging.FileHandler(log_path, 'a', 'utf-8')

    # 設定輸出格式
    formatter = logging.Formatter('%(asctime)-15s %(name)-12s %(levelname)-8s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    # handler 設定輸出格式
    file_d.setFormatter(formatter)
    # 加入 handler 到 root logger
    logger.addHandler(file_d)

    # console 的部分先行關閉
    # console = logging.StreamHandler()
    # console.setLevel(logging.INFO)
    # console.setFormatter(formatter)
    # logger.addHandler(console)


# Use logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('')
change_logger_file()


def try_except_log(func):
    @wraps(func)
    def job_wrapper(*args, **kwargs):
        try:
            r = func(*args, **kwargs)
        except Exception as e:
            r = None
            logger.error("[%s] Exception: %s" % (func.__name__, e))
            logger.error(traceback.format_exc())
        return r

    return job_wrapper


def time_log(func):
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        arg_lst = [str(arg) for arg in args]
        if kwargs is not None:
            kwarg_lst = ['{k}={v}'.format(k=key, v=value) for key, value in kwargs.items()]
            arg_lst.extend(kwarg_lst)
        logger.info('[{func}({args})]'.format(func=func.__name__, args=', '.join(arg_lst)))
        t_start = datetime.datetime.now()
        r = func(*args, **kwargs)
        cost_t = datetime.datetime.now() - t_start
        logger.info('[%s] Done. cost: %.02f seconds' % (func.__name__, cost_t.total_seconds()))
        return r

    return func_wrapper


def args_time_log(func):
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        t_start = datetime.datetime.now()
        r = func(*args, **kwargs)
        cost_t = datetime.datetime.now() - t_start

        arg_lst = [str(arg) for arg in args]
        if kwargs is not None:
            kwarg_lst = ['{k}={v}'.format(k=key, v=value) for key, value in kwargs.items()]
            arg_lst.extend(kwarg_lst)
        msg = '[%s] cost: %.02f seconds' % (', '.join(arg_lst), cost_t.total_seconds())
        logger.info(msg)
        print(msg)
        return r

    return func_wrapper


def gen_rand_sec(base=1, multiplier=1):
    return base + random.random() * multiplier

# def if_exist_load_from(directory_):
#     def call_func(gen_df_func):
#         @wraps(gen_df_func)
#         def job_wrapper(*args, **kwargs):
#             df_name = gen_df_func.__name__.replace('gen_', '')
#             if not os.path.exists(os.path.join(directory_, '%s.pkl' % df_name)):
#
#                 df = gen_df_func(*args, **kwargs)
#                 if isinstance(df, pd.DataFrame):
#                     df.to_pickle(os.path.join(directory_, '%s.pkl' % df_name))
#                 else:
#                     pickle.dump(df, open(os.path.join(directory_, '%s.pkl' % df_name), 'wb'))
#             else:
#                 df = pickle.load(open(os.path.join(directory_, '%s.pkl' % df_name), 'rb'), encoding='utf-8')
#             return df
#
#         return job_wrapper
#
#     return call_func
