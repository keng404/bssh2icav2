import logging
from logging import Logger
logging.getLogger('backoff').addHandler(logging.StreamHandler())
import backoff
import requests
from requests.exceptions import HTTPError
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

#### borrowing code from https://copdips.com/2021/01/python-requests-with-retry.html

# in an internal enterprise environment, we often need to disable the proxy and ignore the ssl check. Of course, if you don't trust the target, then verify the ssl.
NO_PROXY = {"http": None, "https": None}
COMMON_REQUESTS_PARAMS = {"verify": True, "proxies": NO_PROXY}
# This snippet only retries on the response return code >= 500
def fatal_code(e):
    return 400 <= e.response.status_code < 500


BACKOFF_RETRY_ON_EXCEPTION_PARAMS = {
    # expo: [1, 2, 4, 8, etc.] https://github.com/litl/backoff/blob/master/backoff/_wait_gen.py#L6
    "wait_gen": backoff.expo,
    # HTTPError raised by raise_for_status()
    # HTTPError code list: https://github.com/psf/requests/blob/master/requests/models.py#L943
    "exception": (HTTPError,),
    "max_tries": 10,
    "max_time": 300,  # nginx closes a session at 60' second by default
    "giveup": fatal_code,
}


@backoff.on_exception(**BACKOFF_RETRY_ON_EXCEPTION_PARAMS)
def request_with_retry(
    should_log: bool = True,
    logger: Logger = logging.getLogger(),
    logger_level: str = "info",
    **request_params
):
    full_params = COMMON_REQUESTS_PARAMS | request_params
    requests_params_keys_to_log = ["data", "json", "params","header"]
    if should_log:
        params_message = ""
        for key in requests_params_keys_to_log:
            if key in request_params:
                params_message += " with {} {}".format(key, request_params[key])
        log_message = "[{}] {} with params{}.".format(
            full_params["method"], full_params["url"], params_message
        )
        getattr(logger, logger_level.lower())(log_message)
    response = requests.request(**full_params)
    #response.raise_for_status()
    return response

# how to use:
#request_params = {"method": "get", "url": "http://localhost"}
#response = request_with_retry(**request_params)