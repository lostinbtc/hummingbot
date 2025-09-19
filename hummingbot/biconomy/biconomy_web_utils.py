import asyncio
import time
from typing import Callable, Awaitable, Optional, Any, Dict
import aiohttp
import socket

import hummingbot.connector.exchange.biconomy.biconomy_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.connector.utils import TimeSynchronizerRESTPreProcessor
from hummingbot.core.api_throttler.async_throttler import AsyncThrottler
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


def public_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for a public REST endpoint.
    :param path_url: a public REST endpoint.
    :param domain: the Biconomy domain to connect to.
    :return: the full URL to the endpoint.
    """
    return f"{CONSTANTS.REST_URL}{path_url}"


def private_rest_url(path_url: str, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> str:
    """
    Creates a full URL for a private REST endpoint.
    :param path_url: a private REST endpoint.
    :param domain: the Biconomy domain to connect to.
    :return: the full URL to the endpoint.
    """
    return f"{CONSTANTS.REST_URL}{path_url}"


def build_api_factory(
    throttler: Optional[AsyncThrottler] = None,
    time_synchronizer: Optional[TimeSynchronizer] = None,
    auth: Optional[AuthBase] = None,
    domain: str = CONSTANTS.DEFAULT_DOMAIN,
) -> WebAssistantsFactory:
    """
    Creates a web assistants factory for the Biconomy exchange.
    """
    throttler = throttler or create_throttler()
    time_synchronizer = time_synchronizer or TimeSynchronizer()
    time_provider = lambda: get_current_server_time(throttler=throttler, domain=domain)
    
    # We explicitly force the connector to use IPv4 to avoid IP whitelisting errors.
    connector = aiohttp.TCPConnector(family=socket.AF_INET) if hasattr(socket, "AF_INET") else None

    api_factory = WebAssistantsFactory(
        throttler=throttler,
        auth=auth,
        rest_pre_processors=[
            TimeSynchronizerRESTPreProcessor(synchronizer=time_synchronizer, time_provider=time_provider),
        ])
    
    if connector:
        api_factory._conn = connector
        
    return api_factory


def build_api_factory_without_time_synchronizer_pre_processor(throttler: AsyncThrottler) -> WebAssistantsFactory:
    """
    Creates a web assistants factory without the time synchronizer pre-processor.
    """
    # We explicitly force the connector to use IPv4 to avoid IP whitelisting errors.
    connector = aiohttp.TCPConnector(family=socket.AF_INET) if hasattr(socket, "AF_INET") else None
    
    api_factory = WebAssistantsFactory(throttler=throttler)
    if connector:
        api_factory._conn = connector
        
    return api_factory


def create_throttler() -> AsyncThrottler:
    """
    Creates a throttler instance for the Biconomy exchange.
    """
    return AsyncThrottler(CONSTANTS.RATE_LIMITS)


async def get_current_server_time(throttler: Optional[AsyncThrottler] = None, domain: str = CONSTANTS.DEFAULT_DOMAIN) -> float:
    """
    Fetches the current server time from the exchange.
    """
    throttler = throttler or create_throttler()
    api_factory = build_api_factory_without_time_synchronizer_pre_processor(throttler=throttler)
    rest_assistant = await api_factory.get_rest_assistant()
    
    response = await rest_assistant.execute_request(
        url=public_rest_url(path_url=CONSTANTS.SERVER_TIME_PATH_URL),
        method=RESTMethod.GET,
        throttler_limit_id=CONSTANTS.SERVER_TIME_PATH_URL,
    )
    
    if isinstance(response, Dict) and "result" in response:
        return float(response["result"]) * 1e-3
    else:
        raise IOError(f"Error fetching server time. API response: {response}")

