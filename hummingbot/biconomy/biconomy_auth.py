import hashlib
import time
from collections import OrderedDict
from typing import Dict, Awaitable, Any
from urllib.parse import urlencode, urlparse

from hummingbot.connector.exchange.biconomy import biconomy_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class BiconomyAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key.encode("utf8")
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the signature to the request body as required by Biconomy's API.
        """
        params_to_sign = {}
        if request.params:
            params_to_sign.update(request.params)
        
        # Add authentication parameters to the payload
        params_to_sign["api_key"] = self.api_key
        params_to_sign["timestamp"] = str(int(self.time_provider.time() * 1e3))
        
        # Sort the parameters alphabetically for the signature string
        sorted_params = OrderedDict(sorted(params_to_sign.items()))
        
        # Generate the signature
        signature = self._generate_signature(sorted_params)
        
        # Add the signature to the final request data
        request.data = {
            **params_to_sign,
            "sign": signature,
        }

        # Biconomy requires content type for POST requests
        request.headers = {"Content-Type": "application/x-www-form-urlencoded"}

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        # Biconomy's private WebSocket streams are authenticated via the API key and a
        # dedicated subscription message. This method remains a pass-through.
        return request

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        """
        Generates the MD5 signature for the request.
        """
        # The Biconomy API requires a specific signature format: MD5(params_str + secret_key)
        params_str = urlencode(params)
        string_to_sign = params_str + self.secret_key.decode("utf8")
        return hashlib.md5(string_to_sign.encode("utf8")).hexdigest().upper()

