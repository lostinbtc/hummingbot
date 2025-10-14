import aiohttp
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTResponse

class RESTConnection:
    def __init__(self, aiohttp_client_session: aiohttp.ClientSession):
        self._client_session = aiohttp_client_session

    async def call(self, request: RESTRequest) -> RESTResponse:
        """
        Execute the REST request via aiohttp with safe URL handling.
        Only disables compression decoding for Biconomy endpoints.
        Compatible with both Enum and string HTTP methods.
        """
        url = request.url
        if isinstance(url, (tuple, list)):
            url = url[0]
        if not isinstance(url, str):
            url = str(url)

        disable_compression = "biconomy" in url.lower() or "bico" in url.lower()

        method = request.method.value if hasattr(request.method, "value") else request.method

        aiohttp_resp = await self._client_session.request(
            method=method,
            url=url,
            params=request.params,
            data=request.data,
            headers=request.headers,
            compress=None if disable_compression else True,
            allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=30),
            auto_decompress=not disable_compression,
        )

        return RESTResponse(aiohttp_resp)

