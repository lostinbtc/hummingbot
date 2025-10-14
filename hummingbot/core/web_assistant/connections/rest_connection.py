import aiohttp
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTResponse


class RESTConnection:
    def __init__(self, aiohttp_client_session: aiohttp.ClientSession):
        self._client_session = aiohttp_client_session

    async def call(self, request: RESTRequest) -> RESTResponse:
        """
        Execute the REST request via aiohttp with safe URL handling.
        Disables zstd compression decoding for exchanges like Biconomy.
        """
        url = request.url
        if isinstance(url, (tuple, list)):
            url = url[0]
        if not isinstance(url, str):
            url = str(url)

        aiohttp_resp = await self._client_session.request(
            method=request.method.value,
            url=url,
            params=request.params,
            data=request.data,
            headers=request.headers,
            compress=None,                 
            allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=30),
            auto_decompress=False,        
        )

        resp = RESTResponse(aiohttp_resp)
        return resp
