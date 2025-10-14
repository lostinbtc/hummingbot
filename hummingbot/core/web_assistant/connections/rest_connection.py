import aiohttp
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, RESTResponse


class RESTConnection:
    def __init__(self, aiohttp_client_session: aiohttp.ClientSession):
        self._client_session = aiohttp_client_session

    async def call(self, request: RESTRequest) -> RESTResponse:
        """
        Execute the REST request via aiohttp with defensive URL handling
        to ensure it is always a valid string and disable zstd compression.
        """
        url = request.url
        if isinstance(url, (tuple, list)):
            url = url[0]
        if not isinstance(url, str):
            url = str(url)

        method_val = getattr(request.method, "value", request.method)

        headers = {**(request.headers or {}), "Accept-Encoding": "identity"}

        aiohttp_resp = await self._client_session.request(
            method=method_val,
            url=url,
            params=request.params,
            data=request.data,
            headers=headers,
        )

        resp = await self._build_resp(aiohttp_resp)
        return resp

    @staticmethod
    async def _build_resp(aiohttp_resp: aiohttp.ClientResponse) -> RESTResponse:
        """
        Wraps aiohttp.ClientResponse into RESTResponse.
        """
        resp = RESTResponse(aiohttp_resp)
        return resp
