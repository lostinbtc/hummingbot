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

        # --- Normalize URL safely ---
        url = request.url
        if isinstance(url, (tuple, list)):
            url = url[0]
        if not isinstance(url, str):
            url = str(url)

        # --- Normalize method safely (accepts enum or string) ---
        method_value = (
            request.method.value
            if hasattr(request.method, "value")
            else str(request.method)
        )

        # --- Perform HTTP request ---
        aiohttp_resp = await self._client_session.request(
            method=method_value,
            url=url,
            params=request.params,
            data=request.data,
            headers=request.headers,
            compress=None,                  # prevent aiohttp auto-compression
            allow_redirects=True,
            timeout=aiohttp.ClientTimeout(total=30),
            auto_decompress=False,          # disable zstd/deflate decoding
        )

        return RESTResponse(aiohttp_resp)
