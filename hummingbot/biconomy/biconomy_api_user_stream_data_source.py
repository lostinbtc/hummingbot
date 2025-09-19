import asyncio
import time
import aiohttp
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.biconomy import biconomy_constants as CONSTANTS, biconomy_web_utils as web_utils
from hummingbot.connector.exchange.biconomy.biconomy_auth import BiconomyAuth
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.biconomy.biconomy_exchange import BiconomyExchange


class BiconomyAPIUserStreamDataSource(UserStreamTrackerDataSource):

    USER_STREAM_ID = 1

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 auth: BiconomyAuth,
                 trading_pairs: List[str],
                 connector: "BiconomyExchange",
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__()
        self._auth: BiconomyAuth = auth
        self._current_listen_key = None
        self._domain = domain
        self._api_factory = api_factory

    async def listen_for_user_stream(self, output: asyncio.Queue):
        """
        Connects to the user private channel in the exchange using a websocket connection.
        With the established connection listens to all balance events and order updates
        provided by the exchange, and stores them in the output queue.
        """
        while True:
            try:
                self._ws_assistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(websocket_assistant=self._ws_assistant)
                self.logger().info("Subscribed to private account and orders channels...")
                await self._ws_assistant.ping()
                await self._process_websocket_messages(websocket_assistant=self._ws_assistant, queue=output)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Catch specific aiohttp errors for better logging
                if isinstance(e, aiohttp.ClientError):
                    self.logger().error(f"WebSocket connection error: {e}")
                else:
                    self.logger().exception("Unexpected error while listening to user stream. Retrying after 5 seconds...")
                await self._sleep(5.0)
            finally:
                await self._on_user_stream_interruption(websocket_assistant=self._ws_assistant)
                self._ws_assistant = None

    async def _connected_websocket_assistant(self) -> WSAssistant:
        """
        Creates an instance of WSAssistant connected to the exchange
        """
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        # The Biconomy CEX private WebSocket URL does not require a domain.
        await ws.connect(ws_url=CONSTANTS.WSS_URL_PRIVATE, ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        self.logger().info("Connected to Biconomy Private WebSocket")
        return ws

    async def _subscribe_channels(self, websocket_assistant: WSAssistant):
        """
        Subscribes to the user private channels with the authenticated websocket connection.
        """
        try:
            # The Biconomy CEX API documentation does not mention a "listenKey" for
            # the private WebSocket streams. Instead, it seems to rely on a
            # simple subscription with the provided API key for private WS.
            # We will use the common Hummingbot pattern of using the API key in the
            # subscription payload.
            payload = {
                "op": "sub",
                "args": ["balance", "order"],
                "api_key": self._auth.api_key  # Assuming the API key is accessible here
            }
            subscribe_request: WSJSONRequest = WSJSONRequest(payload=payload)
            await websocket_assistant.send(subscribe_request)
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().exception("Unexpected error occurred subscribing to private account and order streams...")
            raise

    async def _process_websocket_messages(self, websocket_assistant: WSAssistant, queue: asyncio.Queue):
        async for ws_response in websocket_assistant.iter_messages():
            data = ws_response.data
            await self._process_event_message(event_message=data, queue=queue)

    async def _process_event_message(self, event_message: Dict[str, Any], queue: asyncio.Queue):
        if event_message and "topic" in event_message and "data" in event_message:
            queue.put_nowait(event_message)

    async def _on_user_stream_interruption(self, websocket_assistant: Optional[WSAssistant]):
        await super()._on_user_stream_interruption(websocket_assistant=websocket_assistant)
        self._current_listen_key = None

