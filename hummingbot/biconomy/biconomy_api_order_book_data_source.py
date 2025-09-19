import asyncio
import time
import aiohttp
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from hummingbot.connector.exchange.biconomy import biconomy_constants as CONSTANTS, biconomy_web_utils as web_utils
from hummingbot.connector.exchange.biconomy.biconomy_order_book import BiconomyOrderBook
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, WSJSONRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory
from hummingbot.core.web_assistant.ws_assistant import WSAssistant
from hummingbot.logger import HummingbotLogger

if TYPE_CHECKING:
    from hummingbot.connector.exchange.biconomy.biconomy_exchange import biconomyExchange


class BiconomyAPIOrderBookDataSource(OrderBookTrackerDataSource):
    # This class name should be capitalized to follow Python conventions.
    
    TRADE_STREAM_ID = 1
    DIFF_STREAM_ID = 2
    ONE_HOUR = 60 * 60

    _logger: Optional[HummingbotLogger] = None

    def __init__(self,
                 trading_pairs: List[str],
                 connector: "biconomyExchange",
                 api_factory: WebAssistantsFactory,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN):
        super().__init__(trading_pairs)
        self._connector = connector
        # The constants for queue keys need to be defined. The XT template used constants
        # but the Biconomy CEX docs use different event names for the WebSocket stream.
        self._trade_messages_queue_key = "trade"
        self._diff_messages_queue_key = "depth_update"
        self._domain = domain
        self._api_factory = api_factory

    async def get_last_traded_prices(self,
                                     trading_pairs: List[str],
                                     domain: Optional[str] = None) -> Dict[str, float]:
        # This method seems to be correctly implemented by deferring to the connector.
        return await self._connector.get_last_traded_prices(trading_pairs=trading_pairs)

    async def _request_order_book_snapshot(self, trading_pair: str) -> Dict[str, Any]:
        """
        Retrieves a copy of the full order book from the exchange, for a particular trading pair.
        :param trading_pair: the trading pair for which the order book will be retrieved
        :return: the response from the exchange (JSON dictionary)
        """
        params = {
            "symbol": await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair),
        }
        
        # The Biconomy API docs for the depth endpoint do not show a 'limit' parameter.
        # We will remove it to avoid errors.
        
        rest_assistant = await self._api_factory.get_rest_assistant()
        data = await rest_assistant.execute_request(
            url=web_utils.public_rest_url(path_url=CONSTANTS.SNAPSHOT_PATH_URL),
            params=params,
            method=RESTMethod.GET,
            throttler_limit_id=CONSTANTS.SNAPSHOT_PATH_URL,
        )

        # Biconomy's API documentation shows the bids and asks are at the top level, not
        # nested under a "result" key. We'll return the full data object.
        return data

    async def listen_for_subscriptions(self):
        """
        Connects to the trade events and order diffs websocket endpoints and listens to the messages sent by the
        exchange. Each message is stored in its own queue.
        """
        ws: Optional[WSAssistant] = None
        while True:
            try:
                ws: WSAssistant = await self._connected_websocket_assistant()
                await self._subscribe_channels(ws)
                await self._process_websocket_messages(websocket_assistant=ws)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Catch specific aiohttp errors for better logging
                if isinstance(e, aiohttp.ClientError):
                    self.logger().error(f"WebSocket connection error: {e}")
                else:
                    self.logger().exception(
                        "Unexpected error occurred when listening to order book streams. Retrying in 5 seconds...",
                    )
                await self._sleep(5.0)  # Changed sleep to 5 seconds as this is a more standard retry time
            finally:
                ws and await ws.disconnect()

    async def _subscribe_channels(self, ws: WSAssistant):
        """
        Subscribes to the trade events and diff orders events through the provided websocket connection.
        :param ws: the websocket assistant used to connect to the exchange
        """
        try:
            trade_params = []
            depth_params = []
            for trading_pair in self._trading_pairs:
                symbol = await self._connector.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
                # Biconomy CEX WebSocket streams are named with a colon (:) not an @
                trade_params.append(f"trade:{symbol.lower()}")
                depth_params.append(f"depth_update:{symbol.lower()}")

            # The method name for subscription is case-sensitive. The documentation uses lowercase "subscribe".
            payload_trade = {
                "method": "subscribe",
                "params": trade_params,
                "id": self.TRADE_STREAM_ID
            }
            subscribe_trade_request: WSJSONRequest = WSJSONRequest(payload=payload_trade)

            payload_depth = {
                "method": "subscribe",
                "params": depth_params,
                "id": self.DIFF_STREAM_ID
            }
            subscribe_orderbook_request: WSJSONRequest = WSJSONRequest(payload=payload_depth)

            await ws.send(subscribe_trade_request)
            await ws.send(subscribe_orderbook_request)

            self.logger().info("Subscribed to public order book and trade channels...")
        except asyncio.CancelledError:
            raise
        except Exception:
            self.logger().error(
                "Unexpected error occurred subscribing to order book trading and delta streams...",
                exc_info=True
            )
            raise

    async def _connected_websocket_assistant(self) -> WSAssistant:
        ws: WSAssistant = await self._api_factory.get_ws_assistant()
        # The Biconomy WebSocket URL does not require a domain parameter like XT.com.
        # It's a static URL, so we can remove the .format(self._domain) part.
        await ws.connect(ws_url=CONSTANTS.WSS_URL_PUBLIC,
                         ping_timeout=CONSTANTS.WS_HEARTBEAT_TIME_INTERVAL)
        return ws

    async def _order_book_snapshot(self, trading_pair: str) -> OrderBookMessage:
        snapshot: Dict[str, Any] = await self._request_order_book_snapshot(trading_pair)
        snapshot_timestamp: float = time.time()
        # The snapshot data is not nested under "result", as identified above.
        snapshot_msg: OrderBookMessage = BiconomyOrderBook.snapshot_message_from_exchange(
            snapshot,
            snapshot_timestamp,
            metadata={"trading_pair": trading_pair}
        )
        return snapshot_msg

    async def _parse_trade_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if raw_message and "topic" in raw_message and "data" in raw_message:
            # We need to correctly parse the symbol from the topic string. The topic format is "trade:<symbol>".
            # The data also contains the symbol, but parsing from the topic is often more robust.
            topic = raw_message["topic"]
            parts = topic.split(":")
            if len(parts) == 2 and parts[0] == "trade":
                exchange_symbol = parts[1].upper()
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=exchange_symbol)
                trade_data = raw_message["data"]
                trade_message = BiconomyOrderBook.trade_message_from_exchange(
                    trade_data, {"trading_pair": trading_pair})
                message_queue.put_nowait(trade_message)

    async def _parse_order_book_diff_message(self, raw_message: Dict[str, Any], message_queue: asyncio.Queue):
        if raw_message and "topic" in raw_message and "data" in raw_message:
            # We need to correctly parse the symbol from the topic string.
            topic = raw_message["topic"]
            parts = topic.split(":")
            if len(parts) == 2 and parts[0] == "depth_update":
                exchange_symbol = parts[1].upper()
                trading_pair = await self._connector.trading_pair_associated_to_exchange_symbol(symbol=exchange_symbol)
                diff_data = raw_message["data"]
                order_book_message: OrderBookMessage = BiconomyOrderBook.diff_message_from_exchange(
                    diff_data, time.time(), {"trading_pair": trading_pair})
                message_queue.put_nowait(order_book_message)

    def _channel_originating_message(self, event_message: Dict[str, Any]) -> str:
        # We'll use the "topic" key to identify the channel, as seen in the Biconomy documentation.
        channel = ""
        if "topic" in event_message:
            event_type = event_message["topic"].split(":")[0]
            if event_type == CONSTANTS.DIFF_EVENT_TYPE:
                channel = self._diff_messages_queue_key
            elif event_type == CONSTANTS.TRADE_EVENT_TYPE:
                channel = self._trade_messages_queue_key
        return channel

