import asyncio
from decimal import Decimal
import time
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple
import uuid

from async_timeout import timeout
from bidict import bidict

from hummingbot.connector.constants import s_decimal_NaN
from hummingbot.connector.exchange.biconomy import biconomy_constants as CONSTANTS, biconomy_web_utils as web_utils
from hummingbot.connector.exchange.biconomy.biconomy_api_order_book_data_source import BiconomyAPIOrderBookDataSource
from hummingbot.connector.exchange.biconomy.biconomy_api_user_stream_data_source import BiconomyAPIUserStreamDataSource
from hummingbot.connector.exchange.biconomy.biconomy_auth import BiconomyAuth
from hummingbot.connector.exchange_py_base import ExchangePyBase
from hummingbot.connector.trading_rule import TradingRule
from hummingbot.connector.utils import combine_to_hb_trading_pair
from hummingbot.core.data_type.cancellation_result import CancellationResult
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder, OrderState, OrderUpdate, TradeUpdate
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.trade_fee import AddedToCostTradeFee, TokenAmount, TradeFeeBase
from hummingbot.core.data_type.user_stream_tracker_data_source import UserStreamTrackerDataSource
from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.web_assistant.connections.data_types import RESTMethod
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory

if TYPE_CHECKING:
    from hummingbot.client.config.config_helpers import ClientConfigAdapter

s_logger = None


class BiconomyExchange(ExchangePyBase):
    SHORT_POLL_INTERVAL = 1.0
    LONG_POLL_INTERVAL = 1.0

    web_utils = web_utils

    def __init__(self,
                 client_config_map: "ClientConfigAdapter",
                 biconomy_api_key: str,
                 biconomy_api_secret: str,
                 trading_pairs: Optional[List[str]] = None,
                 trading_required: bool = True,
                 domain: str = CONSTANTS.DEFAULT_DOMAIN,
                 ):
        self.api_key = biconomy_api_key
        self.secret_key = biconomy_api_secret
        self._domain = domain
        self._trading_required = trading_required
        self._trading_pairs = trading_pairs
        super().__init__(client_config_map)

    @staticmethod
    def biconomy_order_type(order_type: OrderType) -> str:
        return order_type.name.upper()

    @staticmethod
    def to_hb_order_type(biconomy_type: str) -> OrderType:
        return OrderType[biconomy_type]

    @staticmethod
    def _get_client_order_id(is_buy: bool, trading_pair: str) -> str:
        """
        Creates a client order ID for a new order.
        """
        side = "B" if is_buy else "S"
        # The timestamp is in milliseconds, and the UUID ensures uniqueness.
        return f"hbot-{trading_pair}-{side}-{int(time.time() * 1000)}-{uuid.uuid4()}"

    @property
    def authenticator(self):
        return BiconomyAuth(
            api_key=self.api_key,
            secret_key=self.secret_key,
            time_provider=self._time_synchronizer)

    @property
    def name(self) -> str:
        return "biconomy"

    @property
    def rate_limits_rules(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def domain(self):
        return self._domain

    @property
    def client_order_id_max_length(self):
        return CONSTANTS.MAX_ORDER_ID_LEN

    @property
    def client_order_id_prefix(self):
        return CONSTANTS.HBOT_ORDER_ID_PREFIX

    @property
    def trading_rules_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def trading_pairs_request_path(self):
        return CONSTANTS.EXCHANGE_INFO_PATH_URL

    @property
    def check_network_request_path(self):
        return CONSTANTS.SERVER_TIME_PATH_URL

    @property
    def trading_pairs(self):
        return self._trading_pairs

    @property
    def is_cancel_request_in_exchange_synchronous(self) -> bool:
        return True

    @property
    def is_trading_required(self) -> bool:
        return self._trading_required

    def supported_order_types(self):
        return [OrderType.LIMIT, OrderType.MARKET]

    def _is_request_exception_related_to_time_synchronizer(self, request_exception: Exception):
        error_description = str(request_exception)
        return "Signature is not valid" in error_description

    def _is_order_not_found_during_status_update_error(self, status_update_exception: Exception) -> bool:
        return "order not exist" in str(status_update_exception)

    def _is_order_not_found_during_cancelation_error(self, cancelation_exception: Exception) -> bool:
        return "order not exist" in str(cancelation_exception)

    def _create_web_assistants_factory(self) -> WebAssistantsFactory:
        return web_utils.build_api_factory(
            throttler=self._throttler,
            time_synchronizer=self._time_synchronizer,
            auth=self.authenticator)

    def _create_order_book_data_source(self) -> OrderBookTrackerDataSource:
        return BiconomyAPIOrderBookDataSource(
            trading_pairs=self._trading_pairs,
            connector=self,
            domain=self.domain,
            api_factory=self._web_assistants_factory)

    def _create_user_stream_data_source(self) -> UserStreamTrackerDataSource:
        return BiconomyAPIUserStreamDataSource(
            auth=self.authenticator,
            trading_pairs=self._trading_pairs,
            connector=self,
            api_factory=self._web_assistants_factory,
            domain=self.domain,
        )

    def _get_fee(self,
                 base_currency: str,
                 quote_currency: str,
                 order_type: OrderType,
                 order_side: TradeType,
                 amount: Decimal,
                 price: Decimal = s_decimal_NaN,
                 is_maker: Optional[bool] = None) -> AddedToCostTradeFee:
        is_maker = order_type is OrderType.LIMIT_MAKER
        return AddedToCostTradeFee(percent=self.trade_fee_schema.maker_percent_fee_decimal if is_maker else self.trade_fee_schema.taker_percent_fee_decimal)

    async def _place_order(self,
                           order_id: str,
                           trading_pair: str,
                           amount: Decimal,
                           trade_type: TradeType,
                           order_type: OrderType,
                           price: Decimal,
                           **kwargs) -> Tuple[str, float]:
        amount_str = f"{amount:f}"
        price_str = f"{price:f}"
        type_str = BiconomyExchange.biconomy_order_type(order_type)
        side_str = "buy" if trade_type is TradeType.BUY else "sell"
        symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)

        request_body = {
            "symbol": symbol,
            "side": side_str,
            "type": type_str,
            "quantity": amount_str,
            "price": price_str,
            "clientOrderId": order_id
        }

        order_result = await self._api_post(
            path_url=CONSTANTS.ORDER_PATH_URL,
            data=request_body,
            is_auth_required=True)

        if order_result["code"] != 0:
            raise IOError(f"Error placing order: {order_result}")

        o_id = str(order_result["result"]["orderId"])
        transact_time = self.current_timestamp
        return (o_id, transact_time)

    async def _place_cancel(self, order_id: str, tracked_order: InFlightOrder):
        exchange_order_id = await tracked_order.get_exchange_order_id()
        request_body = {
            "orderId": int(exchange_order_id),
            "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=tracked_order.trading_pair)
        }
        await self._api_post(
            path_url=CONSTANTS.ORDER_PATH_URL,
            data=request_body,
            is_auth_required=True,
            limit_id=CONSTANTS.MANAGE_ORDER)

    async def _execute_order_cancel(self, order: InFlightOrder) -> str:
        try:
            await self._place_cancel(order.client_order_id, order)
            return order.client_order_id
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger().network(
                f"Failed to cancel order {order.client_order_id}. Error: {str(e)}",
                exc_info=True,
                app_warning_msg=f"Failed to cancel order {order.client_order_id}. Please check if the order has already been filled."
            )
            return ""

    async def cancel_all(self, timeout_seconds: float) -> List[CancellationResult]:
        incomplete_orders = [o for o in self.in_flight_orders.values() if not o.is_done]
        tasks = [self._place_cancel(o.client_order_id, o) for o in incomplete_orders]
        successful_cancellations = []
        failed_cancellations = []

        try:
            async with timeout(timeout_seconds):
                results = await safe_gather(*tasks, return_exceptions=True)
                for order, result in zip(incomplete_orders, results):
                    if not isinstance(result, Exception):
                        successful_cancellations.append(CancellationResult(order.client_order_id, True))
                    else:
                        failed_cancellations.append(CancellationResult(order.client_order_id, False))
        except asyncio.TimeoutError:
            self.logger().warning(f"Timeout while canceling all orders. Not all orders may have been canceled.")
            remaining_orders = [o for o in incomplete_orders if o not in [res.client_order_id for res in successful_cancellations]]
            for client_order_id in remaining_orders:
                failed_cancellations.append(CancellationResult(client_order_id, False))
        except Exception:
            self.logger().network(
                "Unexpected error cancelling orders.",
                exc_info=True,
                app_warning_msg="Failed to cancel order. Check API key and network connection."
            )

        return successful_cancellations + failed_cancellations

    async def _format_trading_rules(self, exchange_info_dict: List[Dict[str, Any]]) -> List[TradingRule]:
        trading_pair_rules = exchange_info_dict
        retval = []
        for rule in trading_pair_rules:
            try:
                trading_pair = await self.trading_pair_associated_to_exchange_symbol(symbol=rule.get("symbol"))

                min_order_size = Decimal(str(rule.get("minTradeQuantity")))
                min_price_increment = Decimal(str(10 ** -Decimal(rule.get("pricePrecision"))))
                min_base_amount_increment = Decimal(str(10 ** -Decimal(rule.get("baseAssetPrecision"))))
                min_notional_size = Decimal(str(rule.get("minTradeAmount")))

                retval.append(
                    TradingRule(trading_pair,
                                min_order_size=min_order_size,
                                min_price_increment=min_price_increment,
                                min_base_amount_increment=min_base_amount_increment,
                                min_notional_size=min_notional_size))

            except Exception:
                self.logger().exception(f"Error parsing the trading pair rule {rule}. Skipping.")
        return retval

    async def _update_trading_fees(self):
        pass

    async def _cancelled_order_handler(self, client_order_id: str, order_update: Optional[Dict[str, Any]]):
        pass

    async def _user_stream_event_listener(self):
        async for event_message in self._iter_user_event_queue():
            try:
                topic = event_message.get("topic")
                data = event_message.get("data")
                if topic == "balance":
                    balance_entry = data
                    asset_name = balance_entry["currency"].upper()
                    total_balance = Decimal(str(balance_entry["availableAmount"])) + Decimal(str(balance_entry["frozenAmount"]))
                    free_balance = Decimal(str(balance_entry["availableAmount"]))
                    self._account_available_balances[asset_name] = free_balance
                    self._account_balances[asset_name] = total_balance
                elif topic == "order":
                    order_update = data
                    client_order_id = order_update.get("clientOrderId")
                    tracked_order = self._order_tracker.fetch_order(client_order_id)
                    
                    if tracked_order:
                        new_state = CONSTANTS.ORDER_STATE.get(order_update.get("status"), OrderState.UNKNOWN)
                        update = OrderUpdate(
                            trading_pair=tracked_order.trading_pair,
                            update_timestamp=order_update["transactTime"] * 1e-3,
                            new_state=new_state,
                            client_order_id=client_order_id,
                            exchange_order_id=str(order_update["orderId"]),
                            executed_amount_base=Decimal(str(order_update.get("executedQty", "0"))),
                            executed_amount_quote=Decimal(str(order_update.get("cummulativeQuoteQty", "0")))
                        )
                        self._order_tracker.process_order_update(order_update=update)

            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error in user stream listener loop.", exc_info=True)
                await self._sleep(5.0)

    async def _update_orders(self):
        orders_to_update = [o for o in self.in_flight_orders.values() if not o.is_done]
        for order in orders_to_update:
            try:
                order_update = await self._request_order_status(tracked_order=order)
                if order_update:
                    self._order_tracker.process_order_update(order_update)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.logger().network(
                    f"Error fetching status update for order {order.client_order_id}: {str(e)}",
                    exc_info=True,
                    app_warning_msg=f"Failed to fetch status update for order {order.client_order_id}."
                )
    
    async def _all_trade_updates_for_order(self, order: InFlightOrder) -> List[TradeUpdate]:
        trade_updates = []
        if not order.is_done:
            return trade_updates

        exchange_order_id = await order.get_exchange_order_id()
        trading_pair = await self.exchange_symbol_associated_to_pair(trading_pair=order.trading_pair)
        
        response = await self._api_get(
            path_url=CONSTANTS.MY_TRADES_PATH_URL,
            params={
                "symbol": trading_pair,
                "orderId": int(exchange_order_id)
            },
            is_auth_required=True,
            limit_id=CONSTANTS.MANAGE_ORDER
        )
        
        if response.get("code") == 0 and "result" in response and "items" in response["result"]:
            for trade in response["result"]["items"]:
                fee_currency = trade.get("feeCurrency")
                fee_amount = Decimal(str(trade.get("fee", "0")))
                fee = AddedToCostTradeFee(
                    percent=self.trade_fee_schema.maker_percent_fee_decimal if trade.get("isMaker", False) else self.trade_fee_schema.taker_percent_fee_decimal,
                    percent_token=fee_currency,
                    flat_fees=[TokenAmount(amount=fee_amount, token=fee_currency)] if fee_amount > 0 else []
                )

                trade_update = TradeUpdate(
                    trade_id=str(trade.get("tradeId")),
                    client_order_id=order.client_order_id,
                    exchange_order_id=exchange_order_id,
                    trading_pair=order.trading_pair,
                    fee=fee,
                    fill_base_amount=Decimal(str(trade.get("quantity"))),
                    fill_quote_amount=Decimal(str(trade.get("quoteQty"))),
                    fill_price=Decimal(str(trade.get("price"))),
                    fill_timestamp=trade.get("time") * 1e-3,
                    is_maker=trade.get("isMaker", False)
                )
                trade_updates.append(trade_update)

        return trade_updates

    async def _request_order_status(self, tracked_order: InFlightOrder) -> Optional[OrderUpdate]:
        client_order_id = tracked_order.client_order_id
        exchange_order_id = await tracked_order.get_exchange_order_id()
        
        if not exchange_order_id:
            return None

        response = await self._api_post(
            path_url=CONSTANTS.ORDER_PATH_URL,
            params={
                "orderId": int(exchange_order_id)
            },
            is_auth_required=True,
            limit_id=CONSTANTS.MANAGE_ORDER
        )

        if response.get("code") == 0 and "result" in response and "result" in response["result"]:
            updated_order_data = response["result"]
            new_state = CONSTANTS.ORDER_STATE.get(updated_order_data.get("status"), OrderState.UNKNOWN)

            order_update = OrderUpdate(
                client_order_id=tracked_order.client_order_id,
                exchange_order_id=str(updated_order_data.get("orderId")),
                trading_pair=tracked_order.trading_pair,
                update_timestamp=updated_order_data.get("transactTime") * 1e-3,
                new_state=new_state,
                executed_amount_base=Decimal(str(updated_order_data.get("executedQty", "0"))),
                executed_amount_quote=Decimal(str(updated_order_data.get("cummulativeQuoteQty", "0")))
            )
            return order_update
        
        return None

    async def _update_balances(self):
        local_asset_names = set(self._account_balances.keys())
        remote_asset_names = set()

        account_info = await self._api_post(
            path_url=CONSTANTS.ACCOUNTS_PATH_URL,
            is_auth_required=True)

        if account_info.get("code") != 0 or "result" not in account_info:
            raise IOError(f"Error fetching account updates. API response: {account_info}")

        balances = account_info["result"]["assets"]
        for balance_entry in balances:
            asset_name = balance_entry.get("currency", "").upper()
            free_balance = Decimal(str(balance_entry.get("availableAmount", "0")))
            total_balance = free_balance + Decimal(str(balance_entry.get("frozenAmount", "0")))
            self._account_available_balances[asset_name] = free_balance
            self._account_balances[asset_name] = total_balance
            remote_asset_names.add(asset_name)

        asset_names_to_remove = local_asset_names.difference(remote_asset_names)
        for asset_name in asset_names_to_remove:
            del self._account_available_balances[asset_name]
            del self._account_balances[asset_name]

    def _initialize_trading_pair_symbols_from_exchange_info(self, exchange_info: List[Dict[str, Any]]) -> None:
        mapping = bidict()
        for symbol_data in exchange_info:
            if symbol_data.get("status") == "trading":
                mapping[symbol_data["symbol"]] = combine_to_hb_trading_pair(base=symbol_data["baseAsset"].upper(),
                                                                            quote=symbol_data["quoteAsset"].upper())
        self._set_trading_pair_symbol_map(mapping)

    async def _get_last_traded_price(self, trading_pair: str) -> float:
        params = {
            "symbol": await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
        }

        resp_json = await self._api_request(
            method=RESTMethod.GET,
            path_url=CONSTANTS.TICKER_PRICE_CHANGE_PATH_URL,
            params=params
        )

        if resp_json.get("code") != 0 or "result" not in resp_json:
            raise IOError(f"Error fetching last traded price for {trading_pair}. Response: {resp_json}")

        return float(resp_json["result"]["p"])

    async def _get_open_orders(self) -> List[InFlightOrder]:
        """
        Get all pending orders for the current spot trading pair.
        """
        all_open_orders = []
        for trading_pair in self._trading_pairs:
            symbol = await self.exchange_symbol_associated_to_pair(trading_pair=trading_pair)
            params = {
                "symbol": symbol
            }
            try:
                response = await self._api_post(
                    path_url=CONSTANTS.OPEN_ORDER_PATH_URL,
                    params=params,
                    is_auth_required=True,
                    limit_id=CONSTANTS.MANAGE_ORDER
                )
                if response.get("code") == 0 and "result" in response and "result" in response["result"]:
                    for order_data in response["result"]["items"]:
                        order_id = str(order_data.get("orderId"))
                        client_order_id = order_data.get("clientOrderId") or self._get_client_order_id(True, trading_pair)
                        
                        tracked_order = InFlightOrder(
                            client_order_id=client_order_id,
                            exchange_order_id=order_id,
                            trading_pair=trading_pair,
                            order_type=self.to_hb_order_type(order_data.get("type")),
                            trade_type=TradeType.BUY if order_data.get("side") == "buy" else TradeType.SELL,
                            price=Decimal(str(order_data.get("price"))),
                            amount=Decimal(str(order_data.get("origQty"))),
                            creation_timestamp=order_data.get("transactTime") * 1e-3,
                            status=CONSTANTS.ORDER_STATE.get(order_data.get("status", OrderState.UNKNOWN))
                        )
                        all_open_orders.append(tracked_order)
            except Exception as e:
                self.logger().network(f"Error fetching open orders for {trading_pair}: {str(e)}", exc_info=True)
        return all_open_orders

