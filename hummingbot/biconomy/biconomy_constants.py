import hummingbot.connector.exchange.xt.xt_constants as CONSTANTS
from hummingbot.core.data_type.in_flight_order import OrderState
from hummingbot.core.api_throttler.data_types import RateLimit

DEFAULT_DOMAIN = "com"

HBOT_ORDER_ID_PREFIX = "biconomy_"
MAX_ORDER_ID_LEN = 32

# Base URL
REST_URL = "https://www.biconomy.com/api/v1"
WSS_URL_PUBLIC = "wss://bei.biconomy.com/ws"
WSS_URL_PRIVATE = "wss://bei.biconomy.com/ws"

# Public REST API endpoints
TICKER_PRICE_CHANGE_PATH_URL = "/tickers/price"
EXCHANGE_INFO_PATH_URL = "/exchangeInfo"
SNAPSHOT_PATH_URL = "/depth"
SERVER_TIME_PATH_URL = "/time"

# Private REST API endpoints
ACCOUNTS_PATH_URL = "/private/user"
MY_TRADES_PATH_URL = "/private/trade"
ORDER_PATH_URL = "/private/order"
OPEN_ORDER_PATH_URL = "/private/order/open"
GET_ACCOUNT_LISTENKEY = "/ws-token"

WS_HEARTBEAT_TIME_INTERVAL = 15

# Websocket event types
DIFF_EVENT_TYPE = "depth"
TRADE_EVENT_TYPE = "trade"


# Biconomy params
SIDE_BUY = 'BUY'
SIDE_SELL = 'SELL'

TIME_IN_FORCE_GTC = 'GTC'
TIME_IN_FORCE_IOC = 'IOC'
TIME_IN_FORCE_FOK = 'FOK'

XT_VALIDATE_ALGORITHMS = "HmacSHA256"
XT_VALIDATE_RECVWINDOW = "5000"
XT_VALIDATE_CONTENTTYPE_URLENCODE = "application/x-www-form-urlencoded"
XT_VALIDATE_CONTENTTYPE_JSON = "application/json;charset=UTF-8"


# Order States
ORDER_STATE = {
    "PENDING": OrderState.PENDING_CREATE,
    "NEW": OrderState.OPEN,
    "FILLED": OrderState.FILLED,
    "PARTIALLY_FILLED": OrderState.PARTIALLY_FILLED,
    "PENDING_CANCEL": OrderState.OPEN,
    "CANCELED": OrderState.CANCELED,
    "REJECTED": OrderState.FAILED,
    "EXPIRED": OrderState.FAILED,
}


# Rate Limit time intervals
ONE_MINUTE = 60
ONE_SECOND = 1
ONE_DAY = 86400

SINGLE_SYMBOL = 100
MULTIPLE_SYMBOLS = 10

# A single rate limit id for managing orders: GET open-orders, order/trade details, DELETE cancel order.
MANAGE_ORDER = "ManageOrder"

RATE_LIMITS = [
    RateLimit(limit_id=TICKER_PRICE_CHANGE_PATH_URL, limit=SINGLE_SYMBOL, time_interval=ONE_SECOND),
    RateLimit(limit_id=EXCHANGE_INFO_PATH_URL, limit=MULTIPLE_SYMBOLS, time_interval=ONE_SECOND),
    RateLimit(limit_id=SNAPSHOT_PATH_URL, limit=2 * SINGLE_SYMBOL, time_interval=ONE_SECOND),
    RateLimit(limit_id=SERVER_TIME_PATH_URL, limit=100, time_interval=ONE_SECOND),
    RateLimit(limit_id=ACCOUNTS_PATH_URL, limit=10, time_interval=ONE_SECOND),
    RateLimit(limit_id=MANAGE_ORDER, limit=500, time_interval=ONE_SECOND),
    RateLimit(limit_id=ORDER_PATH_URL, limit=50, time_interval=ONE_SECOND),
    RateLimit(limit_id=GET_ACCOUNT_LISTENKEY, limit=10, time_interval=ONE_SECOND)
]
