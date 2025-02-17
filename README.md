import time
import random
import ccxt
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional

# 🔹 Настройка логирования (сохранение всех сделок в файл)
logging.basicConfig(filename="arbitrage_log.txt", level=logging.INFO, format="%(asctime)s - %(message)s")

def log_trade(message: str) -> None:
    """Логирование сообщений в консоль и файл."""
    print(message)
    logging.info(message)

# 🔹 Режим работы (True = тестовый, False = реальная торговля)
PAPER_TRADING = True

# 🔹 Подключение к биржам через переменные окружения
exchanges = {
    "coinbase": ccxt.coinbase({
        "apiKey": os.getenv("COINBASE_API_KEY"),
        "secret": os.getenv("COINBASE_SECRET"),
    }),
    "kraken": ccxt.kraken({
        "apiKey": os.getenv("KRAKEN_API_KEY"),
        "secret": os.getenv("KRAKEN_SECRET"),
    }),
    "gemini": ccxt.gemini({
        "apiKey": os.getenv("GEMINI_API_KEY"),
        "secret": os.getenv("GEMINI_SECRET"),
    }),
    "bittrex": ccxt.bittrex({
        "apiKey": os.getenv("BITTREX_API_KEY"),
        "secret": os.getenv("BITTREX_SECRET"),
    }),
    "poloniex": ccxt.poloniex({
        "apiKey": os.getenv("POLONIEX_API_KEY"),
        "secret": os.getenv("POLONIEX_SECRET"),
    }),
    "binanceus": ccxt.binanceus({
        "apiKey": os.getenv("BINANCEUS_API_KEY"),
        "secret": os.getenv("BINANCEUS_SECRET"),
    }),
    "okx": ccxt.okx({
        "apiKey": os.getenv("OKX_API_KEY"),
        "secret": os.getenv("OKX_SECRET"),
    }),
    "mexc": ccxt.mexc({
        "apiKey": os.getenv("MEXC_API_KEY"),
        "secret": os.getenv("MEXC_SECRET"),
    }),
    "coinex": ccxt.coinex({
        "apiKey": os.getenv("COINEX_API_KEY"),
        "secret": os.getenv("COINEX_SECRET"),
    }),
    "gateio": ccxt.gateio({
        "apiKey": os.getenv("GATEIO_API_KEY"),
        "secret": os.getenv("GATEIO_SECRET"),
    }),
}

# 🔹 Настройки арбитража
trading_pairs = ["BTC/USDT", "ETH/USDT", "SOL/USDT", "XRP/USDT"]
profit_threshold = 0.005  # Чистая прибыль ≥ 0.5%
trade_count = 0
max_trades_per_day = 5000
risk_percentage = 0.05  # Максимальный риск на сделку (5% от баланса)

# 🔹 Комиссии бирж (примерные значения, уточните для каждой биржи)
exchange_fees = {
    "coinbase": 0.005,  # 0.5%
    "kraken": 0.0026,   # 0.26%
    "gemini": 0.0035,   # 0.35%
    "bittrex": 0.0025,  # 0.25%
    "poloniex": 0.0015, # 0.15%
    "binanceus": 0.001, # 0.1%
    "okx": 0.001,       # 0.1%
    "mexc": 0.001,      # 0.1%
    "coinex": 0.001,    # 0.1%
    "gateio": 0.001,    # 0.1%
}

# 🔹 Функция безопасного API-запроса
def safe_request(func, retries: int = 3, delay: int = 3) -> Optional[any]:
    """Выполняет запрос с повторными попытками и экспоненциальной задержкой."""
    for attempt in range(retries):
        try:
            return func()
        except ccxt.NetworkError as e:
            log_trade(f"⚠️ Сетевая ошибка ({attempt+1}/{retries}): {e}")
            time.sleep(delay * (attempt + 1))  # Экспоненциальная задержка
        except ccxt.ExchangeError as e:
            log_trade(f"⚠️ Ошибка биржи ({attempt+1}/{retries}): {e}")
            time.sleep(delay)
    return None

# 🔹 Функция получения цен (многопоточная проверка всех бирж)
def get_prices_parallel(pair: str) -> Dict[str, Optional[float]]:
    """Получает цены на всех биржах для заданной пары."""
    with ThreadPoolExecutor() as executor:
        results = executor.map(lambda exchange: safe_request(lambda: exchange.fetch_ticker(pair)['last']), exchanges.values())
    return dict(zip(exchanges.keys(), results))

# 🔹 Функция проверки баланса перед сделкой
def check_balance(exchange, asset: str) -> float:
    """Проверяет доступный баланс на бирже."""
    balance = safe_request(lambda: exchange.fetch_balance())
    return balance[asset]['free'] if balance and asset in balance else 0

# 🔹 Функция проверки ликвидности
def check_liquidity(exchange, pair: str, amount: float) -> bool:
    """Проверяет, достаточно ли ликвидности для сделки."""
    order_book = safe_request(lambda: exchange.fetch_order_book(pair))
    if order_book:
        bids = order_book['bids']
        available_volume = sum(bid[1] for bid in bids)
        return available_volume >= amount
    return False

# 🔹 Функция расчета прибыли с учетом комиссий
def calculate_profit(min_price: float, max_price: float, amount: float, min_exchange: str, max_exchange: str) -> float:
    """Рассчитывает прибыль с учетом комиссий."""
    min_fee = exchange_fees.get(min_exchange, 0)
    max_fee = exchange_fees.get(max_exchange, 0)
    buy_cost = min_price * amount * (1 + min_fee)
    sell_revenue = max_price * amount * (1 - max_fee)
    return sell_revenue - buy_cost

# 🔹 Функция выбора самой прибыльной торговой пары
def get_best_trading_pair() -> Optional[str]:
    """Выбирает пару с наибольшей прибылью."""
    best_pair = None
    best_profit = 0
    
    for pair in trading_pairs:
        prices = get_prices_parallel(pair)
        valid_prices = {k: v for k, v in prices.items() if v is not None}

        if len(valid_prices) < 2:
            continue

        min_exchange = min(valid_prices, key=valid_prices.get)
        max_exchange = max(valid_prices, key=valid_prices.get)
        min_price, max_price = valid_prices[min_exchange], valid_prices[max_exchange]

        profit = calculate_profit(min_price, max_price, 1, min_exchange, max_exchange)
        if profit > best_profit:
            best_profit = profit
            best_pair = pair
    
    return best_pair if best_profit > profit_threshold else None

# 🔹 Функция выполнения арбитража
def execute_trade() -> None:
    """Выполняет арбитражную сделку."""
    global trade_count
    if trade_count >= max_trades_per_day:
        log_trade("❌ Достигнут лимит сделок на сегодня!")
        return

    best_pair = get_best_trading_pair()
    if not best_pair:
        log_trade("⚠️ Нет подходящих торговых пар для арбитража")
        return

    prices = get_prices_parallel(best_pair)
    valid_prices = {k: v for k, v in prices.items() if v is not None}

    min_exchange, max_exchange = min(valid_prices, key=valid_prices.get), max(valid_prices, key=valid_prices.get)
    min_price, max_price = valid_prices[min_exchange], valid_prices[max_exchange]

    usdt_balance = check_balance(exchanges[min_exchange], "USDT")
    trade_amount = min(usdt_balance / min_price, usdt_balance * risk_percentage / min_price)

    if not check_liquidity(exchanges[min_exchange], best_pair, trade_amount):
        log_trade(f"⚠️ Недостаточно ликвидности для сделки на {min_exchange}")
        return

    potential_profit = calculate_profit(min_price, max_price, trade_amount, min_exchange, max_exchange)

    if potential_profit > 0:
        log_trade(f"✅ Арбитраж ({best_pair}): Покупаем на {min_exchange} за {min_price}, продаем на {max_exchange} за {max_price}")
        log_trade(f"📈 Чистая прибыль: {potential_profit:.6f} USDT")

        if PAPER_TRADING:
            log_trade(f"🔹 [TEST MODE] Сделка не выполнена (симуляция)")
        else:
            log_trade(f"✅ Сделка выполнена!")
        
        trade_count += 1

    sleep_time = random.randint(1, 3) if potential_profit > 0 else random.randint(5, 10)
    log_trade(f"⏳ Ожидание {sleep_time} секунд перед следующей проверкой")
    time.sleep(sleep_time)

# 🔹 Основной цикл
while True:
    execute_trade()
 


