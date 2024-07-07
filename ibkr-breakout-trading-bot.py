import datetime
import threading
import csv
import random
import sqlite3
import time
import yfinance as yf
import pandas as pd
import argparse
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.order import Order

class BreakoutTradingBot(EWrapper, EClient):
    def __init__(self, is_armed=False):
        EClient.__init__(self, self)
        self.nextOrderId = None
        self.positions = {}
        self.symbols = []
        self.historical_data = {}
        self.current_day_data = {}
        self.timeframe = "5m"
        self.max_investment = 200
        self.db_connection = sqlite3.connect('trades.db')
        self.create_trades_table()
        self.is_armed = is_armed
        self.stop_event = threading.Event()

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextOrderId = orderId
        print("NextValidId:", orderId)

    def error(self, reqId, errorCode, errorString):
        print("Error:", reqId, errorCode, errorString)

    def run_loop(self):
        self.run()

    def start(self):
        self.connect("127.0.0.1", 7497, 0)
        api_thread = threading.Thread(target=self.run_loop, daemon=True)
        api_thread.start()
        print("IBKR API started")

    def stop(self):
        self.stop_event.set()
        self.disconnect()
        self.db_connection.close()
        print("IBKR API stopped and database connection closed")

    def create_trades_table(self):
        cursor = self.db_connection.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                order_type TEXT,
                order_time TIMESTAMP,
                price REAL,
                quantity INTEGER,
                reason TEXT
            )
        ''')
        self.db_connection.commit()

    def log_order(self, symbol, order_type, price, quantity, reason):
        cursor = self.db_connection.cursor()
        cursor.execute('''
            INSERT INTO trades (symbol, order_type, order_time, price, quantity, reason)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (symbol, order_type, datetime.datetime.now(), price, quantity, reason))
        self.db_connection.commit()

    def load_symbols(self, filename='stocks.csv'):
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            self.symbols = [row['Symbol'] for row in reader]
        print(f"Loaded {len(self.symbols)} symbols from {filename}")

    def request_historical_data(self, symbol, days=2):
        end_date = datetime.datetime.now()
        start_date = end_date - datetime.timedelta(days=days)
        
        try:
            data = yf.download(symbol, start=start_date, end=end_date, interval=self.timeframe)
            if not data.empty:
                self.historical_data[symbol] = data
                print(f"Downloaded historical data for {symbol}")
            else:
                print(f"No historical data available for {symbol}")
        except Exception as e:
            print(f"Error downloading data for {symbol}: {e}")

    def fetch_real_time_data(self, symbol):
        try:
            end_date = datetime.datetime.now()
            start_date = end_date - datetime.timedelta(minutes=5)
            data = yf.download(symbol, start=start_date, end=end_date, interval="1m")
            if not data.empty:
                return data.iloc[-1]
            else:
                print(f"No real-time data available for {symbol}")
                return None
        except Exception as e:
            print(f"Error fetching real-time data for {symbol}: {e}")
            return None

    def calculate_average_candle_size(self, symbol):
        if symbol in self.historical_data:
            data = self.historical_data[symbol]
            sizes = data['High'] - data['Low']
            return sizes.mean()
        return 0

    def check_breakout(self, symbol, current_candle):
        if symbol not in self.current_day_data or symbol not in self.historical_data:
            return False

        data = self.historical_data[symbol]
        opening_15min = data.iloc[0]
        prev_day_close = data.iloc[-1]['Close']

        if opening_15min['High'] <= prev_day_close:
            return False

        opening_range = opening_15min['High'] - opening_15min['Low']
        avg_candle_size = self.calculate_average_candle_size(symbol)

        if (current_candle['Close'] > opening_15min['High'] and
            current_candle['High'] - current_candle['Low'] >= 2 * avg_candle_size):
            return True

        return False

    def place_order(self, symbol, action, quantity, price=None):
        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"

        order = Order()
        order.action = action
        order.totalQuantity = quantity
        order.orderType = "MKT" if price is None else "LMT"
        if price:
            order.lmtPrice = price

        self.placeOrder(self.nextOrderId, contract, order)
        self.nextOrderId += 1

        # Log the order
        if action == "BUY":
            self.log_order(symbol, "BUY", price if price else self.current_day_data[symbol].iloc[-1]['Close'], quantity, "Breakout found")
        elif action == "SELL":
            reason = self.positions[symbol]['exit_reason'] if symbol in self.positions else "Unknown"
            self.log_order(symbol, "SELL", price if price else self.current_day_data[symbol].iloc[-1]['Close'], quantity, reason)

    def manage_trade(self, symbol):
        if symbol not in self.positions:
            return

        position = self.positions[symbol]
        current_price = self.current_day_data[symbol].iloc[-1]['Close']

        if current_price >= position['target']:
            self.positions[symbol]['exit_reason'] = "Target reached"
            self.place_order(symbol, "SELL", position['quantity'])
            del self.positions[symbol]
            print(f"Target reached. Closing position for {symbol}")
        elif current_price <= position['stop_loss']:
            self.positions[symbol]['exit_reason'] = "Stop loss hit"
            self.place_order(symbol, "SELL", position['quantity'])
            del self.positions[symbol]
            print(f"Stop loss hit. Closing position for {symbol}")

    def on_new_candle(self, symbol, candle):
        if symbol not in self.current_day_data:
            self.current_day_data[symbol] = pd.DataFrame()
        
        new_row = pd.DataFrame([candle])
        self.current_day_data[symbol] = pd.concat([self.current_day_data[symbol], new_row], ignore_index=True)

        if self.check_breakout(symbol, candle):
            opening_15min = self.historical_data[symbol].iloc[0]
            entry_price = candle['Close']
            target_price = opening_15min['High'] + (opening_15min['High'] - opening_15min['Low'])
            stop_loss = opening_15min['Low']

            quantity = int(self.max_investment / entry_price)

            self.place_order(symbol, "BUY", quantity)
            self.positions[symbol] = {
                'quantity': quantity,
                'entry': entry_price,
                'target': target_price,
                'stop_loss': stop_loss,
                'entry_time': candle.name,
                'exit_reason': None
            }
            print(f"Breakout detected. Entering long position for {symbol}")

        self.manage_trade(symbol)

    def simulate_new_candle(self, symbol, last_candle):
        open_price = last_candle['Close']
        close_price = open_price * (1 + (random.random() - 0.5) * 0.01)
        high_price = max(open_price, close_price) * (1 + random.random() * 0.005)
        low_price = min(open_price, close_price) * (1 - random.random() * 0.005)
        volume = int(last_candle['Volume'] * (0.8 + random.random() * 0.4))

        # Handle the case where the index might not be a datetime
        if isinstance(last_candle.name, (pd.Timestamp, datetime.datetime)):
            new_datetime = last_candle.name + pd.Timedelta(minutes=5)
        else:
            new_datetime = pd.Timestamp.now()

        new_candle = pd.Series({
            'Open': open_price,
            'High': high_price,
            'Low': low_price,
            'Close': close_price,
            'Volume': volume,
        }, name=new_datetime)

        return new_candle

    def run_strategy(self):
        self.load_symbols()
        for symbol in self.symbols:
            self.request_historical_data(symbol)
        
        if self.is_armed:
            self.run_live_strategy()
        else:
            self.run_simulation()

    def run_simulation(self):
        simulation_periods = 100  # Simulate 100 new candles for each symbol
        
        for _ in range(simulation_periods):
            for symbol in self.symbols:
                if symbol in self.historical_data and not self.historical_data[symbol].empty:
                    last_candle = self.historical_data[symbol].iloc[-1]
                    new_candle = self.simulate_new_candle(symbol, last_candle)
                    
                    # Use concat instead of append
                    self.historical_data[symbol] = pd.concat([self.historical_data[symbol], pd.DataFrame([new_candle])], ignore_index=True)
                    
                    self.on_new_candle(symbol, new_candle)
                    
                    print(f"Processed new candle for {symbol} at {new_candle.name}")
                else:
                    print(f"No historical data available for {symbol}")
            
            print(f"Completed simulation period {_ + 1}/{simulation_periods}")

    def run_live_strategy(self):
        while not self.stop_event.is_set():
            for symbol in self.symbols:
                new_candle = self.fetch_real_time_data(symbol)
                if new_candle is not None:
                    self.on_new_candle(symbol, new_candle)
                    print(f"Processed new real-time candle for {symbol} at {new_candle.name}")
            
            # Wait for 5 minutes before the next update
            for _ in range(300):  # 300 seconds = 5 minutes
                if self.stop_event.is_set():
                    break
                time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='IBKR Breakout Trading Bot')
    parser.add_argument('--arm', action='store_true', help='Arm the trading bot (default: False)')
    args = parser.parse_args()

    bot = BreakoutTradingBot(is_armed=args.arm)
    print(f"Bot armed: {bot.is_armed}")
    bot.start()
    bot.run_strategy()
    input("Press Enter to stop...")
    bot.stop()
