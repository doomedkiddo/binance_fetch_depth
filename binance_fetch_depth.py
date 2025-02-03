import websocket
import json
from datetime import datetime
import time
from clickhouse_driver import Client

# Global variables to store the last price and last quantity of DOGE-USDT
last_price_doge_usdt = None
last_quantity_doge_usdt = None

# ClickHouse client
client = Client('localhost', user='default', password='kali')

# Function to handle incoming messages
def on_message(ws, message):
    global last_price_doge_usdt, last_quantity_doge_usdt

    data = json.loads(message)
    
    # 处理ticker数据
    if 'e' in data and data['e'] == '24hrTicker':
        last_price_doge_usdt = float(data['c'])
        last_quantity_doge_usdt = float(data['Q'])
        print(f"Updated DOGE-USDT last price to {last_price_doge_usdt} and last quantity to {last_quantity_doge_usdt}")
    
    # 处理深度数据
    elif 'e' in data and data['e'] == 'depth5':
        timestamp = datetime.fromtimestamp(data['E']/1000)
        bids = data['b'][:5]  # 前五档买盘
        asks = data['a'][:5]  # 前五档卖盘
        
        row = [timestamp]
        row.append(float(last_price_doge_usdt if last_price_doge_usdt else 0))
        row.append(float(last_quantity_doge_usdt if last_quantity_doge_usdt else 0))
        
        # 处理卖单（asks）
        for ask in asks:
            row.extend([float(ask[0]), float(ask[1])])
        
        # 处理买单（bids）
        for bid in bids:
            row.extend([float(bid[0]), float(bid[1])])
        
        # 插入ClickHouse
        client.execute(
            '''INSERT INTO binance_doge_usdt_swap (
                Timestamp, Last_Price, Last_Quantity,
                Ask0_Price, Ask0_Volume, Ask1_Price, Ask1_Volume, 
                Ask2_Price, Ask2_Volume, Ask3_Price, Ask3_Volume,
                Ask4_Price, Ask4_Volume, Bid0_Price, Bid0_Volume,
                Bid1_Price, Bid1_Volume, Bid2_Price, Bid2_Volume,
                Bid3_Price, Bid3_Volume, Bid4_Price, Bid4_Volume
            ) VALUES''',
            [tuple(row)],
            types_check=True
        )
        print(f"Data written for {timestamp}")

# 错误处理
def on_error(ws, error):
    print(f"Error: {error}")

# 关闭处理
def on_close(ws, close_status_code, close_msg):
    print("### Connection Closed ###")

# 连接开启处理
def on_open(ws):
    print("### Connection Opened ###")
    # 订阅DOGEUSDT永续合约的ticker和depth5
    subscribe_message = {
        "method": "SUBSCRIBE",
        "params": [
            "dogeusdt@depth5",
            "dogeusdt@ticker"
        ],
        "id": 1
    }
    ws.send(json.dumps(subscribe_message))

# 主程序
if __name__ == "__main__":
    while True:
        try:
            ws = websocket.WebSocketApp(
                "wss://fstream.binance.com/ws",
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.on_open = on_open
            ws.run_forever(ping_interval=60, ping_timeout=10)
        except KeyboardInterrupt:
            print('Terminating the program...')
            raise SystemExit
        except Exception as e:
            print(f"Exception: {e}. Attempting to reconnect in 10 seconds.")
            time.sleep(10)
