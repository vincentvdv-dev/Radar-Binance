#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Outil d'enregistrement en continu les liquidations Binance Futures
Tools for listen liquidation of binance Futures and record that in csv (for use in bot or just analysing data)
(WebSocket !forceOrder@arr) dans /data/liquidations.csv
"""

import websocket, json, csv, os, time
from threading import Thread

# Parameters
OUT_DIR = "data"
OUT_FILE = os.path.join(OUT_DIR, "liquidations.csv")
URL = "wss://fstream.binance.com/ws/!forceOrder@arr"

# Prepare the folder and file (CSV header)
os.makedirs(OUT_DIR, exist_ok=True)
if not os.path.exists(OUT_FILE):
    with open(OUT_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "symbol", "side", "price", "qty"])

def on_message(ws, msg):
    data = json.loads(msg)
    o = data["o"]
    ts = data["E"]          
    symbol = o["s"]         # for example BTCUSDT
    side = o["S"]           # BUY=shorts liquidés, SELL=longs liquidés / special for liquidation binance buy for a short
    price = o["p"]
    qty   = o["q"]
        # Print the liquidation
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts/1000))}] "
          f"{symbol} {side} @ {price} × {qty}")
    # write in CSV
    # Append to CSV
    with open(OUT_FILE, "a", newline="") as f:
        csv.writer(f).writerow([ts, symbol, side, price, qty])

def on_error(ws, err):
    print("WebSocket error:", err)

def on_close(ws, code, reason):
    print(f"WebSocket closed: {code} / {reason}")

def run_logger():
    ws = websocket.WebSocketApp(URL,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close)
    ws.run_forever()

if __name__ == "__main__":
    print("▶️  Démarrage du logger liquidations… (CTRL+C pour stopper)")
    Thread(target=run_logger, daemon=True).start()
    # Simple loop for keep process good
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n⏹️  Logger arrêté")
