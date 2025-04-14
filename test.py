
from datetime import datetime, timedelta

print(datetime.utcnow())
print(datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0))

from get_crypto_data_binance import fetch_crypto_daily_binance

print(fetch_crypto_daily_binance('BTCUSDT'))
