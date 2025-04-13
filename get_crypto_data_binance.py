#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
币安API数据获取模块
功能：调用币安API获取加密货币数据
"""

import requests
import logging
from datetime import datetime
from typing import List, Dict, Optional
import time

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# API配置
BASE_URL = "https://api.binance.com/api/v3"
ENDPOINTS = {
    "exchange_info": "/exchangeInfo",
    "klines": "/klines"
}

# 请求配置
REQUEST_TIMEOUT = 30  # 请求超时时间（秒）
MAX_RETRIES = 3      # 最大重试次数
RETRY_DELAY = 5      # 重试间隔（秒）

def make_request(url: str, params: Dict = None) -> Dict:
    """
    发送HTTP请求，包含重试机制
    """
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt == MAX_RETRIES - 1:  # 最后一次重试
                raise
            logger.warning(f"请求失败，{RETRY_DELAY}秒后重试: {str(e)}")
            time.sleep(RETRY_DELAY)

def fetch_crypto_list_binance() -> List[Dict]:
    """
    从币安获取加密货币列表
    返回：
    List[Dict]: 包含name和symbol的字典列表
        [
            {
                "name": str,    # 加密货币名称
                "symbol": str   # 加密货币符号
            }
        ]
    """
    try:
        # 调用Binance API获取交易对信息
        data = make_request(f"{BASE_URL}{ENDPOINTS['exchange_info']}")
        
        # 提取USDT交易对信息
        crypto_list = []
        for symbol_data in data['symbols']:
            if symbol_data['quoteAsset'] == 'USDT' and symbol_data['status'] == 'TRADING':
                crypto_list.append({
                    'name': symbol_data['baseAsset'],
                    'symbol': symbol_data['symbol']
                })
        
        logger.info(f"成功从币安获取{len(crypto_list)}个加密货币信息")
        return crypto_list
        
    except Exception as e:
        logger.error(f"从币安获取加密货币列表失败: {str(e)}")
        raise

def fetch_crypto_daily_binance(symbol: str, start_date: Optional[datetime] = None) -> List[Dict]:
    """
    从币安获取指定加密货币的日线数据
    
    参数：
    symbol: str - 加密货币的符号
    start_date: datetime - 可选，获取该日期之后的数据，默认获取所有历史数据
    
    返回：
    List[Dict]: 日线数据列表
        [
            {
                "date": datetime,
                "open": float,
                "close": float,
                "high": float,
                "low": float,
                "volume": float
            }
        ]
    """
    try:
        all_daily_data = []
        seen_dates = set()  # 用于去重的日期集合
        
        # 准备初始请求参数
        params = {
            'symbol': symbol,
            'interval': '1d',  # 日线数据
            'limit': 1000     # 单次请求最大限制
        }
        
        if start_date:
            # 如果指定了起始日期，从该日期开始获取
            params['startTime'] = int(start_date.timestamp() * 1000)
        else:
            # 如果没有指定起始日期，从最早的数据开始获取
            # 先获取最近的数据以确定结束时间
            recent_data = make_request(f"{BASE_URL}{ENDPOINTS['klines']}", params)
            if recent_data:
                # 使用最早可能的时间作为起始时间（2017年8月，币安成立时间）
                params['startTime'] = int(datetime(2017, 8, 1).timestamp() * 1000)
                # 使用最近数据的最后一个时间戳作为结束时间
                params['endTime'] = recent_data[-1][0]
        
        while True:
            # 调用Binance API获取K线数据
            klines = make_request(f"{BASE_URL}{ENDPOINTS['klines']}", params)
            
            if not klines:
                break
                
            # 转换数据格式
            for kline in klines:
                date = datetime.fromtimestamp(kline[0] / 1000)
                date_str = date.strftime('%Y-%m-%d')  # 使用日期字符串进行去重
                
                # 如果这个日期的数据已经存在，跳过
                if date_str in seen_dates:
                    continue
                    
                seen_dates.add(date_str)
                daily_data = {
                    'date': date,
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                }
                all_daily_data.append(daily_data)
            
            # 如果返回的数据少于1000条，说明已经是全部数据
            if len(klines) < 1000:
                if not start_date and 'endTime' in params:
                    # 如果是在获取历史数据，现在开始获取最近的数据
                    params.pop('startTime')
                    params.pop('endTime')
                    continue
                break
                
            # 更新下一批数据的开始时间
            params['startTime'] = klines[-1][0] + 1
            
            # 添加短暂延时避免API限制
            time.sleep(1)
        
        # 按日期排序
        all_daily_data.sort(key=lambda x: x['date'])
        
        logger.info(f"成功从币安获取{symbol}的{len(all_daily_data)}条日线数据")
        return all_daily_data
        
    except Exception as e:
        logger.error(f"从币安获取{symbol}日线数据失败: {str(e)}")
        raise 