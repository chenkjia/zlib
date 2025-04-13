#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
加密货币数据采集程序
功能：初始数据抓取和每日更新

MongoDB数据结构：
{
    name: str,          # 加密货币名称
    symbol: str,        # 加密货币符号
    dayline: [          # 日线数据数组
        {
            date: datetime,    # 日期
            open: float,       # 开盘价
            close: float,      # 收盘价
            high: float,       # 最高价
            low: float,        # 最低价
            volume: float      # 交易量
        }
    ]
}
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
import time
from apscheduler.schedulers.background import BackgroundScheduler
from get_crypto_data_binance import fetch_crypto_list_binance, fetch_crypto_daily_binance

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MongoDB配置
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "crypto"
COLLECTION_NAME = "crypto_data"

def get_mongo_collection() -> Collection:
    """
    获取MongoDB集合对象
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        return db[COLLECTION_NAME]
    except Exception as e:
        logger.error(f"MongoDB连接失败: {str(e)}")
        raise

def fetch_crypto_list() -> List[Dict]:
    """
    获取加密货币列表
    """
    return fetch_crypto_list_binance()

def fetch_crypto_daily(symbol: str, start_date: Optional[datetime] = None) -> List[Dict]:
    """获取指定加密货币的日线数据"""
    try:
        # 检查现有数据
        collection = get_mongo_collection()
        crypto_data = collection.find_one({'symbol': symbol}, {'dayline': {'$slice': -1}})
        
        # 如果有数据且是今天的，直接返回空列表
        if crypto_data and crypto_data.get('dayline'):
            last_date = crypto_data['dayline'][0]['date']
            if last_date >= datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0):
                logger.info(f"{symbol} 数据已是最新")
                return []
            start_date = last_date + timedelta(days=1)
            
        # 获取并返回数据
        return fetch_crypto_daily_binance(symbol, start_date)
        
    except Exception as e:
        logger.error(f"获取{symbol}数据出错: {e}")
        raise

def init_crypto_list():
    """
    初始化加密货币列表数据
    功能：
    1. 调用fetch_crypto_list()获取加密货币基础信息
    2. 数据清洗和标准化
    3. 创建初始文档结构：{name, symbol, dayline: []}
    4. 存储到MongoDB
    """
    try:
        # 获取加密货币列表
        crypto_list = fetch_crypto_list()
        
        # 获取MongoDB集合
        collection = get_mongo_collection()
        
        # 构建并插入文档
        for crypto in crypto_list:
            # 检查是否已存在
            if not collection.find_one({'symbol': crypto['symbol']}):
                doc = {
                    'name': crypto['name'],
                    'symbol': crypto['symbol'],
                    'dayline': []
                }
                collection.insert_one(doc)
                logger.info(f"添加新加密货币: {crypto['symbol']}")
            
        logger.info("加密货币列表初始化完成")
        
    except Exception as e:
        logger.error(f"初始化加密货币列表失败: {str(e)}")
        raise

def init_crypto_daily():
    """
    初始化所有加密货币的历史日线数据
    功能：
    1. 从MongoDB获取所有已初始化的加密货币文档
    2. 遍历每个加密货币：
       - 调用fetch_crypto_daily()获取历史日线数据
       - 构建日线数据数组：[{date, open, close, high, low, volume}]
    3. 更新对应加密货币文档的dayline字段
    """
    try:
        collection = get_mongo_collection()
        
        # 获取所有加密货币
        cryptos = collection.find({}, {'symbol': 1})
        
        for crypto in cryptos:
            try:
                # 获取历史日线数据
                daily_data = fetch_crypto_daily(crypto['symbol'])
                
                # 更新到MongoDB
                collection.update_one(
                    {'symbol': crypto['symbol']},
                    {'$set': {'dayline': daily_data}}
                )
                
                logger.info(f"初始化{crypto['symbol']}的历史日线数据完成")
                
                # 添加延时避免API限制
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"初始化{crypto['symbol']}的历史日线数据失败: {str(e)}")
                continue
        
        logger.info("所有加密货币的历史日线数据初始化完成")
        
    except Exception as e:
        logger.error(f"初始化历史日线数据失败: {str(e)}")
        raise

def update_crypto_list():
    """
    更新加密货币列表数据
    功能：
    1. 从MongoDB获取现有的加密货币列表(name, symbol)
    2. 调用fetch_crypto_list()获取最新列表
    3. 过滤出新增的加密货币
    4. 为新增加密货币创建文档结构：{name, symbol, dayline: []}
    """
    try:
        collection = get_mongo_collection()
        
        # 获取现有的加密货币符号列表
        existing_symbols = set(doc['symbol'] for doc in collection.find({}, {'symbol': 1}))
        
        # 获取最新的加密货币列表
        new_crypto_list = fetch_crypto_list()
        
        # 找出新增的加密货币
        for crypto in new_crypto_list:
            if crypto['symbol'] not in existing_symbols:
                # 创建新文档
                doc = {
                    'name': crypto['name'],
                    'symbol': crypto['symbol'],
                    'dayline': []
                }
                collection.insert_one(doc)
                logger.info(f"添加新加密货币: {crypto['symbol']}")
        
        logger.info("加密货币列表更新完成")
        
    except Exception as e:
        logger.error(f"更新加密货币列表失败: {str(e)}")
        raise

def update_crypto_daily():
    """
    更新所有加密货币的日线数据
    功能：
    1. 从MongoDB获取所有加密货币文档
    2. 对每个加密货币：
       - 获取最后一条日线数据的日期
       - 如果最后更新日期是昨天或更新，则跳过
       - 否则调用fetch_crypto_daily()获取该日期之后的数据
       - 合并新数据到dayline数组
    """
    try:
        collection = get_mongo_collection()
        
        # 获取所有加密货币
        cryptos = list(collection.find({}, {'symbol': 1, 'name': 1, 'dayline': {'$slice': -1}}))
        total_count = len(cryptos)
        
        logger.info(f"开始更新{total_count}个加密货币的日线数据")
        
        # 计算昨天的日期（使用UTC时间）
        yesterday = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        
        for index, crypto in enumerate(cryptos, 1):
            try:
                symbol = crypto['symbol']
                name = crypto.get('name', symbol)
                
                logger.info(f"[{index}/{total_count}] 检查 {name}({symbol}) 的日线数据...")
                
                # 获取最后一条数据的日期
                last_date = None
                if crypto.get('dayline'):
                    last_date = crypto['dayline'][0]['date']
                    logger.info(f"最后更新日期: {last_date.strftime('%Y-%m-%d')}")
                    
                    # 如果最后更新日期是昨天或更新，跳过这个币种
                    if last_date >= yesterday:
                        logger.info(f"数据已是最新（{last_date.strftime('%Y-%m-%d')}），跳过更新")
                        continue
                
                # 获取新数据
                start_date = last_date + timedelta(days=1) if last_date else None
                logger.info(f"开始获取从 {start_date.strftime('%Y-%m-%d') if start_date else '最早'} 开始的数据...")
                
                daily_data = fetch_crypto_daily(symbol, start_date)
                
                if daily_data:
                    # 更新到MongoDB
                    collection.update_one(
                        {'symbol': symbol},
                        {'$push': {'dayline': {'$each': daily_data}}}
                    )
                    logger.info(f"成功更新了{len(daily_data)}条日线数据")
                    logger.info(f"数据范围: {daily_data[0]['date'].strftime('%Y-%m-%d')} 至 {daily_data[-1]['date'].strftime('%Y-%m-%d')}")
                else:
                    logger.info("没有新的日线数据需要更新")
                
                # 添加延时避免API限制
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"更新{symbol}的日线数据失败: {str(e)}")
                continue
        
        logger.info("所有加密货币的日线数据更新完成")
        
    except Exception as e:
        logger.error(f"更新日线数据失败: {str(e)}")
        raise

def update_daily_only():
    """
    仅执行日线数据更新
    """
    try:
        logger.info("开始执行单独的日线数据更新任务")
        update_crypto_daily()
        logger.info("日线数据更新任务完成")
    except Exception as e:
        logger.error(f"日线数据更新任务失败: {str(e)}")

def initial_data():
    """
    初始数据抓取主函数
    功能：
    1. 调用init_crypto_list()初始化基础文档结构
    2. 调用init_crypto_daily()填充历史日线数据
    """
    try:
        logger.info("开始初始化数据...")
        init_crypto_list()
        init_crypto_daily()
        logger.info("数据初始化完成")
    except Exception as e:
        logger.error(f"数据初始化失败: {str(e)}")
        raise

def update_data():
    """
    数据更新主函数
    功能：
    1. 调用update_crypto_list()处理新增加密货币
    2. 调用update_crypto_daily()更新所有加密货币的日线数据
    """
    try:
        logger.info("开始更新数据...")
        update_crypto_list()
        update_crypto_daily()
        logger.info("数据更新完成")
    except Exception as e:
        logger.error(f"数据更新失败: {str(e)}")
        raise

def setup_scheduler():
    """
    设置定时任务
    功能：
    1. 创建定时任务调度器
    2. 设置每日执行时间
    3. 添加update_data()为每日更新任务
    4. 启动调度器
    """
    try:
        # 创建后台调度器
        scheduler = BackgroundScheduler()
        
        # 设置每日凌晨1点执行更新任务
        # 选择凌晨1点是为了确保前一天的数据已经完全更新
        scheduler.add_job(
            update_data,
            trigger='cron',
            hour=1,
            minute=0,
            id='daily_update'
        )
        
        # 启动调度器
        scheduler.start()
        logger.info("定时任务调度器启动成功，设置为每日凌晨1点执行更新")
        
        return scheduler
        
    except Exception as e:
        logger.error(f"设置定时任务失败: {str(e)}")
        raise

def main():
    """
    主程序入口
    功能：
    1. 执行initial_data()完成初始数据抓取
    2. 调用setup_scheduler()启动每日更新定时器
    """
    try:
        # 检查MongoDB连接
        collection = get_mongo_collection()
        logger.info("MongoDB连接成功")
        
        # 执行初始化
        if collection.count_documents({}) == 0:
            logger.info("数据库为空，开始执行初始化...")
            initial_data()
        else:
            logger.info("数据库已存在数据，跳过初始化步骤")
            # 执行一次更新以确保数据最新
            update_data()
        
        # 设置定时任务
        scheduler = setup_scheduler()
        
        # 保持程序运行
        try:
            while True:
                time.sleep(60)  # 每分钟检查一次
        except KeyboardInterrupt:
            logger.info("接收到终止信号，正在关闭程序...")
            scheduler.shutdown()
            logger.info("程序已安全终止")
            
    except Exception as e:
        logger.error(f"程序运行出错: {str(e)}")
        raise

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "update_daily":
        update_daily_only()
    else:
        main()
