#!/usr/bin/env python3
"""
板块追踪分析系统
- 每日收盘后自动分析行业板块和概念板块
- 筛选热门/潜力板块
- 在潜力板块内选股（EMA13/55 + 回踩买点）
- 生成报告并通过飞书推送
"""

import os
import sys
import json
import time
import sqlite3
import logging
from datetime import datetime, timedelta
from pathlib import Path

import akshare as ak
import baostock as bs
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from pathlib import Path
import requests

# 全局超时和重试配置
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3

# ========== 配置 ==========
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "data" / "sector_history.db"
REPORT_DIR = BASE_DIR / "reports"
CONFIG_PATH = BASE_DIR / "config.json"

DEFAULT_CONFIG = {
    "hot_sectors_count": 4,       # 热门板块数量
    "potential_sectors_count": 7, # 潜力板块数量
    "stocks_per_sector": 3,       # 每板块选股数量
    "ema_short": 13,              # 短期EMA
    "ema_long": 55,               # 长期EMA
    "ma_periods": [5, 10, 20, 60],# MA均线周期
    "pullback_threshold": 0.015,  # 回踩EMA13的阈值（±1.5%）
    "analysis_days": [5, 10, 20], # 分析周期（日）
    "report_time": "17:00",       # 报告时间（北京时间）
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


# ========== 缓存模块 ==========
CACHE_DIR = BASE_DIR / "cache"

def save_cache(data, cache_type):
    """保存数据到缓存"""
    CACHE_DIR.mkdir(exist_ok=True)
    date_str = datetime.now().strftime('%Y%m%d')
    cache_file = CACHE_DIR / f"{cache_type}_{date_str}.json"
    with open(cache_file, 'w', encoding='utf-8') as f:
        json.dump({'date': datetime.now().strftime('%Y-%m-%d'), 'data': data}, f, ensure_ascii=False)
    logger.info(f"缓存已保存: {cache_type}_{date_str}.json")


def load_cache(cache_type):
    """加载最近的缓存数据"""
    if not CACHE_DIR.exists():
        return None
    
    cache_files = sorted(
        [f for f in CACHE_DIR.iterdir() if f.name.startswith(f"{cache_type}_")],
        reverse=True
    )
    
    if not cache_files:
        return None
    
    try:
        with open(cache_files[0], encoding='utf-8') as f:
            cached = json.load(f)
        logger.info(f"加载缓存: {cache_files[0].name} (日期: {cached['date']})")
        return cached['data']
    except Exception as e:
        logger.warning(f"缓存加载失败: {e}")
        return None


def is_data_valid(sectors):
    """检查数据是否有效（不是全0）"""
    if not sectors:
        return False
    sample = sectors[:5]
    return any(s.get('change_pct', 0) != 0 for s in sample)


# ========== 数据库模块 ==========
def init_db():
    """初始化SQLite数据库"""
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    
    # 板块每日排名表
    c.execute('''CREATE TABLE IF NOT EXISTS daily_sector_ranking (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        sector_name TEXT NOT NULL,
        sector_type TEXT NOT NULL,  -- 'industry' or 'concept'
        rank INTEGER,
        score REAL,
        change_pct REAL,
        fund_flow REAL,
        volume_ratio REAL,
        ma_status TEXT,  -- 'bullish', 'bearish', 'neutral'
        category TEXT,   -- 'hot', 'potential', 'normal'
        UNIQUE(date, sector_name, sector_type)
    )''')
    
    # 每日推荐个股表
    c.execute('''CREATE TABLE IF NOT EXISTS daily_stock_picks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        sector_name TEXT NOT NULL,
        stock_code TEXT NOT NULL,
        stock_name TEXT NOT NULL,
        price REAL,
        change_pct REAL,
        ema13 REAL,
        ema55 REAL,
        ema13_dist_pct REAL,
        signal TEXT,           -- 'pullback', 'golden_cross', 'trend', 'watch'
        score REAL,
        pe_ratio REAL,
        risk_note TEXT,
        hot_topic TEXT,
        summary TEXT,
        UNIQUE(date, stock_code)
    )''')
    
    # 板块连续上榜记录（用于轮动分析）
    c.execute('''CREATE TABLE IF NOT EXISTS sector_streak (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        date TEXT NOT NULL,
        sector_name TEXT NOT NULL,
        sector_type TEXT NOT NULL,
        consecutive_days INTEGER,
        trend TEXT,  -- 'rising', 'falling', 'stable'
        UNIQUE(date, sector_name, sector_type)
    )''')
    
    conn.commit()
    conn.close()
    logger.info("数据库初始化完成")


def save_daily_results(date_str, sector_results, stock_picks):
    """保存每日分析结果到数据库"""
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    
    # 保存板块排名
    for cat, sectors in sector_results.items():
        for rank, s in enumerate(sectors, 1):
            try:
                c.execute('''INSERT OR REPLACE INTO daily_sector_ranking 
                    (date, sector_name, sector_type, rank, score, change_pct, 
                     fund_flow, volume_ratio, ma_status, category)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                    (date_str, s.get('name', ''), s.get('type', cat), rank, s.get('score', 0),
                     s.get('change_pct', s.get('change', 0)), s.get('fund_flow', s.get('flow', 0)), 
                     s.get('volume_ratio', 0), s.get('ma_status', 'neutral'), cat))
            except Exception as e:
                logger.warning(f"保存板块数据失败: {s.get('name', '?')}: {e}")
    
    # 保存个股推荐
    for sp in stock_picks:
        c.execute('''INSERT OR REPLACE INTO daily_stock_picks
            (date, sector_name, stock_code, stock_name, price, change_pct,
             ema13, ema55, ema13_dist_pct, signal, score, pe_ratio,
             risk_note, hot_topic, summary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (date_str, sp['sector'], sp['code'], sp['name'], sp['price'],
             sp['change_pct'], sp.get('ema13', 0), sp.get('ema55', 0),
             sp.get('ema13_dist_pct', 0), sp['signal'], sp['score'],
             sp.get('pe', 0), sp.get('risk', ''), sp.get('hot_topic', ''),
             sp.get('summary', '')))
    
    conn.commit()
    conn.close()
    logger.info(f"数据已保存: {len(stock_picks)} 只个股, {sum(len(v) for v in sector_results.values())} 个板块")


def get_sector_streak(date_str):
    """获取板块连续上榜情况"""
    conn = sqlite3.connect(str(DB_PATH))
    c = conn.cursor()
    
    # 查询最近7天的板块出现次数
    date_7d_ago = (datetime.strptime(date_str, '%Y-%m-%d') - timedelta(days=10)).strftime('%Y-%m-%d')
    c.execute('''SELECT sector_name, sector_type, category, COUNT(*) as days,
                        GROUP_CONCAT(DISTINCT category) as categories
                 FROM daily_sector_ranking 
                 WHERE date >= ? AND date <= ?
                 GROUP BY sector_name, sector_type
                 HAVING days >= 2
                 ORDER BY days DESC''', (date_7d_ago, date_str))
    
    streaks = []
    for row in c.fetchall():
        streaks.append({
            'name': row[0],
            'type': row[1],
            'current_category': row[2],
            'days': row[3],
            'categories': row[4]
        })
    
    conn.close()
    return streaks


# ========== 数据采集模块 ==========
def get_industry_sectors():
    """获取行业板块数据（带重试+缓存降级）"""
    for attempt in range(MAX_RETRIES):
        try:
            df = ak.stock_board_industry_name_em()
            sectors = []
            for _, row in df.iterrows():
                sectors.append({
                    'name': row['板块名称'],
                    'code': row['板块代码'],
                    'price': float(row['最新价']) if pd.notna(row['最新价']) else 0,
                    'change_pct': float(row['涨跌幅']) if pd.notna(row['涨跌幅']) else 0,
                    'total_mv': float(row['总市值']) if pd.notna(row['总市值']) else 0,
                    'turnover': float(row['换手率']) if pd.notna(row['换手率']) else 0,
                    'up_count': int(row['上涨家数']) if pd.notna(row['上涨家数']) else 0,
                    'down_count': int(row['下跌家数']) if pd.notna(row['下跌家数']) else 0,
                    'lead_stock': row['领涨股票'],
                    'lead_change': float(row['领涨股票-涨跌幅']) if pd.notna(row['领涨股票-涨跌幅']) else 0,
                    'type': 'industry'
                })
            
            # 检查数据有效性
            if is_data_valid(sectors):
                logger.info(f"获取行业板块: {len(sectors)} 个 (实时数据)")
                save_cache(sectors, 'industry')
                return sectors
            else:
                logger.warning("行业板块数据全为0，降级到缓存")
                break
        except Exception as e:
            logger.warning(f"获取行业板块失败 (尝试{attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
    
    # 降级：使用缓存
    cached = load_cache('industry')
    if cached:
        logger.info(f"使用缓存行业板块数据: {len(cached)} 个")
        return cached
    
    logger.error("行业板块数据获取失败且无缓存")
    return []


def get_concept_sectors():
    """获取概念板块数据（带重试+缓存降级）"""
    for attempt in range(MAX_RETRIES):
        try:
            df = ak.stock_board_concept_name_em()
            sectors = []
            for _, row in df.iterrows():
                sectors.append({
                    'name': row['板块名称'],
                    'code': row['板块代码'],
                    'price': float(row['最新价']) if pd.notna(row['最新价']) else 0,
                    'change_pct': float(row['涨跌幅']) if pd.notna(row['涨跌幅']) else 0,
                    'total_mv': float(row['总市值']) if pd.notna(row['总市值']) else 0,
                    'turnover': float(row['换手率']) if pd.notna(row['换手率']) else 0,
                    'up_count': int(row['上涨家数']) if pd.notna(row['上涨家数']) else 0,
                    'down_count': int(row['下跌家数']) if pd.notna(row['下跌家数']) else 0,
                    'lead_stock': row['领涨股票'],
                    'lead_change': float(row['领涨股票-涨跌幅']) if pd.notna(row['领涨股票-涨跌幅']) else 0,
                    'type': 'concept'
                })
            
            if is_data_valid(sectors):
                logger.info(f"获取概念板块: {len(sectors)} 个 (实时数据)")
                save_cache(sectors, 'concept')
                return sectors
            else:
                logger.warning("概念板块数据全为0，降级到缓存")
                break
        except Exception as e:
            logger.warning(f"获取概念板块失败 (尝试{attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(5)
    
    cached = load_cache('concept')
    if cached:
        logger.info(f"使用缓存概念板块数据: {len(cached)} 个")
        return cached
    
    logger.error("概念板块数据获取失败且无缓存")
    return []


def get_fund_flow_rank():
    """获取板块资金流向排名（带重试）"""
    for attempt in range(MAX_RETRIES):
        try:
            df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
            flows = {}
            for _, row in df.iterrows():
                name = row['名称']
                flows[name] = {
                    'today_flow': float(row['今日主力净流入-净额']) if pd.notna(row['今日主力净流入-净额']) else 0,
                    'today_flow_pct': float(row['今日主力净流入-净占比']) if pd.notna(row['今日主力净流入-净占比']) else 0,
                    'super_big_flow': float(row['今日超大单净流入-净额']) if pd.notna(row['今日超大单净流入-净额']) else 0,
                }
            logger.info(f"获取资金流向: {len(flows)} 个板块")
            return flows
        except Exception as e:
            logger.warning(f"获取资金流向失败 (尝试{attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(3)
    logger.warning("资金流向获取全部失败，将使用涨幅数据代替")
    return {}


def get_sector_members(sector_name, sector_type='industry'):
    """获取板块成分股"""
    try:
        if sector_type == 'industry':
            df = ak.stock_board_industry_cons_em(symbol=sector_name)
        else:
            df = ak.stock_board_concept_cons_em(symbol=sector_name)
        
        stocks = []
        for _, row in df.iterrows():
            stocks.append({
                'code': str(row['代码']).zfill(6),
                'name': row['名称'],
                'price': float(row['最新价']) if pd.notna(row['最新价']) else 0,
                'change_pct': float(row['涨跌幅']) if pd.notna(row['涨跌幅']) else 0,
                'volume': float(row['成交量']) if pd.notna(row['成交量']) else 0,
                'amount': float(row['成交额']) if pd.notna(row['成交额']) else 0,
                'turnover': float(row['换手率']) if pd.notna(row['换手率']) else 0,
                'pe': float(row['市盈率-动态']) if pd.notna(row['市盈率-动态']) else 0,
                'pb': float(row['市净率']) if pd.notna(row['市净率']) else 0,
            })
        return stocks
    except Exception as e:
        logger.error(f"获取板块成分股失败 [{sector_name}]: {e}")
        return []


def get_stock_history(code, days=80):
    """获取个股历史K线（用BaoStock）"""
    try:
        if code.startswith('6'):
            bs_code = f"sh.{code}"
        else:
            bs_code = f"sz.{code}"
        
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days + 30)).strftime('%Y-%m-%d')
        
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,open,high,low,close,volume,amount,turn",
            start_date=start_date, end_date=end_date,
            frequency="d", adjustflag="2"
        )
        
        data_list = []
        while (rs.error_code == '0') & rs.next():
            data_list.append(rs.get_row_data())
        
        if not data_list:
            return pd.DataFrame()
        
        df = pd.DataFrame(data_list, columns=rs.fields)
        # 清理空值
        df['close'] = pd.to_numeric(df['close'], errors='coerce')
        df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df.dropna(subset=['close'])
        return df
    except Exception as e:
        logger.error(f"获取K线失败 [{code}]: {e}")
        return pd.DataFrame()


def get_stock_concept_info(code):
    """获取个股所属概念板块"""
    # 通过遍历热门概念板块的成分股来反查
    # 这里简化处理，从成分股接口反向匹配
    return []


def get_sector_news(sector_name):
    """获取板块相关新闻（从东方财富）"""
    try:
        # 使用AKShare的新闻接口
        # 如果没有直接的板块新闻接口，返回空
        # 后续可以用web scraping补充
        return []
    except Exception as e:
        return []


# ========== 技术分析模块 ==========
def calc_ema(series, period):
    """计算EMA"""
    return series.ewm(span=period, adjust=False).mean()


def calc_ma(series, period):
    """计算MA"""
    return series.rolling(window=period).mean()


def analyze_stock_tech(df):
    """分析个股技术指标，返回最新状态"""
    if df.empty or len(df) < 55:
        return None
    
    close = df['close']
    volume = df['volume']
    
    # 计算EMA
    ema13 = calc_ema(close, 13)
    ema55 = calc_ema(close, 55)
    
    # 计算MA
    ma5 = calc_ma(close, 5)
    ma10 = calc_ma(close, 10)
    ma20 = calc_ma(close, 20)
    ma60 = calc_ma(close, 60)
    
    # 最新值
    latest = {
        'price': close.iloc[-1],
        'ema13': ema13.iloc[-1],
        'ema55': ema55.iloc[-1],
        'ma5': ma5.iloc[-1] if not pd.isna(ma5.iloc[-1]) else 0,
        'ma10': ma10.iloc[-1] if not pd.isna(ma10.iloc[-1]) else 0,
        'ma20': ma20.iloc[-1] if not pd.isna(ma20.iloc[-1]) else 0,
        'ma60': ma60.iloc[-1] if not pd.isna(ma60.iloc[-1]) else 0,
    }
    
    # EMA13距离百分比（用于判断回踩）
    latest['ema13_dist_pct'] = (latest['price'] - latest['ema13']) / latest['ema13']
    
    # EMA13 vs EMA55（趋势判断）
    latest['ema_bullish'] = latest['ema13'] > latest['ema55']
    
    # EMA13方向（是否向上）
    latest['ema13_rising'] = ema13.iloc[-1] > ema13.iloc[-3] if len(ema13) >= 3 else False
    
    # MA多头排列
    latest['ma_bullish'] = (latest['ma5'] > latest['ma10'] > latest['ma20']) if all([
        latest['ma5'], latest['ma10'], latest['ma20']
    ]) else False
    
    # 量比（今日成交量 / 5日均量）
    vol_ma5 = volume.tail(5).mean()
    latest['vol_ratio'] = volume.iloc[-1] / vol_ma5 if vol_ma5 > 0 else 1
    
    # 5日涨幅
    if len(close) >= 6:
        latest['change_5d'] = (close.iloc[-1] - close.iloc[-6]) / close.iloc[-6] * 100
    else:
        latest['change_5d'] = 0
    
    # 10日涨幅
    if len(close) >= 11:
        latest['change_10d'] = (close.iloc[-1] - close.iloc[-11]) / close.iloc[-11] * 100
    else:
        latest['change_10d'] = 0
    
    return latest


def check_pullback_ema13(tech):
    """检查是否为回踩EMA13买点（严格条件）"""
    if not tech:
        return False, 0
    
    conditions = {
        'ema_bullish': tech['ema_bullish'],               # EMA13 > EMA55（上升趋势）- 必须满足
        'ema13_rising': tech['ema13_rising'],              # EMA13方向向上
        'near_ema13': abs(tech['ema13_dist_pct']) < 0.012,# 股价在EMA13附近±1.2%
        'shrink_vol': tech['vol_ratio'] < 1.05,           # 缩量或平量回调
        'positive_5d': tech['change_5d'] > 0,             # 5日涨幅为正（趋势未破）
    }
    
    score = sum(conditions.values())
    # 必须满足 ema_bullish + near_ema13（核心条件）+ 至少2个其他条件
    is_pullback = (conditions['ema_bullish'] 
                   and conditions['near_ema13'] 
                   and score >= 4)
    
    return is_pullback, score


def get_signal_type(tech, is_pullback):
    """判断信号类型"""
    if not tech:
        return '⚪ 观望'
    
    if is_pullback:
        return '🟡 回踩买点'
    
    # 检查EMA金叉（EMA13刚上穿EMA55）
    if tech['ema_bullish'] and tech['ema13_rising']:
        # 简化判断：EMA13/55差距不大说明刚交叉
        gap = (tech['ema13'] - tech['ema55']) / tech['ema55']
        if 0 < gap < 0.03:
            return '🔴 趋势启动'
    
    if tech['ema_bullish']:
        return '🟢 趋势延续'
    
    return '⚪ 观望'


# ========== 板块筛选模块 ==========
def score_sector(sector, fund_flows):
    """给板块打分"""
    score = 0
    
    # 涨幅得分 (0-30)
    change = sector['change_pct']
    if change > 5:
        score += 30
    elif change > 3:
        score += 25
    elif change > 1:
        score += 20
    elif change > 0:
        score += 15
    else:
        score += 5
    
    # 资金流得分 (0-30)
    name = sector['name']
    if name in fund_flows:
        flow = fund_flows[name]['today_flow']
        if flow > 1e9:
            score += 30
        elif flow > 5e8:
            score += 25
        elif flow > 0:
            score += 20
        else:
            score += 10
        sector['fund_flow'] = flow
    else:
        sector['fund_flow'] = 0
        score += 10
    
    # 上涨家数占比 (0-25)
    total = sector['up_count'] + sector['down_count']
    if total > 0:
        up_ratio = sector['up_count'] / total
        score += int(up_ratio * 25)
    
    # 换手率（活跃度）(0-15)
    if sector['turnover'] > 3:
        score += 15
    elif sector['turnover'] > 1.5:
        score += 10
    else:
        score += 5
    
    sector['score'] = score
    return sector


def classify_sectors(industry_sectors, concept_sectors, fund_flows):
    """分类板块：热门 + 潜力"""
    
    # 对所有行业板块打分
    for s in industry_sectors:
        score_sector(s, fund_flows)
    
    # 按分数排序
    industry_sorted = sorted(industry_sectors, key=lambda x: x['score'], reverse=True)
    
    # 热门板块：分数最高的
    hot = industry_sorted[:4]
    
    # 潜力板块：涨幅不大但有资金流入或上涨占比高的（拐点信号）
    potential_candidates = []
    for s in industry_sorted[4:]:
        # 潜力条件：涨幅<4%且（资金净流入 或 上涨占比>60%）
        total = s['up_count'] + s['down_count']
        up_ratio = s['up_count'] / total if total > 0 else 0
        if s['change_pct'] < 4 and (s.get('fund_flow', 0) > 0 or up_ratio > 0.6):
            potential_candidates.append(s)
    
    # 概念板块交叉验证
    concept_names = {s['name']: s for s in concept_sectors}
    for s in potential_candidates:
        # 检查是否有同名或相关概念板块也在涨
        s['concept_boost'] = False
        for cs in concept_sectors[:20]:  # 前20个热门概念
            if cs['change_pct'] > 2:
                # 简化：如果概念板块名称包含行业关键词
                if any(kw in cs['name'] for kw in [s['name'][:2]]):
                    s['concept_boost'] = True
                    s['score'] += 5
                    break
    
    potential_candidates = sorted(potential_candidates, key=lambda x: x['score'], reverse=True)
    potential = potential_candidates[:7]
    
    # 补充概念板块信息
    hot_concepts = [s for s in concept_sectors if s['change_pct'] > 2][:10]
    
    return {
        'hot': hot,
        'potential': potential,
        'hot_concepts': hot_concepts,
    }


# ========== 个股筛选模块 ==========
def screen_stocks_in_sector(sector, config):
    """在板块内筛选潜力个股"""
    members = get_sector_members(sector['name'], sector['type'])
    if not members:
        return []
    
    time.sleep(0.5)
    
    candidates = []
    bs.login()
    
    # 只分析前20只成分股
    valid_members = [m for m in members[:20] if 'ST' not in m['name'] and m['price'] > 0 and m['pe'] > -100]
    
    for stock in valid_members:
        code = stock['code']
        df_hist = get_stock_history(code, days=80)
        if df_hist.empty:
            continue
        
        time.sleep(0.2)
        
        tech = analyze_stock_tech(df_hist)
        if not tech:
            continue
        
        is_pullback, pullback_score = check_pullback_ema13(tech)
        signal = get_signal_type(tech, is_pullback)
        
        stock_score = 0
        if tech['ema_bullish']: stock_score += 30
        if tech['ema13_rising']: stock_score += 15
        if is_pullback: stock_score += 40
        if tech['ma_bullish']: stock_score += 15
        if tech['vol_ratio'] > 1.2 and tech['change_5d'] > 0: stock_score += 10
        
        risk_notes = []
        if stock['pe'] > 100: risk_notes.append(f"PE {stock['pe']:.0f}倍，估值偏高")
        elif stock['pe'] < 0: risk_notes.append(f"PE {stock['pe']:.0f}，亏损状态")
        
        candidates.append({
            'sector': sector['name'],
            'code': code,
            'name': stock['name'],
            'price': stock['price'],
            'change_pct': stock['change_pct'],
            'ema13': round(tech['ema13'], 2),
            'ema55': round(tech['ema55'], 2),
            'ema13_dist_pct': round(tech['ema13_dist_pct'] * 100, 2),
            'signal': signal,
            'is_pullback': is_pullback,
            'score': stock_score,
            'pe': stock['pe'],
            'vol_ratio': round(tech['vol_ratio'], 2),
            'change_5d': round(tech['change_5d'], 2),
            'change_10d': round(tech['change_10d'], 2),
            'risk': '；'.join(risk_notes) if risk_notes else '正常',
            'ma_bullish': tech['ma_bullish'],
            'hot_topic': '',  # 稍后只对重点股填充
        })
    
    bs.logout()
    
    # 按评分排序
    candidates.sort(key=lambda x: x['score'], reverse=True)
    
    # 优先保留回踩买点，再取趋势强的
    pullback_stocks = [s for s in candidates if s['is_pullback']]
    other_stocks = [s for s in candidates if not s['is_pullback']]
    
    result = pullback_stocks[:config['stocks_per_sector']]
    remaining = config['stocks_per_sector'] - len(result)
    if remaining > 0:
        result.extend(other_stocks[:remaining])
    
    return result


# ========== 报告生成模块 ==========
def generate_report(date_str, classified, all_stock_picks, streaks):
    """生成文字报告"""
    lines = []
    lines.append(f"📊 板块追踪日报 | {date_str}")
    lines.append("=" * 40)
    
    # ===== 重点推荐（回踩EMA13买点）=====
    pullback_stocks = [s for s in all_stock_picks if s.get('is_pullback')]
    
    lines.append("")
    lines.append("⭐ 重点推荐（回踩EMA13买点）")
    lines.append("-" * 40)
    
    if pullback_stocks:
        for s in pullback_stocks:
            # 板块上下文：该股所在板块是热门还是潜力，排名多少
            sector_rank = ""
            for i, hs in enumerate(classified['hot'], 1):
                if hs['name'] == s['sector']:
                    sector_rank = f"🔥热门板块第{i}名"
                    break
            if not sector_rank:
                for i, ps in enumerate(classified['potential'], 1):
                    if ps['name'] == s['sector']:
                        sector_rank = f"💎潜力板块第{i}名"
                        break
            
            lines.append(f"┌─ {s['name']} {s['code']} [{s['sector']}] {sector_rank}")
            lines.append(f"│ 🟡 回踩买点 | 当前价 ¥{s['price']:.2f}")
            lines.append(f"│ EMA13: ¥{s['ema13']} | 距离 {s['ema13_dist_pct']:+.1f}%")
            lines.append(f"│ EMA55: ¥{s['ema55']} | {'✓ 上升趋势' if s['ema13'] > s['ema55'] else '✗ 趋势不明'}")
            lines.append(f"│ 量比: {s['vol_ratio']} | 5日涨幅: {s['change_5d']:+.1f}%")
            if s.get('hot_topic'):
                lines.append(f"│ 🏷️ {s['hot_topic']}")
            lines.append(f"│ ⚠ {s['risk']}")
            lines.append(f"└{'─' * 38}")
            lines.append("")
    else:
        lines.append("今日暂无回踩EMA13买点个股")
        lines.append("")
    
    # ===== 热门板块 TOP4 =====
    lines.append("🔥 热门板块 TOP4")
    lines.append("-" * 40)
    
    for i, s in enumerate(classified['hot'], 1):
        flow_str = f"{s.get('fund_flow', 0)/1e8:.1f}亿" if s.get('fund_flow', 0) > 0 else "—"
        up_total = s['up_count'] + s['down_count']
        if up_total > 0:
            up_pct = s['up_count'] / up_total * 100
            up_ratio = f"{up_pct:.0f}%"
        else:
            up_ratio = "—"
        
        lines.append(f"{i}. {s['name']} | 综合评分 {s['score']}")
        lines.append(f"   涨幅 {s['change_pct']:+.2f}% | 资金净流入 {flow_str} | 上涨占比 {up_ratio}")
        
        # 该板块推荐个股
        sector_stocks = [st for st in all_stock_picks if st['sector'] == s['name']]
        for st in sector_stocks:
            tag = "⭐" if st.get('is_pullback') else "•"
            lines.append(f"   {tag} {st['name']} {st['code']} {st['signal']} | {st['change_pct']:+.2f}%")
        lines.append("")
    
    # ===== 潜力板块 TOP7 =====
    lines.append("💎 潜力板块 TOP7")
    lines.append("-" * 40)
    
    for i, s in enumerate(classified['potential'], 1):
        flow_str = f"{s.get('fund_flow', 0)/1e8:.1f}亿" if s.get('fund_flow', 0) > 0 else "—"
        boost = " ⚡概念加持" if s.get('concept_boost') else ""
        
        lines.append(f"{i}. {s['name']} | 综合评分 {s['score']}{boost}")
        lines.append(f"   涨幅 {s['change_pct']:+.2f}% | 资金净流入 {flow_str}")
        
        # 该板块推荐个股
        sector_stocks = [st for st in all_stock_picks if st['sector'] == s['name']]
        for st in sector_stocks:
            tag = "⭐" if st.get('is_pullback') else "•"
            lines.append(f"   {tag} {st['name']} {st['code']} {st['signal']} | {st['change_pct']:+.2f}%")
        lines.append("")
    
    # ===== 热门概念补充 =====
    if classified['hot_concepts']:
        lines.append("🏷️ 热门概念板块")
        lines.append("-" * 40)
        for i, s in enumerate(classified['hot_concepts'][:8], 1):
            lines.append(f"  {i}. {s['name']} {s['change_pct']:+.2f}% | 领涨: {s['lead_stock']}")
        lines.append("")
    
    # ===== 板块轮动观察 =====
    lines.append("📊 板块轮动观察")
    lines.append("-" * 40)
    if streaks:
        for s in streaks[:5]:
            lines.append(f"  • {s['name']} 连续上榜{s['days']}天 ({s['categories']})")
    else:
        lines.append("  暂无足够历史数据，持续跟踪中...")
    lines.append("")
    
    # ===== 风险提示 =====
    high_pe = [s for s in all_stock_picks if s.get('pe', 0) > 100]
    lines.append("⚠️ 风险提示")
    lines.append("-" * 40)
    if high_pe:
        pe_str = "、".join([f"{s['name']}({s['pe']:.0f})" for s in high_pe])
        lines.append(f"  PE超100倍：{pe_str}")
    lines.append("  以上分析基于技术面，不构成投资建议")
    lines.append("  股市有风险，投资需谨慎")
    
    return "\n".join(lines)


# ========== 新闻热点分析（B方案补充）=========
def enrich_stock_with_concept(stock, concept_sectors):
    """用概念板块交叉验证，补充热点勾连信息（精简版：只查前5个热门概念）"""
    hot_concepts = []
    checked = 0
    for cs in concept_sectors[:50]:  # 检查前50个热门概念
        if cs['change_pct'] > 2:
            try:
                members = ak.stock_board_concept_cons_em(symbol=cs['name'])
                member_codes = set(members['代码'].astype(str).str.zfill(6).tolist())
                if stock['code'] in member_codes:
                    hot_concepts.append(f"{cs['name']}(+{cs['change_pct']:.1f}%)")
                checked += 1
                if checked >= 5:  # 最多查5个
                    break
                time.sleep(0.3)
            except:
                continue
    
    if hot_concepts:
        stock['hot_topic'] = f"所属热门概念：{'、'.join(hot_concepts[:3])}"
    else:
        stock['hot_topic'] = ""
    
    return stock


# ========== 主流程 ==========
def run_analysis():
    """执行完整分析流程"""
    # 加载配置
    config = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH) as f:
            config.update(json.load(f))
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    logger.info(f"===== 开始分析 {date_str} =====")
    
    # 1. 初始化数据库
    init_db()
    
    # 2. 采集数据
    logger.info("步骤1: 采集板块数据...")
    industry_sectors = get_industry_sectors()
    concept_sectors = get_concept_sectors()
    fund_flows = get_fund_flow_rank()
    time.sleep(1)
    
    if not industry_sectors:
        logger.error("行业板块数据获取失败，终止分析")
        return None
    
    # 3. 板块筛选
    logger.info("步骤2: 筛选热门和潜力板块...")
    classified = classify_sectors(industry_sectors, concept_sectors, fund_flows)
    
    # 4. 板块内选股
    logger.info("步骤3: 板块内选股...")
    all_stock_picks = []
    seen_codes = set()  # 去重
    
    # 合并所有目标板块（热门+潜力）
    target_sectors = classified['hot'] + classified['potential']
    
    for sector in target_sectors:
        logger.info(f"  分析板块: {sector['name']}")
        picks = screen_stocks_in_sector(sector, config)
        for p in picks:
            if p['code'] not in seen_codes:
                seen_codes.add(p['code'])
                all_stock_picks.append(p)
        time.sleep(1)
    
    # 5. 热点勾连分析（只对重点推荐的回踩买点股票做深度分析）
    logger.info("步骤4: 热点勾连分析（仅重点推荐股）...")
    pullback_candidates = [s for s in all_stock_picks if s.get('is_pullback')]
    for stock in pullback_candidates[:3]:  # 最多3只
        logger.info(f"  深度分析: {stock['name']} {stock['code']}")
        enrich_stock_with_concept(stock, concept_sectors)
    
    # 6. 获取历史轮动数据
    streaks = get_sector_streak(date_str)
    
    # 7. 保存数据
    logger.info("步骤5: 保存数据...")
    save_daily_results(date_str, classified, all_stock_picks)
    
    # 8. 生成报告
    logger.info("步骤6: 生成报告...")
    report = generate_report(date_str, classified, all_stock_picks, streaks)
    
    # 保存报告到文件
    report_path = REPORT_DIR / f"{date_str}.md"
    with open(report_path, 'w', encoding='utf-8') as f:
        f.write(report)
    logger.info(f"报告已保存: {report_path}")
    
    # 输出报告到stdout（供cron job读取）
    print("===REPORT_START===")
    print(report)
    print("===REPORT_END===")
    
    return report


if __name__ == "__main__":
    run_analysis()
