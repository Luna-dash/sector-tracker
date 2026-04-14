#!/usr/bin/env python3
"""A-share sector rotation tracker."""

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

try:
    import akshare as ak
    import baostock as bs
except ImportError:
    ak = None
    bs = None


MAX_RETRIES = 3
BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "data" / "sector_history.db"
REPORT_DIR = BASE_DIR / "reports"
CONFIG_PATH = BASE_DIR / "config.json"
CACHE_DIR = BASE_DIR / "cache"

DEFAULT_CONFIG = {
    "hot_sectors_count": 4,
    "potential_sectors_count": 7,
    "stocks_per_sector": 3,
    "ema_short": 13,
    "ema_long": 55,
    "ma_periods": [5, 10, 20, 60],
    "pullback_threshold": 0.015,
    "analysis_days": [5, 10, 20],
    "rps_periods": [10, 20, 50, 120, 250],
    "rps_threshold": 90,
    "report_time": "17:00",
    "use_cache": True,
    "sector_member_limit": 20,
    "min_stock_score": 55,
    "concept_scan_limit": 20,
    "concept_match_limit": 5,
    "concept_overlap_min_count": 2,
    "concept_overlap_min_ratio": 0.1,
    "score_weights": {
        "sector": {
            "change_pct": 30,
            "fund_flow": 30,
            "up_ratio": 25,
            "turnover": 15,
            "trend_bonus": 5,
            "concept_boost": 5,
        },
        "stock": {
            "ema_bullish": 30,
            "ema_rising": 15,
            "pullback": 40,
            "ma_bullish": 15,
            "volume_confirm": 10,
            "low_risk": 10,
            "sector_leader": 10,
            "rps_three_cycle_red": 8,
            "rps_four_cycle_red": 15,
            "big_order_alert": 8,
            "macd_ignite": 6,
            "turnaround": 10,
            "breakout": 18,
        },
    },
    "risk_filters": {
        "high_pe_warning": 100,
        "exclude_negative_pe": False,
        "max_change_5d": 12.0,
        "max_vol_ratio": 2.5,
        "max_volatility_10d": 8.0,
        "strict_mode": False,
    },
    "backtest": {
        "lookback_days": 30,
        "holding_days": [1, 3, 5],
        "min_samples": 5,
        "entry_price_basis": "signal_close",
    },
}
def configure_stdio_encoding():
    for stream_name in ("stdout", "stderr"):
        stream = getattr(sys, stream_name, None)
        if stream is None or not hasattr(stream, "reconfigure"):
            continue
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except ValueError:
            continue


configure_stdio_encoding()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

RUNTIME_META = {
    "analysis_date": None,
    "notes": [],
    "sources": {},
    "cache_stats": defaultdict(lambda: {"live": 0, "cache": 0}),
}


def ensure_dirs():
    DB_PATH.parent.mkdir(exist_ok=True)
    REPORT_DIR.mkdir(exist_ok=True)
    CACHE_DIR.mkdir(exist_ok=True)


def ensure_dependencies():
    if ak is None or bs is None:
        raise RuntimeError("missing dependencies: please install akshare and baostock")


def reset_runtime_meta():
    RUNTIME_META["analysis_date"] = None
    RUNTIME_META["notes"] = []
    RUNTIME_META["sources"] = {}
    RUNTIME_META["cache_stats"] = defaultdict(lambda: {"live": 0, "cache": 0})


def get_proxy_environment():
    keys = [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
        "no_proxy",
    ]
    return {key: os.environ.get(key) for key in keys if os.environ.get(key)}


def build_network_diagnostics():
    diagnostics = {
        "has_akshare": ak is not None,
        "has_baostock": bs is not None,
        "proxy_env": get_proxy_environment(),
        "cwd": str(BASE_DIR),
        "cache_dir": str(CACHE_DIR),
        "cache_files": len(list(CACHE_DIR.glob("*.json"))) if CACHE_DIR.exists() else 0,
    }
    diagnostics["proxy_enabled"] = bool(diagnostics["proxy_env"])
    return diagnostics


def deep_merge(base, patch):
    result = dict(base)
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def load_config():
    config = DEFAULT_CONFIG.copy()
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, encoding="utf-8") as file:
            config = deep_merge(config, json.load(file))

    config["hot_sectors_count"] = max(1, int(config.get("hot_sectors_count", 4)))
    config["potential_sectors_count"] = max(1, int(config.get("potential_sectors_count", 7)))
    config["stocks_per_sector"] = max(1, int(config.get("stocks_per_sector", 3)))
    config["ema_short"] = max(2, int(config.get("ema_short", 13)))
    config["ema_long"] = max(config["ema_short"] + 1, int(config.get("ema_long", 55)))
    config["pullback_threshold"] = max(0.001, float(config.get("pullback_threshold", 0.015)))
    config["sector_member_limit"] = max(5, int(config.get("sector_member_limit", 20)))
    config["min_stock_score"] = max(0, float(config.get("min_stock_score", 55)))
    config["concept_scan_limit"] = max(5, int(config.get("concept_scan_limit", 20)))
    config["concept_match_limit"] = max(1, int(config.get("concept_match_limit", 5)))
    config["concept_overlap_min_count"] = max(1, int(config.get("concept_overlap_min_count", 2)))
    config["concept_overlap_min_ratio"] = min(1.0, max(0.01, float(config.get("concept_overlap_min_ratio", 0.1))))

    ma_periods = config.get("ma_periods", [5, 10, 20, 60])
    if not isinstance(ma_periods, list) or len(ma_periods) < 4:
        ma_periods = [5, 10, 20, 60]
    config["ma_periods"] = [int(item) for item in ma_periods[:4]]

    analysis_days = config.get("analysis_days", [5, 10, 20])
    if not isinstance(analysis_days, list) or not analysis_days:
        analysis_days = [5, 10, 20]
    normalized_days = []
    for item in analysis_days:
        try:
            value = int(item)
        except (TypeError, ValueError):
            continue
        if value > 0:
            normalized_days.append(value)
    config["analysis_days"] = normalized_days or [5, 10, 20]

    rps_periods = config.get("rps_periods", [10, 20, 50, 120, 250])
    if not isinstance(rps_periods, list) or not rps_periods:
        rps_periods = [10, 20, 50, 120, 250]
    normalized_rps_periods = []
    for item in rps_periods:
        try:
            value = int(item)
        except (TypeError, ValueError):
            continue
        if value > 0 and value not in normalized_rps_periods:
            normalized_rps_periods.append(value)
    config["rps_periods"] = normalized_rps_periods or [10, 20, 50, 120, 250]
    config["rps_threshold"] = min(100.0, max(0.0, float(config.get("rps_threshold", 90))))

    backtest_cfg = config.get("backtest", {})
    holding_days = backtest_cfg.get("holding_days", [1, 3, 5])
    if not isinstance(holding_days, list) or not holding_days:
        holding_days = [1, 3, 5]
    backtest_cfg["holding_days"] = sorted({int(day) for day in holding_days if int(day) > 0})
    backtest_cfg["lookback_days"] = max(1, int(backtest_cfg.get("lookback_days", 30)))
    backtest_cfg["min_samples"] = max(1, int(backtest_cfg.get("min_samples", 5)))
    entry_price_basis = str(backtest_cfg.get("entry_price_basis", "signal_close")).strip().lower()
    if entry_price_basis != "signal_close":
        entry_price_basis = "signal_close"
    backtest_cfg["entry_price_basis"] = entry_price_basis
    config["backtest"] = backtest_cfg
    return config


def parse_args():
    parser = argparse.ArgumentParser(description="A-share sector rotation tracker")
    parser.add_argument("--date", help="analysis date, format YYYY-MM-DD")
    parser.add_argument("--no-cache", action="store_true", help="disable local cache")
    parser.add_argument("--cache-only", action="store_true", help="read only local cache and skip network fetches")
    parser.add_argument("--offline", action="store_true", help="alias of --cache-only")
    parser.add_argument("--diagnose-network", action="store_true", help="print proxy related diagnostics and exit")
    parser.add_argument("--sector-limit", type=int, help="limit hot/potential sector counts")
    parser.add_argument("--sector", action="append", help="only keep picks in the given sector, repeatable")
    parser.add_argument("--only-pullback", action="store_true", help="only keep pullback picks")
    parser.add_argument("--min-score", type=float, default=None, help="minimum stock score threshold")
    parser.add_argument("--exclude-high-pe", action="store_true", help="exclude picks over high PE warning line")
    parser.add_argument("--alerts-only", action="store_true", help="only output alert summary")
    parser.add_argument("--output-json", help="custom json output path")
    parser.add_argument("--skip-backtest", action="store_true", help="skip backtest summary")
    return parser.parse_args()


def to_float(value, default=0.0):
    try:
        if pd.isna(value):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def to_int(value, default=0):
    try:
        if pd.isna(value):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def calc_latest_change_pct(df):
    if df.empty or len(df) < 2 or "close" not in df.columns:
        return 0.0
    prev_close = to_float(df["close"].iloc[-2], 0.0)
    latest_close = to_float(df["close"].iloc[-1], 0.0)
    if prev_close <= 0:
        return 0.0
    return (latest_close - prev_close) / prev_close * 100


def resolve_signal_entry(history, signal_date):
    if history.empty or "date" not in history.columns or "close" not in history.columns:
        return None, None
    history = history.reset_index(drop=True)
    try:
        signal_index = history.index[history["date"] == signal_date][0]
    except IndexError:
        return None, None

    entry_price = to_float(history.iloc[signal_index]["close"], 0.0)
    if entry_price <= 0:
        return None, None
    return signal_index, entry_price


def json_default(value):
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.strftime("%Y-%m-%d")
    raise TypeError(f"Unsupported type: {type(value)!r}")


def cache_key(*parts):
    raw = "::".join(str(part) for part in parts)
    return hashlib.md5(raw.encode("utf-8")).hexdigest()


def save_cache(name, data):
    ensure_dirs()
    cache_file = CACHE_DIR / f"{name}.json"
    with open(cache_file, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, default=json_default)


def load_cache(name):
    cache_file = CACHE_DIR / f"{name}.json"
    if not cache_file.exists():
        return None
    try:
        with open(cache_file, encoding="utf-8") as file:
            return json.load(file)
    except (OSError, ValueError):
        return None


def note(message):
    RUNTIME_META["notes"].append(message)
    logger.info(message)


def record_source(name, source, records=None, cache_name=None):
    RUNTIME_META["sources"][name] = {
        "source": source,
        "records": records,
        "cache_name": cache_name,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


def bump_cache_stat(name, source):
    if source not in ("live", "cache"):
        return
    RUNTIME_META["cache_stats"][name][source] += 1


def is_sector_data_valid(items):
    return bool(items) and any(abs(item.get("change_pct", 0)) > 0 for item in items[:5])


def fetch_with_retry(name, fetcher, cache_name=None, validator=None, sleep_seconds=3, use_cache=True, cache_only=False):
    if cache_only:
        if use_cache and cache_name:
            cached = load_cache(cache_name)
            if cached:
                record_source(name, "cache", records=len(cached.get("data", [])), cache_name=cache_name)
                bump_cache_stat(name, "cache")
                return cached.get("data", [])
        record_source(name, "cache_miss", records=0, cache_name=cache_name)
        note(f"cache-only mode: no cache available for {name}")
        return [] if cache_name else {}

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            data = fetcher()
            if validator and not validator(data):
                raise ValueError("invalid data")
            if use_cache and cache_name:
                save_cache(cache_name, {"date": datetime.now().strftime("%Y-%m-%d"), "data": data})
            record_source(name, "live", records=len(data) if hasattr(data, "__len__") else None, cache_name=cache_name)
            bump_cache_stat(name, "live")
            return data
        except Exception as exc:  # noqa: BLE001
            logger.warning("%s fetch failed (%s/%s): %s", name, attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(sleep_seconds)

    if use_cache and cache_name:
        cached = load_cache(cache_name)
        if cached:
            record_source(name, "cache", records=len(cached.get("data", [])), cache_name=cache_name)
            bump_cache_stat(name, "cache")
            return cached.get("data", [])

    record_source(name, "empty", records=0, cache_name=cache_name)
    return [] if cache_name else {}


def init_db():
    ensure_dirs()
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_sector_ranking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            sector_name TEXT NOT NULL,
            sector_type TEXT NOT NULL,
            rank INTEGER,
            score REAL,
            change_pct REAL,
            fund_flow REAL,
            volume_ratio REAL,
            ma_status TEXT,
            category TEXT,
            UNIQUE(date, sector_name, sector_type)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_stock_picks (
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
            signal TEXT,
            score REAL,
            pe_ratio REAL,
            risk_note TEXT,
            hot_topic TEXT,
            summary TEXT,
            UNIQUE(date, stock_code)
        )
        """
    )
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS market_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            snapshot_type TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(date, snapshot_type)
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sector_ranking_date ON daily_sector_ranking(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_picks_date ON daily_stock_picks(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_market_snapshots_date ON market_snapshots(date)")
    conn.commit()
    conn.close()


def save_market_snapshot(date_str, industry_sectors, concept_sectors, fund_flows):
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    snapshot_rows = {
        "industry": industry_sectors,
        "concept": concept_sectors,
        "fund_flow": fund_flows,
    }
    for snapshot_type, payload in snapshot_rows.items():
        cursor.execute(
            """
            INSERT OR REPLACE INTO market_snapshots (date, snapshot_type, payload, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (date_str, snapshot_type, json.dumps(payload, ensure_ascii=False), created_at),
        )
    conn.commit()
    conn.close()


def load_market_snapshot(date_str):
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT snapshot_type, payload
        FROM market_snapshots
        WHERE date = ?
        """,
        (date_str,),
    )
    rows = cursor.fetchall()
    conn.close()
    if not rows:
        return None

    result = {}
    for snapshot_type, payload in rows:
        result[snapshot_type] = json.loads(payload)
    if {"industry", "concept", "fund_flow"} - set(result):
        return None
    return result


def save_daily_results(date_str, sector_results, stock_picks):
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    for category in ("hot", "potential"):
        for rank, sector in enumerate(sector_results.get(category, []), 1):
            cursor.execute(
                """
                INSERT OR REPLACE INTO daily_sector_ranking
                (date, sector_name, sector_type, rank, score, change_pct, fund_flow, volume_ratio, ma_status, category)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    date_str,
                    sector.get("name", ""),
                    sector.get("type", category),
                    rank,
                    sector.get("score", 0),
                    sector.get("change_pct", 0),
                    sector.get("fund_flow", 0),
                    sector.get("volume_ratio", 0),
                    sector.get("ma_status", "neutral"),
                    category,
                ),
            )
    for stock in stock_picks:
        cursor.execute(
            """
            INSERT OR REPLACE INTO daily_stock_picks
            (date, sector_name, stock_code, stock_name, price, change_pct, ema13, ema55, ema13_dist_pct,
             signal, score, pe_ratio, risk_note, hot_topic, summary)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                date_str,
                stock["sector"],
                stock["code"],
                stock["name"],
                stock["price"],
                stock["change_pct"],
                stock.get("ema13", 0),
                stock.get("ema55", 0),
                stock.get("ema13_dist_pct", 0),
                stock["signal"],
                stock["score"],
                stock.get("pe", 0),
                stock.get("risk", ""),
                stock.get("hot_topic", ""),
                stock.get("summary", ""),
            ),
        )
    conn.commit()
    conn.close()


def get_previous_analysis_date(date_str):
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT MAX(date)
        FROM daily_sector_ranking
        WHERE date < ?
        """,
        (date_str,),
    )
    row = cursor.fetchone()
    conn.close()
    return row[0] if row and row[0] else None


def load_saved_analysis(date_str):
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH))
    sector_query = """
    SELECT sector_name, sector_type, rank, score, change_pct, fund_flow, category
    FROM daily_sector_ranking
    WHERE date = ?
    ORDER BY category ASC, rank ASC
    """
    stock_query = """
    SELECT sector_name, stock_code, stock_name, price, change_pct, signal, score, pe_ratio, risk_note, hot_topic, summary
    FROM daily_stock_picks
    WHERE date = ?
    ORDER BY score DESC, stock_code ASC
    """
    sector_rows = pd.read_sql_query(sector_query, conn, params=(date_str,))
    stock_rows = pd.read_sql_query(stock_query, conn, params=(date_str,))
    conn.close()

    if sector_rows.empty and stock_rows.empty:
        return None

    classified = {"hot": [], "potential": [], "hot_concepts": []}
    for _, row in sector_rows.iterrows():
        item = {
            "name": row["sector_name"],
            "type": row["sector_type"],
            "rank": to_int(row["rank"]),
            "score": to_float(row["score"]),
            "change_pct": to_float(row["change_pct"]),
            "fund_flow": to_float(row["fund_flow"]),
        }
        category = row["category"]
        if category in classified:
            classified[category].append(item)

    picks = []
    for _, row in stock_rows.iterrows():
        picks.append(
            {
                "sector": row["sector_name"],
                "code": row["stock_code"],
                "name": row["stock_name"],
                "price": to_float(row["price"]),
                "change_pct": to_float(row["change_pct"]),
                "signal": row["signal"],
                "score": to_float(row["score"]),
                "pe": to_float(row["pe_ratio"]),
                "risk": row["risk_note"] or "",
                "hot_topic": row["hot_topic"] or "",
                "summary": row["summary"] or "",
            }
        )
    return {"classified": classified, "stock_picks": picks}


def get_sector_streak(date_str):
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    start_date = (datetime.strptime(date_str, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d")
    cursor.execute(
        """
        SELECT current.sector_name, current.sector_type, current.category, history.days, history.categories
        FROM daily_sector_ranking AS current
        JOIN (
            SELECT sector_name, sector_type, COUNT(*) AS days, GROUP_CONCAT(DISTINCT category) AS categories
            FROM daily_sector_ranking
            WHERE date >= ? AND date <= ?
            GROUP BY sector_name, sector_type
            HAVING COUNT(*) >= 2
        ) AS history
          ON current.sector_name = history.sector_name
         AND current.sector_type = history.sector_type
        WHERE current.date = ?
        ORDER BY history.days DESC, current.rank ASC
        """,
        (start_date, date_str, date_str),
    )
    rows = cursor.fetchall()
    conn.close()
    return [
        {"name": row[0], "type": row[1], "current_category": row[2], "days": row[3], "categories": row[4] or ""}
        for row in rows
    ]


def get_industry_sectors(use_cache=True, cache_only=False):
    def fetch():
        df = ak.stock_board_industry_name_em()
        result = []
        for _, row in df.iterrows():
            result.append(
                {
                    "name": row["板块名称"],
                    "code": row["板块代码"],
                    "price": to_float(row["最新价"]),
                    "change_pct": to_float(row["涨跌幅"]),
                    "total_mv": to_float(row["总市值"]),
                    "turnover": to_float(row["换手率"]),
                    "up_count": to_int(row["上涨家数"]),
                    "down_count": to_int(row["下跌家数"]),
                    "lead_stock": row["领涨股票"],
                    "lead_change": to_float(row["领涨股票-涨跌幅"]),
                    "type": "industry",
                }
            )
        return result

    cache_name = f"industry_{datetime.now():%Y%m%d}"
    return fetch_with_retry(
        "industry",
        fetch,
        cache_name=cache_name,
        validator=is_sector_data_valid,
        sleep_seconds=5,
        use_cache=use_cache,
        cache_only=cache_only,
    )


def get_concept_sectors(use_cache=True, cache_only=False):
    def fetch():
        df = ak.stock_board_concept_name_em()
        result = []
        for _, row in df.iterrows():
            result.append(
                {
                    "name": row["板块名称"],
                    "code": row["板块代码"],
                    "price": to_float(row["最新价"]),
                    "change_pct": to_float(row["涨跌幅"]),
                    "total_mv": to_float(row["总市值"]),
                    "turnover": to_float(row["换手率"]),
                    "up_count": to_int(row["上涨家数"]),
                    "down_count": to_int(row["下跌家数"]),
                    "lead_stock": row["领涨股票"],
                    "lead_change": to_float(row["领涨股票-涨跌幅"]),
                    "type": "concept",
                }
            )
        return result

    cache_name = f"concept_{datetime.now():%Y%m%d}"
    return fetch_with_retry(
        "concept",
        fetch,
        cache_name=cache_name,
        validator=is_sector_data_valid,
        sleep_seconds=5,
        use_cache=use_cache,
        cache_only=cache_only,
    )


def get_fund_flow_rank(use_cache=True, cache_only=False):
    def fetch():
        df = ak.stock_sector_fund_flow_rank(indicator="今日", sector_type="行业资金流")
        flows = {}
        for _, row in df.iterrows():
            flows[row["名称"]] = {
                "today_flow": to_float(row["今日主力净流入-净额"]),
                "today_flow_pct": to_float(row["今日主力净流入-净占比"]),
                "super_big_flow": to_float(row["今日超大单净流入-净额"]),
            }
        return flows

    cache_name = f"fund_flow_{datetime.now():%Y%m%d}"
    return fetch_with_retry(
        "fund_flow",
        fetch,
        cache_name=cache_name,
        sleep_seconds=3,
        use_cache=use_cache,
        cache_only=cache_only,
    )


def get_sector_members(
    sector_name,
    sector_type="industry",
    use_cache=True,
    cache_only=False,
    snapshot_date=None,
    allow_live=None,
):
    snapshot_date = snapshot_date or datetime.now().strftime("%Y-%m-%d")
    snapshot_key = snapshot_date.replace("-", "")
    if allow_live is None:
        allow_live = snapshot_date == datetime.now().strftime("%Y-%m-%d")
    hashed = cache_key("members", sector_type, sector_name, snapshot_key)
    cache_name = f"members_{hashed}"
    if use_cache:
        cached = load_cache(cache_name)
        if cached:
            bump_cache_stat("sector_members", "cache")
            return cached.get("data", [])
    if cache_only:
        note(f"cache-only mode: no cache available for sector members {sector_type}:{sector_name}")
        return []
    if not allow_live:
        note(f"historical member snapshot missing for {snapshot_date} {sector_type}:{sector_name}")
        return []

    try:
        if sector_type == "industry":
            df = ak.stock_board_industry_cons_em(symbol=sector_name)
        else:
            df = ak.stock_board_concept_cons_em(symbol=sector_name)
    except Exception as exc:  # noqa: BLE001
        logger.error("get sector members failed [%s]: %s", sector_name, exc)
        return []

    result = []
    for _, row in df.iterrows():
        result.append(
            {
                "code": str(row["代码"]).zfill(6),
                "name": row["名称"],
                "price": to_float(row["最新价"]),
                "change_pct": to_float(row["涨跌幅"]),
                "volume": to_float(row["成交量"]),
                "amount": to_float(row["成交额"]),
                "turnover": to_float(row["换手率"]),
                "pe": to_float(row["市盈率-动态"]),
                "pb": to_float(row["市净率"]),
            }
        )
    if use_cache:
        save_cache(cache_name, {"data": result})
    bump_cache_stat("sector_members", "live")
    return result


def build_hot_concept_member_map(concept_sectors, config, analysis_date, use_cache=True, cache_only=False):
    hot_concepts = [
        item
        for item in concept_sectors[: config["concept_scan_limit"]]
        if item.get("change_pct", 0) > 2
    ]
    member_map = defaultdict(list)
    concept_members = {}
    for concept in hot_concepts:
        members = get_sector_members(
            concept["name"],
            "concept",
            use_cache=use_cache,
            cache_only=cache_only,
            snapshot_date=analysis_date,
        )
        codes = {item["code"] for item in members if item.get("code")}
        if not codes:
            continue
        concept_members[concept["name"]] = codes
        for code in codes:
            member_map[code].append(
                {
                    "name": concept["name"],
                    "change_pct": concept.get("change_pct", 0),
                }
            )
    return hot_concepts, dict(member_map), concept_members


def get_stock_history(code, end_date=None, days=120, use_cache=True, cache_only=False):
    end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    hashed = cache_key("history", code, end_date, days)
    cache_name = f"history_{hashed}"
    if use_cache:
        cached = load_cache(cache_name)
        if cached:
            bump_cache_stat("stock_history", "cache")
            return pd.DataFrame(cached.get("data", []))
    if cache_only:
        note(f"cache-only mode: no cache available for stock history {code}")
        return pd.DataFrame()

    try:
        bs_code = f"sh.{code}" if code.startswith(("5", "6", "9")) else f"sz.{code}"
        start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days + 30)).strftime("%Y-%m-%d")
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,open,high,low,close,volume,amount,turn",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="2",
        )
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=rs.fields)
        for column in ("open", "high", "low", "close", "volume", "amount", "turn"):
            df[column] = pd.to_numeric(df[column], errors="coerce")
        df = df.dropna(subset=["close"]).reset_index(drop=True)
        if use_cache:
            save_cache(cache_name, {"data": df.to_dict("records")})
        bump_cache_stat("stock_history", "live")
        return df
    except Exception as exc:  # noqa: BLE001
        logger.error("get history failed [%s]: %s", code, exc)
        return pd.DataFrame()


def get_benchmark_history(name, bs_code, end_date=None, days=120, use_cache=True, cache_only=False):
    end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    hashed = cache_key("benchmark", name, bs_code, end_date, days)
    cache_name = f"benchmark_{hashed}"
    if use_cache:
        cached = load_cache(cache_name)
        if cached:
            bump_cache_stat("benchmark_history", "cache")
            return pd.DataFrame(cached.get("data", []))
    if cache_only:
        note(f"cache-only mode: no cache available for benchmark {name}")
        return pd.DataFrame()

    try:
        start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days + 30)).strftime("%Y-%m-%d")
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,close",
            start_date=start_date,
            end_date=end_date,
            frequency="d",
            adjustflag="3",
        )
        rows = []
        while rs.error_code == "0" and rs.next():
            rows.append(rs.get_row_data())
        if not rows:
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=rs.fields)
        df["close"] = pd.to_numeric(df["close"], errors="coerce")
        df = df.dropna(subset=["close"]).reset_index(drop=True)
        if use_cache:
            save_cache(cache_name, {"data": df.to_dict("records")})
        bump_cache_stat("benchmark_history", "live")
        return df
    except Exception as exc:  # noqa: BLE001
        logger.error("get benchmark history failed [%s]: %s", name, exc)
        return pd.DataFrame()


def calc_ema(series, period):
    return series.ewm(span=period, adjust=False).mean()


def calc_ma(series, period):
    return series.rolling(window=period).mean()


def calc_tdx_sma(series, period, weight):
    result = []
    prev = None
    for value in series.fillna(0):
        value = float(value)
        if prev is None:
            prev = value
        else:
            prev = (weight * value + (period - weight) * prev) / period
        result.append(prev)
    return pd.Series(result, index=series.index, dtype="float64")


def crossed_above(left, right):
    if len(left) < 2 or len(right) < 2:
        return False
    return left.iloc[-2] <= right.iloc[-2] and left.iloc[-1] > right.iloc[-1]


def analyze_stock_tech(df, config):
    ma_periods = config["ma_periods"]
    analysis_days = config["analysis_days"]
    rps_periods = config["rps_periods"]
    ema_short = config["ema_short"]
    ema_long = config["ema_long"]
    required_days = sorted(set(analysis_days) | set(rps_periods))
    min_length = max(ema_long, max(ma_periods), max(required_days) + 1, 20)
    if df.empty or len(df) < min_length:
        return None

    close = df["close"]
    volume = df["volume"]
    ema_short_series = calc_ema(close, ema_short)
    ema_long_series = calc_ema(close, ema_long)
    ma_short = calc_ma(close, ma_periods[0])
    ma_mid = calc_ma(close, ma_periods[1])
    ma_long = calc_ma(close, ma_periods[2])
    ma_longer = calc_ma(close, ma_periods[3])

    latest = {
        "price": float(close.iloc[-1]),
        "ema_short": float(ema_short_series.iloc[-1]),
        "ema_long": float(ema_long_series.iloc[-1]),
        "ema13": float(ema_short_series.iloc[-1]),
        "ema55": float(ema_long_series.iloc[-1]),
        "ma5": float(ma_short.iloc[-1]) if not pd.isna(ma_short.iloc[-1]) else 0,
        "ma10": float(ma_mid.iloc[-1]) if not pd.isna(ma_mid.iloc[-1]) else 0,
        "ma20": float(ma_long.iloc[-1]) if not pd.isna(ma_long.iloc[-1]) else 0,
        "ma60": float(ma_longer.iloc[-1]) if not pd.isna(ma_longer.iloc[-1]) else 0,
    }
    latest["ema_dist_pct"] = (latest["price"] - latest["ema_short"]) / latest["ema_short"]
    latest["ema13_dist_pct"] = latest["ema_dist_pct"]
    latest["ema_bullish"] = latest["ema_short"] > latest["ema_long"]
    latest["ema13_rising"] = ema_short_series.iloc[-1] > ema_short_series.iloc[-3] if len(ema_short_series) >= 3 else False
    latest["ma_bullish"] = (
        latest["ma5"] > latest["ma10"] > latest["ma20"]
        if all([latest["ma5"], latest["ma10"], latest["ma20"]])
        else False
    )
    vol_ma5 = volume.tail(5).mean()
    latest["vol_ratio"] = float(volume.iloc[-1] / vol_ma5) if vol_ma5 > 0 else 1.0
    latest["volatility_10d"] = float(close.pct_change().tail(10).std() * 100) if len(close) >= 11 else 0.0

    for day in required_days:
        key = f"change_{day}d"
        latest[key] = float((close.iloc[-1] - close.iloc[-(day + 1)]) / close.iloc[-(day + 1)] * 100) if len(close) >= day + 1 else 0.0
    latest.setdefault("change_5d", 0.0)
    latest.setdefault("change_10d", 0.0)
    return latest


def check_pullback_signal(tech, config):
    if not tech:
        return False, {}
    threshold = config["pullback_threshold"]
    conditions = {
        "ema_bullish": bool(tech["ema_bullish"]),
        "ema_rising": bool(tech["ema13_rising"]),
        "near_ema": abs(tech["ema_dist_pct"]) < threshold,
        "shrink_volume": tech["vol_ratio"] < 1.05,
        "positive_5d": tech["change_5d"] > 0,
    }
    is_pullback = conditions["ema_bullish"] and conditions["near_ema"] and sum(conditions.values()) >= 4
    return is_pullback, conditions


def get_signal_type(tech, is_pullback):
    if not tech:
        return "观察"
    if is_pullback:
        return "回踩买点"
    if tech["ema_bullish"] and tech["ema13_rising"]:
        gap = (tech["ema_short"] - tech["ema_long"]) / tech["ema_long"]
        if 0 < gap < 0.03:
            return "趋势启动"
    if tech["ema_bullish"]:
        return "趋势延续"
    return "观察"


def score_sector(sector, fund_flows, config):
    weights = config["score_weights"]["sector"]
    score = 0.0
    change_pct = sector["change_pct"]
    if change_pct > 5:
        score += weights["change_pct"]
    elif change_pct > 3:
        score += weights["change_pct"] * 0.85
    elif change_pct > 1:
        score += weights["change_pct"] * 0.65
    elif change_pct > 0:
        score += weights["change_pct"] * 0.45
    else:
        score += weights["change_pct"] * 0.15

    flow = fund_flows.get(sector["name"], {}).get("today_flow", 0)
    if flow > 1e9:
        score += weights["fund_flow"]
    elif flow > 5e8:
        score += weights["fund_flow"] * 0.85
    elif flow > 0:
        score += weights["fund_flow"] * 0.65
    else:
        score += weights["fund_flow"] * 0.2
    sector["fund_flow"] = flow

    total = sector["up_count"] + sector["down_count"]
    up_ratio = sector["up_count"] / total if total > 0 else 0
    score += up_ratio * weights["up_ratio"]

    if sector["turnover"] > 3:
        score += weights["turnover"]
    elif sector["turnover"] > 1.5:
        score += weights["turnover"] * 0.67
    else:
        score += weights["turnover"] * 0.34

    if sector["lead_change"] > sector["change_pct"] and sector["change_pct"] > 0:
        score += weights["trend_bonus"]

    sector["score"] = round(score, 2)
    sector["breadth_ratio"] = round(up_ratio * 100, 2)
    return sector


def classify_sectors(industry_sectors, concept_sectors, fund_flows, config):
    scored_industries = [score_sector(dict(item), fund_flows, config) for item in industry_sectors]
    industry_sorted = sorted(scored_industries, key=lambda item: item["score"], reverse=True)

    hot_count = config["hot_sectors_count"]
    potential_count = config["potential_sectors_count"]
    if config.get("_sector_limit"):
        hot_count = min(hot_count, config["_sector_limit"])
        potential_count = min(potential_count, config["_sector_limit"])

    hot = industry_sorted[:hot_count]
    potential_candidates = []
    for sector in industry_sorted[hot_count:]:
        total = sector["up_count"] + sector["down_count"]
        up_ratio = sector["up_count"] / total if total > 0 else 0
        if sector["change_pct"] < 4 and (sector.get("fund_flow", 0) > 0 or up_ratio > 0.6):
            sector["concept_boost"] = False
            sector["related_concepts"] = []
            potential_candidates.append(sector)

    potential = sorted(potential_candidates, key=lambda item: item["score"], reverse=True)[:potential_count]
    hot_concepts = [item for item in concept_sectors if item["change_pct"] > 2][:10]
    return {"hot": hot, "potential": potential, "hot_concepts": hot_concepts}


def apply_sector_concept_boost(
    classified,
    concept_members,
    config,
    analysis_date,
    use_cache=True,
    cache_only=False,
):
    if not classified.get("potential") or not concept_members:
        return classified

    min_overlap = config["concept_overlap_min_count"]
    min_ratio = config["concept_overlap_min_ratio"]
    for sector in classified["potential"]:
        members = get_sector_members(
            sector["name"],
            sector.get("type", "industry"),
            use_cache=use_cache,
            cache_only=cache_only,
            snapshot_date=analysis_date,
        )
        sector_codes = {item["code"] for item in members if item.get("code")}
        if not sector_codes:
            continue

        matches = []
        for concept_name, concept_codes in concept_members.items():
            overlap = len(sector_codes & concept_codes)
            if overlap <= 0:
                continue
            overlap_ratio = overlap / len(sector_codes)
            if overlap >= min_overlap and overlap_ratio >= min_ratio:
                matches.append((overlap, overlap_ratio, concept_name))

        if not matches:
            continue

        matches.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
        sector["concept_boost"] = True
        sector["related_concepts"] = [item[2] for item in matches[: config["concept_match_limit"]]]
        sector["score"] = round(sector["score"] + config["score_weights"]["sector"]["concept_boost"], 2)

    classified["potential"] = sorted(classified["potential"], key=lambda item: item["score"], reverse=True)
    return classified


def build_risk_assessment(stock, tech, config):
    risk_cfg = config["risk_filters"]
    notes = []
    penalty = 0
    blocked = False

    if stock["pe"] > risk_cfg["high_pe_warning"]:
        notes.append(f"PE {stock['pe']:.0f} 偏高")
        penalty += 6
    if stock["pe"] < 0:
        notes.append(f"PE {stock['pe']:.0f}")
        if risk_cfg["exclude_negative_pe"]:
            blocked = True
    if tech["change_5d"] > risk_cfg["max_change_5d"]:
        notes.append(f"5日涨幅过高 {tech['change_5d']:.1f}%")
        penalty += 8
    if tech["vol_ratio"] > risk_cfg["max_vol_ratio"]:
        notes.append(f"量比过高 {tech['vol_ratio']:.2f}")
        penalty += 5
    if tech["volatility_10d"] > risk_cfg["max_volatility_10d"]:
        notes.append(f"10日波动偏高 {tech['volatility_10d']:.1f}%")
        penalty += 5
    return notes or ["正常"], penalty, blocked


def build_condition_summary(conditions):
    label_map = {
        "ema_bullish": "EMA多头",
        "ema_rising": "EMA向上",
        "near_ema": "靠近EMA",
        "shrink_volume": "缩量/平量",
        "positive_5d": "5日为正",
    }
    matched = [label_map[key] for key, value in conditions.items() if value]
    failed = [label_map[key] for key, value in conditions.items() if not value]
    return matched, failed


def build_sector_leaders(members):
    ranked = sorted(
        members,
        key=lambda item: (to_float(item.get("amount")), to_float(item.get("change_pct"))),
        reverse=True,
    )
    return {item["code"] for item in ranked[:3]}


def rank_sector_members(members):
    return sorted(
        members,
        key=lambda item: (
            to_float(item.get("amount")),
            to_float(item.get("turnover")),
            to_float(item.get("change_pct")),
            to_float(item.get("price")),
        ),
        reverse=True,
    )


def get_signal_grade(score, is_pullback, risk_notes, is_sector_leader):
    if is_pullback and score >= 80 and risk_notes == ["正常"]:
        return "A"
    if score >= 70 or (is_sector_leader and score >= 60):
        return "B"
    if score >= 50:
        return "C"
    return "D"


def summarize_returns(returns):
    if not returns:
        return None
    return {
        "samples": len(returns),
        "avg_return": round(sum(returns) / len(returns), 2),
        "win_rate": round(sum(1 for item in returns if item > 0) / len(returns) * 100, 2),
        "best": round(max(returns), 2),
        "worst": round(min(returns), 2),
    }


def build_daily_strategy_state(df, stock):
    if df.empty or len(df) < 250:
        return {
            "ddx": 0.0,
            "ddx_positive_days_5": 0,
            "big_order_alert": False,
            "macd_ignite": False,
            "turnaround_signal": False,
            "breakout_signal": False,
            "stop_profit_line": None,
            "strategy_tags": [],
            "strategy_summary": "无",
        }

    close = df["close"].astype(float)
    open_ = df["open"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    volume = df["volume"].astype(float)
    amount = df["amount"].astype(float)
    turn = df["turn"].astype(float)
    prev_close = close.shift(1).fillna(close)

    k1 = pd.Series(
        [
            (h - o + c - l + c - pc) if c >= pc else (h - o + c - l)
            for h, o, c, l, pc in zip(high, open_, close, low, prev_close)
        ],
        index=df.index,
        dtype="float64",
    )
    k2 = pd.Series(
        [
            (o - l + h - c + pc - c) if c <= pc else (o - l + h - c)
            for o, l, h, c, pc in zip(open_, low, high, close, prev_close)
        ],
        index=df.index,
        dtype="float64",
    )
    denom = (k1 + k2).replace(0, pd.NA)
    net_order_bias = ((k1 - k2) / denom).fillna(0.0)
    ddx = calc_ema(net_order_bias * turn, 5)
    ddx1 = calc_tdx_sma(ddx, 3, 1)
    ddx2 = calc_tdx_sma(ddx1, 3, 1)

    volume_ma5 = calc_ma(volume, 5)
    big_order_alert = bool(
        ddx.iloc[-1] > 0
        and volume_ma5.iloc[-1] > calc_ma(volume_ma5.fillna(0.0), 5).iloc[-1]
        and crossed_above(ddx1, ddx2)
    )
    ddx_positive_days_5 = int((ddx.tail(5) > 0).sum())

    dif = calc_ema(close, 12) - calc_ema(close, 26)
    dea = calc_ema(dif, 9)
    macd = (dif - dea) * 2
    hdh = bool(
        len(macd) >= 3
        and macd.iloc[-1] >= macd.iloc[-2]
        and macd.iloc[-3] > macd.iloc[-2]
        and macd.iloc[-1] > 0
    )
    limit_up = bool(close.iloc[-1] / close.iloc[-2] >= 1.0985)
    macd_ignite = hdh and limit_up

    ma90 = calc_ma(close, 90)
    ma100 = calc_ma(close, 100)
    ma120 = calc_ma(close, 120)
    ma250 = calc_ma(close, 250)
    rps50 = stock.get("rps50", 0)
    rps120 = stock.get("rps120", 0)
    rps250 = stock.get("rps250", 0)

    nh = high >= high.rolling(50).max()
    b_value = int(nh.tail(30).sum())
    above_ma250_days = int((close.tail(30) > ma250.tail(30)).sum())
    near_high_120 = bool(high.iloc[-1] / high.tail(120).max() > 0.9)
    turnaround_signal = bool(
        close.iloc[-1] > ma250.iloc[-1]
        and b_value > 0
        and rps50 > 87
        and 2 < above_ma250_days < 30
        and near_high_120
    )

    kd1 = any(value > 90 for value in (rps50, rps120, rps250))
    kd2 = bool(close.iloc[-1] >= prev_close.iloc[-1] * 1.05)
    high_250 = high.tail(250).max()
    low_15 = low.tail(15).min()
    low_50 = low.tail(50).min()
    low_100 = low.tail(100).min()
    fkd250 = bool(high.iloc[-1] >= high_250)
    fkd41 = fkd250 or low_15 > low_50
    fkd42 = low_15 == low_50 and low_15 > low_100 and high.iloc[-1] / high_250 > 0.88
    fkd43 = (
        low_15 == low_50
        and low_15 > low_100
        and high.iloc[-1] / high_250 > 0.75
        and high.iloc[-1] >= high.tail(40).max()
        and close.iloc[-1] / prev_close.iloc[-1] > 1.07
    )
    kd4 = fkd41 or fkd42 or fkd43

    fkd51 = bool(close.iloc[-1] > ma90.iloc[-1] and ma90.iloc[-1] >= ma90.iloc[-6] and high.iloc[-1] >= high.tail(90).max())
    fkd52 = bool(
        close.iloc[-1] > ma100.iloc[-1]
        and ma100.iloc[-1] >= ma100.iloc[-6]
        and high.iloc[-1] >= high.tail(100).max()
        and ma90.iloc[-1] >= ma90.iloc[-6]
    )
    fkd53 = bool(close.iloc[-1] > ma120.iloc[-1] and ma120.iloc[-1] >= ma120.iloc[-3])
    kd5 = fkd51 or fkd52 or fkd53

    recent_high_window = high.tail(120).reset_index(drop=True)
    recent_low_window = low.tail(120).reset_index(drop=True)
    highest_idx = int(recent_high_window.idxmax())
    h120 = float(recent_high_window.max())
    low_from_high = float(recent_low_window.iloc[highest_idx:].min())
    fkd61 = bool(low.tail(40).min() / h120 > 0.5)
    kd6 = bool(low_from_high / h120 > 0.54 and (fkd61 or fkd250))

    amount_ma10 = calc_ma(amount, 10)
    kd20 = bool(
        amount.iloc[-1] >= amount.tail(10).max()
        or close.iloc[-1] >= prev_close.iloc[-1] * 1.099
        or amount.iloc[-1] >= amount_ma10.iloc[-1] * 2
    )
    breakout_signal = bool(kd1 and kd2 and kd4 and kd5 and kd6 and kd20)

    recent_gain_days = [index for index in range(len(close) - 1, 0, -1) if close.iloc[index] / close.iloc[index - 1] >= 1.05]
    stop_profit_line = None
    if recent_gain_days:
        last_gain_index = recent_gain_days[0]
        distance = len(close) - 1 - last_gain_index
        if distance <= 10:
            stop_profit_line = round((close.iloc[last_gain_index] + open_.iloc[last_gain_index]) / 2, 2)

    strategy_tags = []
    if breakout_signal:
        strategy_tags.append("趋势突破")
    if turnaround_signal:
        strategy_tags.append("翻身计划")
    if big_order_alert:
        strategy_tags.append("资金预警")
    if macd_ignite:
        strategy_tags.append("MACD点火")

    return {
        "ddx": round(float(ddx.iloc[-1]), 4),
        "ddx_positive_days_5": ddx_positive_days_5,
        "big_order_alert": big_order_alert,
        "macd_ignite": macd_ignite,
        "turnaround_signal": turnaround_signal,
        "breakout_signal": breakout_signal,
        "stop_profit_line": stop_profit_line,
        "strategy_tags": strategy_tags,
        "strategy_summary": "、".join(strategy_tags) if strategy_tags else "无",
    }


def apply_sector_rps_metrics(candidates, config):
    if not candidates:
        return candidates

    periods = config["rps_periods"]
    threshold = config["rps_threshold"]
    for period in periods:
        change_key = f"change_{period}d"
        ranked = sorted(candidates, key=lambda item: item.get(change_key, float("-inf")))
        total = len(ranked)
        for rank, item in enumerate(ranked, 1):
            item[f"rps{period}"] = round(rank / total * 100, 2)

    for item in candidates:
        rps10 = item.get("rps10", 0)
        rps20 = item.get("rps20", 0)
        rps50 = item.get("rps50", 0)
        rps120 = item.get("rps120", 0)
        rps250 = item.get("rps250", 0)

        ar1 = rps10 > threshold
        br1 = rps20 > threshold
        ir1 = rps50 > threshold
        j1 = rps120 > threshold
        dr1 = rps250 > threshold

        three_cycle_red = ar1 and (br1 or ir1) and (dr1 or j1)
        four_cycle_red = ar1 and sum((br1, ir1, j1, dr1)) >= 3
        strong_periods = sum((ar1, br1, ir1, j1, dr1))

        item["three_cycle_red"] = three_cycle_red
        item["four_cycle_red"] = four_cycle_red
        item["rps_strong_periods"] = strong_periods
        item["rps_summary"] = (
            f"RPS10/20/50/120/250="
            f"{item.get('rps10', 0):.1f}/{item.get('rps20', 0):.1f}/{item.get('rps50', 0):.1f}/"
            f"{item.get('rps120', 0):.1f}/{item.get('rps250', 0):.1f}"
        )
        if four_cycle_red:
            item["rps_summary"] += " | 四周期红"
        elif three_cycle_red:
            item["rps_summary"] += " | 三周期红"
    return candidates


def screen_stocks_in_sector(sector, config, analysis_date, use_cache=True, cache_only=False):
    members = get_sector_members(
        sector["name"],
        sector["type"],
        use_cache=use_cache,
        cache_only=cache_only,
        snapshot_date=analysis_date,
    )
    if not members:
        return []

    history_days = max(
        80,
        config["ema_long"] + max(config["analysis_days"]) + 30,
        max(config["rps_periods"]) + 30,
    )
    ranked_members = rank_sector_members(members)
    valid_members = [
        item
        for item in ranked_members
        if "ST" not in item["name"] and item["price"] > 0 and item["pe"] > -100
    ][: config["sector_member_limit"]]
    leader_codes = build_sector_leaders(valid_members)
    candidates = []
    weights = config["score_weights"]["stock"]
    min_stock_score = config["min_stock_score"]

    for stock in valid_members:
        df_hist = get_stock_history(
            stock["code"],
            end_date=analysis_date,
            days=history_days,
            use_cache=use_cache,
            cache_only=cache_only,
        )
        if df_hist.empty:
            continue
        tech = analyze_stock_tech(df_hist, config)
        if not tech:
            continue

        is_pullback, conditions = check_pullback_signal(tech, config)
        matched, failed = build_condition_summary(conditions)
        signal = get_signal_type(tech, is_pullback)
        risk_notes, risk_penalty, blocked = build_risk_assessment(stock, tech, config)
        if blocked and config["risk_filters"]["strict_mode"]:
            continue

        score = 0
        if tech["ema_bullish"]:
            score += weights["ema_bullish"]
        if tech["ema13_rising"]:
            score += weights["ema_rising"]
        if is_pullback:
            score += weights["pullback"]
        if tech["ma_bullish"]:
            score += weights["ma_bullish"]
        if tech["vol_ratio"] > 1.2 and tech["change_5d"] > 0:
            score += weights["volume_confirm"]
        if risk_notes == ["正常"]:
            score += weights["low_risk"]
        is_sector_leader = stock["code"] in leader_codes
        if is_sector_leader:
            score += weights.get("sector_leader", 0)
        score -= risk_penalty
        candidates.append(
            {
                "sector": sector["name"],
                "code": stock["code"],
                "name": stock["name"],
                "price": round(tech["price"], 2),
                "change_pct": round(calc_latest_change_pct(df_hist), 2),
                "ema13": round(tech["ema13"], 2),
                "ema55": round(tech["ema55"], 2),
                "ema13_dist_pct": round(tech["ema13_dist_pct"] * 100, 2),
                "signal": signal,
                "is_pullback": is_pullback,
                "score": round(score, 2),
                "pe": stock["pe"],
                "vol_ratio": round(tech["vol_ratio"], 2),
                "change_5d": round(tech["change_5d"], 2),
                "change_10d": round(tech["change_10d"], 2),
                "change_50d": round(tech.get("change_50d", 0), 2),
                "change_120d": round(tech.get("change_120d", 0), 2),
                "change_250d": round(tech.get("change_250d", 0), 2),
                "volatility_10d": round(tech["volatility_10d"], 2),
                "risk": "；".join(risk_notes),
                "signal_grade": "C",
                "is_sector_leader": is_sector_leader,
                "amount": round(to_float(stock.get("amount")), 2),
                "turnover": round(to_float(stock.get("turnover")), 2),
                "hot_topic": "",
                "matched_conditions": matched,
                "failed_conditions": failed,
                "risk_notes": risk_notes,
                "base_score": round(score, 2),
                "_history": df_hist,
                "summary": "",
            }
        )

    apply_sector_rps_metrics(candidates, config)
    for item in candidates:
        score = item["base_score"]
        strategy_state = build_daily_strategy_state(item["_history"], item)
        item.update(strategy_state)
        if item.get("four_cycle_red"):
            score += weights.get("rps_four_cycle_red", 0)
        elif item.get("three_cycle_red"):
            score += weights.get("rps_three_cycle_red", 0)
        if item.get("big_order_alert"):
            score += weights.get("big_order_alert", 0)
        if item.get("macd_ignite"):
            score += weights.get("macd_ignite", 0)
        if item.get("turnaround_signal"):
            score += weights.get("turnaround", 0)
        if item.get("breakout_signal"):
            score += weights.get("breakout", 0)
            item["signal"] = "趋势突破"
        elif item.get("turnaround_signal") and item["signal"] != "回踩买点":
            item["signal"] = "翻身计划"
        elif item.get("macd_ignite") and item["signal"] == "观察":
            item["signal"] = "点火预备"
        item["score"] = round(score, 2)
        item["signal_grade"] = get_signal_grade(score, item["is_pullback"], item["risk_notes"], item["is_sector_leader"])
        item["summary"] = (
            f"满足: {'、'.join(item['matched_conditions']) if item['matched_conditions'] else '无'}; "
            f"未满足: {'、'.join(item['failed_conditions']) if item['failed_conditions'] else '无'}; "
            f"风险: {'、'.join(item['risk_notes'])}; "
            f"强度: {item['rps_summary']}; "
            f"策略: {item['strategy_summary']}"
        )

    candidates = [item for item in candidates if item["score"] >= min_stock_score]
    candidates.sort(
        key=lambda item: (
            item["score"],
            item.get("four_cycle_red", False),
            item.get("three_cycle_red", False),
            item["is_pullback"],
            item["is_sector_leader"],
            item["amount"],
            item["turnover"],
        ),
        reverse=True,
    )
    pullbacks = [item for item in candidates if item["is_pullback"]]
    others = [item for item in candidates if not item["is_pullback"]]
    result = pullbacks[: config["stocks_per_sector"]]
    remaining = config["stocks_per_sector"] - len(result)
    if remaining > 0:
        result.extend(others[:remaining])
    for item in result:
        item.pop("risk_notes", None)
        item.pop("base_score", None)
        item.pop("_history", None)
    return result


def enrich_stocks_with_concepts(stocks, concept_member_map, config):
    for stock in stocks:
        hot_concepts = [
            f"{item['name']}(+{item['change_pct']:.1f}%)"
            for item in concept_member_map.get(stock["code"], [])[: config["concept_match_limit"]]
        ]
        stock["hot_topic"] = f"所属热点概念：{'、'.join(hot_concepts)}" if hot_concepts else ""
    return stocks


def evaluate_saved_picks(as_of_date, config, use_cache=True, cache_only=False):
    if not DB_PATH.exists():
        return None

    lookback_days = config["backtest"]["lookback_days"]
    holding_days = config["backtest"]["holding_days"]
    min_samples = config["backtest"]["min_samples"]
    start_date = (datetime.strptime(as_of_date, "%Y-%m-%d") - timedelta(days=lookback_days)).strftime("%Y-%m-%d")

    conn = sqlite3.connect(str(DB_PATH))
    query = """
    SELECT date, stock_code, stock_name, price, signal
    FROM daily_stock_picks
    WHERE date >= ? AND date <= ?
    ORDER BY date ASC
    """
    picks = pd.read_sql_query(query, conn, params=(start_date, as_of_date))
    conn.close()
    if picks.empty:
        return None

    results = {day: [] for day in holding_days}
    signal_results = defaultdict(lambda: {day: [] for day in holding_days})
    benchmark_defs = {"沪深300": "sh.000300", "中证1000": "sh.000852"}
    benchmark_results = {name: {day: [] for day in holding_days} for name in benchmark_defs}
    benchmark_histories = {
        name: get_benchmark_history(
            name,
            code,
            end_date=as_of_date,
            days=lookback_days + max(holding_days) + 30,
            use_cache=use_cache,
            cache_only=cache_only,
        )
        for name, code in benchmark_defs.items()
    }

    for row in picks.itertuples(index=False):
        signal_name = row.signal or "未分类"
        history = get_stock_history(
            row.stock_code,
            end_date=as_of_date,
            days=lookback_days + max(holding_days) + 30,
            use_cache=use_cache,
            cache_only=cache_only,
        )
        if history.empty or "date" not in history.columns:
            continue
        history = history.reset_index(drop=True)
        entry_index, entry_price = resolve_signal_entry(history, row.date)
        if entry_index is None:
            continue
        for day in holding_days:
            target_index = entry_index + day
            if target_index >= len(history):
                continue
            exit_price = float(history.iloc[target_index]["close"])
            forward_return = (exit_price - entry_price) / entry_price * 100
            results[day].append(forward_return)
            signal_results[signal_name][day].append(forward_return)

        for benchmark_name, benchmark_history in benchmark_histories.items():
            if benchmark_history.empty or "date" not in benchmark_history.columns:
                continue
            benchmark_history = benchmark_history.reset_index(drop=True)
            try:
                benchmark_index = benchmark_history.index[benchmark_history["date"] == row.date][0]
            except IndexError:
                continue
            benchmark_entry = float(benchmark_history.iloc[benchmark_index]["close"])
            if benchmark_entry <= 0:
                continue
            for day in holding_days:
                target_index = benchmark_index + day
                if target_index >= len(benchmark_history):
                    continue
                benchmark_exit = float(benchmark_history.iloc[target_index]["close"])
                benchmark_return = (benchmark_exit - benchmark_entry) / benchmark_entry * 100
                benchmark_results[benchmark_name][day].append(benchmark_return)

    summary = {
        "sample_days": lookback_days,
        "entry_price_basis": config["backtest"]["entry_price_basis"],
        "metrics": {},
    }
    for day, returns in results.items():
        metric = summarize_returns(returns)
        if metric:
            summary["metrics"][day] = metric
    summary["benchmarks"] = {}
    for benchmark_name, benchmark_days in benchmark_results.items():
        metrics = {}
        for day, returns in benchmark_days.items():
            metric = summarize_returns(returns)
            if not metric:
                continue
            metrics[day] = {
                "samples": metric["samples"],
                "avg_return": metric["avg_return"],
            }
            if day in summary["metrics"]:
                metrics[day]["excess_return"] = round(summary["metrics"][day]["avg_return"] - metric["avg_return"], 2)
        if metrics:
            summary["benchmarks"][benchmark_name] = metrics
    summary["by_signal"] = {}
    for signal_name, signal_days in signal_results.items():
        metrics = {}
        for day, returns in signal_days.items():
            metric = summarize_returns(returns)
            if metric:
                metrics[day] = metric
        if metrics:
            summary["by_signal"][signal_name] = metrics
    if not summary["metrics"]:
        return None
    summary["enough_samples"] = all(metric["samples"] >= min_samples for metric in summary["metrics"].values())
    return summary


def normalize_sector_filters(values):
    sectors = []
    for item in values or []:
        for name in str(item).split(","):
            normalized = name.strip()
            if normalized:
                sectors.append(normalized)
    return sorted(set(sectors))


def filter_classified_sectors(classified, sector_filters):
    if not sector_filters:
        return classified
    allowed = set(sector_filters)
    return {
        "hot": [item for item in classified["hot"] if item["name"] in allowed],
        "potential": [item for item in classified["potential"] if item["name"] in allowed],
        "hot_concepts": classified.get("hot_concepts", []),
    }


def apply_pick_filters(picks, args, config):
    filtered = list(picks)
    sector_filters = normalize_sector_filters(args.sector)
    if sector_filters:
        allowed = set(sector_filters)
        filtered = [item for item in filtered if item["sector"] in allowed]
    if args.only_pullback:
        filtered = [item for item in filtered if item.get("is_pullback")]
    if args.min_score is not None:
        filtered = [item for item in filtered if item.get("score", 0) >= args.min_score]
    if args.exclude_high_pe:
        threshold = config["risk_filters"]["high_pe_warning"]
        filtered = [item for item in filtered if item.get("pe", 0) <= threshold]
    return filtered


def build_day_over_day_comparison(date_str, classified, picks, sector_filters=None):
    previous_date = get_previous_analysis_date(date_str)
    if not previous_date:
        return {
            "previous_date": None,
            "new_hot": [],
            "dropped_hot": [],
            "new_potential": [],
            "dropped_potential": [],
            "new_pullbacks": [],
            "removed_pullbacks": [],
        }

    previous = load_saved_analysis(previous_date) or {"classified": {"hot": [], "potential": []}, "stock_picks": []}
    if sector_filters:
        previous["classified"] = filter_classified_sectors(previous["classified"], sector_filters)
        allowed = set(sector_filters)
        previous["stock_picks"] = [item for item in previous["stock_picks"] if item["sector"] in allowed]
    current_hot = {item["name"] for item in classified["hot"]}
    previous_hot = {item["name"] for item in previous["classified"]["hot"]}
    current_potential = {item["name"] for item in classified["potential"]}
    previous_potential = {item["name"] for item in previous["classified"]["potential"]}

    current_pullbacks = {
        item["code"]: f"{item['name']}({item['sector']})"
        for item in picks
        if item.get("is_pullback")
    }
    previous_pullbacks = {
        item["code"]: f"{item['name']}({item['sector']})"
        for item in previous["stock_picks"]
        if item.get("signal") == "回踩买点"
    }

    return {
        "previous_date": previous_date,
        "new_hot": sorted(current_hot - previous_hot),
        "dropped_hot": sorted(previous_hot - current_hot),
        "new_potential": sorted(current_potential - previous_potential),
        "dropped_potential": sorted(previous_potential - current_potential),
        "new_pullbacks": [current_pullbacks[code] for code in sorted(current_pullbacks.keys() - previous_pullbacks.keys())],
        "removed_pullbacks": [previous_pullbacks[code] for code in sorted(previous_pullbacks.keys() - current_pullbacks.keys())],
    }


def build_alerts(classified, picks, comparison, config):
    alerts = []
    grade_a = [item for item in picks if item.get("signal_grade") == "A"]
    high_pe = [item for item in picks if item.get("pe", 0) > config["risk_filters"]["high_pe_warning"]]

    if comparison["new_hot"]:
        alerts.append({"level": "info", "title": "新晋热门板块", "detail": "、".join(comparison["new_hot"])})
    if comparison["dropped_hot"]:
        alerts.append({"level": "warn", "title": "掉出热门板块", "detail": "、".join(comparison["dropped_hot"])})
    if comparison["new_pullbacks"]:
        alerts.append({"level": "signal", "title": "新增回踩买点", "detail": "、".join(comparison["new_pullbacks"])})
    if grade_a:
        alerts.append(
            {
                "level": "signal",
                "title": "A 级信号",
                "detail": "、".join(f"{item['name']}({item['sector']})" for item in grade_a[:5]),
            }
        )
    if high_pe:
        alerts.append(
            {
                "level": "warn",
                "title": "高估值暴露",
                "detail": "、".join(f"{item['name']}({item['pe']:.0f})" for item in high_pe[:5]),
            }
        )
    if not alerts:
        alerts.append({"level": "info", "title": "暂无新增告警", "detail": "今日未触发新的重点变化。"})
    return alerts


def format_source_lines():
    lines = []
    for name, meta in sorted(RUNTIME_META["sources"].items()):
        source = meta["source"]
        records = meta.get("records")
        lines.append(f"- {name}: {source}, records={records if records is not None else '-'}")
    for name, stats in sorted(RUNTIME_META["cache_stats"].items()):
        if stats["live"] or stats["cache"]:
            lines.append(f"- {name}: live={stats['live']}, cache={stats['cache']}")
    for item in RUNTIME_META["notes"]:
        lines.append(f"- note: {item}")
    return lines


def build_payload(date_str, classified, picks, streaks, backtest, comparison, alerts, filters, config):
    return {
        "analysis_date": date_str,
        "metadata": {
            "sources": RUNTIME_META["sources"],
            "cache_stats": dict(RUNTIME_META["cache_stats"]),
            "notes": RUNTIME_META["notes"],
        },
        "config": config,
        "filters": filters,
        "classified": classified,
        "stock_picks": picks,
        "streaks": streaks,
        "comparison": comparison,
        "alerts": alerts,
        "backtest": backtest,
    }


def generate_alert_report(date_str, alerts, comparison):
    lines = [f"告警摘要 | {date_str}", "=" * 40, ""]
    if comparison.get("previous_date"):
        lines.append(f"对比基准: {comparison['previous_date']}")
        lines.append("")
    for item in alerts:
        lines.append(f"- [{item['level']}] {item['title']}: {item['detail']}")
    return "\n".join(lines)


def generate_report(date_str, classified, picks, streaks, backtest, comparison, alerts, config, alerts_only=False):
    if alerts_only:
        return generate_alert_report(date_str, alerts, comparison)

    hot_count = len(classified["hot"])
    potential_count = len(classified["potential"])
    lines = [f"板块跟踪日报 | {date_str}", "=" * 40, "", "数据状态", "-" * 40]
    lines.extend(format_source_lines() or ["- 无"])
    lines.append("")

    lines.extend(["告警摘要", "-" * 40])
    for item in alerts:
        lines.append(f"- [{item['level']}] {item['title']}: {item['detail']}")
    lines.append("")

    lines.extend(["和上次结果相比", "-" * 40])
    if comparison.get("previous_date"):
        lines.append(f"- 对比日期: {comparison['previous_date']}")
        lines.append(f"- 新晋热门板块: {'、'.join(comparison['new_hot']) if comparison['new_hot'] else '无'}")
        lines.append(f"- 掉出热门板块: {'、'.join(comparison['dropped_hot']) if comparison['dropped_hot'] else '无'}")
        lines.append(f"- 新晋潜力板块: {'、'.join(comparison['new_potential']) if comparison['new_potential'] else '无'}")
        lines.append(f"- 新增回踩买点: {'、'.join(comparison['new_pullbacks']) if comparison['new_pullbacks'] else '无'}")
    else:
        lines.append("- 暂无可对比的上一日结果")
    lines.append("")

    pullbacks = [item for item in picks if item.get("is_pullback")]
    lines.extend([f"重点推荐（回踩 EMA{config['ema_short']}）", "-" * 40])
    if pullbacks:
        for stock in pullbacks:
            leader_tag = " | 板块核心票" if stock.get("is_sector_leader") else ""
            lines.append(
                f"- {stock['name']} {stock['code']} [{stock['sector']}] | {stock['signal']} | 等级 {stock.get('signal_grade', '-')}{leader_tag}"
            )
            lines.append(f"  价格 {stock['price']:.2f} | EMA差 {stock['ema13_dist_pct']:+.1f}% | 量比 {stock['vol_ratio']}")
            lines.append(f"  解释 {stock['summary']}")
            if stock.get("rps_summary"):
                lines.append(f"  RPS {stock['rps_summary']}")
            if stock.get("strategy_summary") and stock["strategy_summary"] != "无":
                lines.append(f"  策略 {stock['strategy_summary']}")
            if stock.get("stop_profit_line") is not None:
                lines.append(f"  参考止盈线 {stock['stop_profit_line']:.2f}")
            if stock["hot_topic"]:
                lines.append(f"  热点 {stock['hot_topic']}")
            lines.append("")
    else:
        lines.extend(["- 今日暂无回踩买点个股", ""])

    lines.extend([f"热门板块 TOP{hot_count}", "-" * 40])
    for index, sector in enumerate(classified["hot"], 1):
        flow = f"{sector.get('fund_flow', 0) / 1e8:.1f}亿" if sector.get("fund_flow", 0) > 0 else "-"
        lines.append(f"{index}. {sector['name']} | 评分 {sector['score']} | 涨幅 {sector['change_pct']:+.2f}% | 资金 {flow}")
        for stock in [item for item in picks if item["sector"] == sector["name"]]:
            leader_tag = " | 核心票" if stock.get("is_sector_leader") else ""
            lines.append(
                f"   - {stock['name']} {stock['code']} | {stock['signal']} | 等级 {stock.get('signal_grade', '-')}{leader_tag} | {stock['summary']}"
            )
        lines.append("")

    lines.extend([f"潜力板块 TOP{potential_count}", "-" * 40])
    for index, sector in enumerate(classified["potential"], 1):
        boost = " | 概念加成" if sector.get("concept_boost") else ""
        flow = f"{sector.get('fund_flow', 0) / 1e8:.1f}亿" if sector.get("fund_flow", 0) > 0 else "-"
        lines.append(f"{index}. {sector['name']} | 评分 {sector['score']}{boost} | 涨幅 {sector['change_pct']:+.2f}% | 资金 {flow}")
        if sector.get("related_concepts"):
            lines.append(f"   - 关联概念: {'、'.join(sector['related_concepts'])}")
        for stock in [item for item in picks if item["sector"] == sector["name"]]:
            leader_tag = " | 核心票" if stock.get("is_sector_leader") else ""
            lines.append(
                f"   - {stock['name']} {stock['code']} | {stock['signal']} | 等级 {stock.get('signal_grade', '-')}{leader_tag} | {stock['summary']}"
            )
        lines.append("")

    lines.extend(["板块轮动观察", "-" * 40])
    if streaks:
        for item in streaks[:5]:
            lines.append(f"- {item['name']} 连续上榜 {item['days']} 天 | 当前分类 {item['current_category']} | 历史分类 {item['categories']}")
    else:
        lines.append("- 暂无足够历史数据")
    lines.append("")

    lines.extend(["回测摘要", "-" * 40])
    if backtest and backtest.get("metrics"):
        lines.append(f"- 评估窗口: 最近 {backtest['sample_days']} 天")
        for day, metric in sorted(backtest["metrics"].items()):
            lines.append(
                f"- 持有 {day} 天 | 样本 {metric['samples']} | 平均收益 {metric['avg_return']:+.2f}% | 胜率 {metric['win_rate']:.2f}%"
            )
        for signal_name, metrics in sorted(backtest.get("by_signal", {}).items()):
            for day, metric in sorted(metrics.items()):
                lines.append(
                    f"- 信号 {signal_name} | 持有 {day} 天 | 样本 {metric['samples']} | 平均收益 {metric['avg_return']:+.2f}% | 胜率 {metric['win_rate']:.2f}%"
                )
        for benchmark_name, metrics in sorted(backtest.get("benchmarks", {}).items()):
            for day, metric in sorted(metrics.items()):
                lines.append(
                    f"- 基准 {benchmark_name} 持有 {day} 天 | 平均收益 {metric['avg_return']:+.2f}% | 超额 {metric.get('excess_return', 0):+.2f}%"
                )
    else:
        lines.append("- 暂无可用回测样本")
    lines.append("")

    high_pe = [item for item in picks if item.get("pe", 0) > config["risk_filters"]["high_pe_warning"]]
    lines.extend(["风险提示", "-" * 40])
    if high_pe:
        lines.append("- 高估值个股: " + "、".join(f"{item['name']}({item['pe']:.0f})" for item in high_pe))
    lines.append("- 以上分析基于公开行情和技术指标，不构成投资建议。")
    return "\n".join(lines)


def write_json_report(path, payload):
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2, default=json_default)


def print_network_diagnostics():
    diagnostics = build_network_diagnostics()
    print(json.dumps(diagnostics, ensure_ascii=False, indent=2))


def run_analysis(args):
    reset_runtime_meta()
    ensure_dirs()

    if args.diagnose_network:
        print_network_diagnostics()
        return {"diagnostics": build_network_diagnostics()}

    ensure_dependencies()
    config = load_config()
    cache_only = bool(args.cache_only or args.offline)
    sector_filters = normalize_sector_filters(args.sector)
    if cache_only and args.no_cache:
        note("cache-only/offline 模式已启用，忽略 --no-cache。")
    elif args.no_cache:
        config["use_cache"] = False

    if cache_only:
        config["use_cache"] = True
        config["_cache_only"] = True
        note("当前运行使用离线缓存模式，不会发起网络请求。")

    if args.sector_limit:
        config["_sector_limit"] = max(1, args.sector_limit)

    analysis_date = args.date or datetime.now().strftime("%Y-%m-%d")
    today = datetime.now().strftime("%Y-%m-%d")
    RUNTIME_META["analysis_date"] = analysis_date

    if sector_filters:
        note(f"结果已按板块过滤: {', '.join(sector_filters)}")
    if args.only_pullback:
        note("结果仅保留回踩买点。")
    if args.min_score is not None:
        note(f"结果最小评分阈值: {args.min_score}")
    if args.exclude_high_pe:
        note(f"结果排除高估值个股: PE>{config['risk_filters']['high_pe_warning']}")

    init_db()

    proxy_env = get_proxy_environment()
    if proxy_env:
        note(f"检测到代理环境变量: {', '.join(sorted(proxy_env.keys()))}")

    use_cache = config.get("use_cache", True)
    snapshot = load_market_snapshot(analysis_date)
    using_historical_snapshot = bool(snapshot)
    if snapshot:
        industry_sectors = snapshot["industry"]
        concept_sectors = snapshot["concept"]
        fund_flows = snapshot["fund_flow"]
        note(f"已加载 {analysis_date} 的历史市场快照。")
    else:
        if analysis_date != today:
            logger.error("未找到 %s 的历史市场快照", analysis_date)
            note(f"未找到 {analysis_date} 的历史市场快照。")
            return None
        industry_sectors = get_industry_sectors(use_cache=use_cache, cache_only=cache_only)
        concept_sectors = get_concept_sectors(use_cache=use_cache, cache_only=cache_only)
        fund_flows = get_fund_flow_rank(use_cache=use_cache, cache_only=cache_only)
        if not industry_sectors:
            if cache_only:
                logger.error("cache-only mode failed: no industry sector cache available")
            else:
                logger.error("failed to fetch industry sectors")
            return None
        if analysis_date == today and not cache_only:
            save_market_snapshot(analysis_date, industry_sectors, concept_sectors, fund_flows)

    classified = classify_sectors(industry_sectors, concept_sectors, fund_flows, config)
    _, concept_member_map, concept_members = build_hot_concept_member_map(
        concept_sectors,
        config,
        analysis_date,
        use_cache=use_cache,
        cache_only=cache_only,
    )
    classified = apply_sector_concept_boost(
        classified,
        concept_members,
        config,
        analysis_date,
        use_cache=use_cache,
        cache_only=cache_only,
    )
    all_stock_picks = []
    seen_codes = set()

    login_needed = not cache_only and bool(classified["hot"] or classified["potential"] or not args.skip_backtest)

    if login_needed:
        login_result = bs.login()
        if getattr(login_result, "error_code", "0") != "0":
            logger.error("BaoStock login failed: %s", getattr(login_result, "error_msg", "unknown"))
            return None

    backtest = None
    try:
        for sector in classified["hot"] + classified["potential"]:
            logger.info("screen sector: %s", sector["name"])
            picks = screen_stocks_in_sector(
                sector,
                config,
                analysis_date,
                use_cache=use_cache,
                cache_only=cache_only,
            )
            for stock in picks:
                if stock["code"] not in seen_codes:
                    seen_codes.add(stock["code"])
                    all_stock_picks.append(stock)
            if not cache_only:
                time.sleep(0.5)

        enrich_stocks_with_concepts(all_stock_picks, concept_member_map, config)

        if not args.skip_backtest and (login_needed or cache_only):
            backtest = evaluate_saved_picks(
                analysis_date,
                config,
                use_cache=use_cache,
                cache_only=cache_only,
            )
    finally:
        if login_needed:
            bs.logout()

    allow_persist = analysis_date == today or using_historical_snapshot
    if allow_persist:
        save_daily_results(analysis_date, classified, all_stock_picks)
        streaks = get_sector_streak(analysis_date)
    else:
        note("未使用历史快照，本次结果不写入数据库，避免污染历史记录。")
        streaks = []

    visible_classified = filter_classified_sectors(classified, sector_filters)
    visible_picks = apply_pick_filters(all_stock_picks, args, config)
    comparison = build_day_over_day_comparison(analysis_date, visible_classified, visible_picks, sector_filters=sector_filters)
    alerts = build_alerts(visible_classified, visible_picks, comparison, config)
    filters = {
        "sector": sector_filters,
        "only_pullback": args.only_pullback,
        "min_score": args.min_score,
        "exclude_high_pe": args.exclude_high_pe,
        "alerts_only": args.alerts_only,
    }

    report = generate_report(
        analysis_date,
        visible_classified,
        visible_picks,
        streaks,
        backtest,
        comparison,
        alerts,
        config,
        alerts_only=args.alerts_only,
    )
    payload = build_payload(
        analysis_date,
        visible_classified,
        visible_picks,
        streaks,
        backtest,
        comparison,
        alerts,
        filters,
        config,
    )

    report_path = REPORT_DIR / f"{analysis_date}.md"
    json_path = Path(args.output_json) if args.output_json else REPORT_DIR / f"{analysis_date}.json"
    with open(report_path, "w", encoding="utf-8") as file:
        file.write(report)
    write_json_report(json_path, payload)

    print("===REPORT_START===")
    print(report)
    print("===REPORT_END===")
    return payload


if __name__ == "__main__":
    run_analysis(parse_args())
