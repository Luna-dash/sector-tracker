#!/usr/bin/env python3
"""A-share sector rotation tracker."""

import argparse
import copy
import hashlib
import json
import logging
import os
import re
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
    "hot_sectors_count": 8,
    "potential_sectors_count": 8,
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
    "runtime_memory_cache": True,
    "write_output_files": True,
    "sector_scan_sleep_seconds": 0.0,
    "sector_member_limit": 20,
    "min_stock_score": 55,
    "history_window_days": 540,
    "track_candidates": True,
    "export_candidates": True,
    "edge_candidates_limit": 10,
    "edge_candidate_score_gap": 5,
    "rps_min_sample": 5,
    "merge_similar_sectors": True,
    "excluded_sector_keywords": [
        "昨日涨停",
        "昨日连板",
        "昨日触板",
        "首板",
        "连板",
        "二板",
        "三板",
        "一字板",
        "涨停板",
        "涨停股池",
        "龙虎榜",
    ],
    "light_breakout_setup": {
        "near_high_window": 120,
        "near_high_pct": 0.92,
        "min_vol_ratio": 1.2,
        "require_positive_5d": True,
        "max_change_20d": 25.0,
        "max_ema55_dist_pct": 0.25,
        "max_recent_drawdown_pct": 0.18,
    },
    "anchored_breakout": {
        "enabled": True,
        "breakout_window": 15,
        "anchor_reference_window": 0,
        "anchor_min_gap": 16,
        "pivot_left": 3,
        "pivot_right": 3,
        "anchor_zone_pct": 0.03,
        "absolute_high_near_pct": 0.02,
        "active_touch_days": 90,
        "min_pullback_pct": 0.06,
        "min_base_days": 8,
        "close_break_buffer_pct": 0.003,
        "base_below_buffer_pct": 0.005,
        "volume_confirm_ratio": 1.2,
        "breakout_amount_ratio20_min": 1.5,
        "breakout_amount_ratio20_max": 4.0,
        "breakout_close_position_min": 0.65,
        "pullback_window": 7,
        "pullback_amount_vs_breakout_max": 0.65,
        "pullback_amount_ratio20_max": 1.0,
        "bad_down_amount_ratio20_min": 1.5,
        "bad_down_change_pct": -3.0,
        "confirm_window": 5,
        "confirm_price_volume_up_days": 2,
        "confirm_amount_ratio20_min": 1.0,
        "confirm_amount_ratio20_max": 2.5,
        "best_breakout_day": 11,
        "min_breakout_score": 10,
        "volume_price_weight": 10,
    },
    "concept_scan_limit": 20,
    "concept_match_limit": 5,
    "concept_overlap_min_count": 2,
    "concept_overlap_min_ratio": 0.1,
    "breakout_pool": {
        "enabled": True,
        "min_size": 30,
        "max_size": 100,
        "seed_breakout_days": 5,
        "track_limit_per_run": 100,
        "pick_limit": 10,
        "full_market_scan_limit": 600,
        "sector_discovery_enabled": True,
        "sector_discovery_limit_per_run": 200,
        "break_below_breakout_pct": 0.20,
        "ema55_break_days": 2,
        "max_watch_days": 20,
        "min_amount": 0,
    },
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
            "anchored_breakout": 10,
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
        "hard_filters": {
            "exclude_negative_pe": False,
        },
        "soft_penalties": {
            "high_pe": 6,
            "negative_pe": 0,
            "change_5d": 8,
            "vol_ratio": 5,
            "volatility_10d": 5,
        },
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
    "runtime_memory_cache_enabled": True,
}
PRIOR_HIGH_ZONE_CACHE = {}
RUNTIME_CACHE = {}


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
    RUNTIME_META["runtime_memory_cache_enabled"] = True
    RUNTIME_CACHE.clear()
    PRIOR_HIGH_ZONE_CACHE.clear()


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


def validate_config(config):
    stock_weights = config.get("score_weights", {}).get("stock", {})
    negative_weights = [key for key, value in stock_weights.items() if to_float(value) < 0]
    if negative_weights:
        raise ValueError(f"stock score weights must be non-negative: {', '.join(negative_weights)}")
    if config["history_window_days"] < get_light_history_days(config):
        raise ValueError("history_window_days must cover the light screening window")
    return config


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
    config["runtime_memory_cache"] = bool(config.get("runtime_memory_cache", True))
    config["write_output_files"] = bool(config.get("write_output_files", True))
    config["sector_scan_sleep_seconds"] = min(5.0, max(0.0, float(config.get("sector_scan_sleep_seconds", 0.0))))
    config["sector_member_limit"] = max(5, int(config.get("sector_member_limit", 20)))
    config["min_stock_score"] = max(0, float(config.get("min_stock_score", 55)))
    config["history_window_days"] = max(260, int(config.get("history_window_days", 540)))
    config["track_candidates"] = bool(config.get("track_candidates", True))
    config["export_candidates"] = bool(config.get("export_candidates", True))
    config["edge_candidates_limit"] = max(0, int(config.get("edge_candidates_limit", 10)))
    config["edge_candidate_score_gap"] = max(0.0, float(config.get("edge_candidate_score_gap", 5)))
    config["rps_min_sample"] = max(1, int(config.get("rps_min_sample", 5)))
    config["merge_similar_sectors"] = bool(config.get("merge_similar_sectors", True))
    excluded_keywords = config.get("excluded_sector_keywords", DEFAULT_CONFIG["excluded_sector_keywords"])
    if not isinstance(excluded_keywords, list):
        excluded_keywords = DEFAULT_CONFIG["excluded_sector_keywords"]
    config["excluded_sector_keywords"] = [str(item).strip() for item in excluded_keywords if str(item).strip()]
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

    light_breakout_cfg = deep_merge(DEFAULT_CONFIG.get("light_breakout_setup", {}), config.get("light_breakout_setup", {}))
    light_breakout_cfg["near_high_window"] = max(20, int(light_breakout_cfg.get("near_high_window", 120)))
    light_breakout_cfg["near_high_pct"] = min(
        1.0,
        max(0.5, float(light_breakout_cfg.get("near_high_pct", 0.92))),
    )
    light_breakout_cfg["min_vol_ratio"] = max(0.5, float(light_breakout_cfg.get("min_vol_ratio", 1.2)))
    light_breakout_cfg["require_positive_5d"] = bool(light_breakout_cfg.get("require_positive_5d", True))
    light_breakout_cfg["max_change_20d"] = max(0.0, float(light_breakout_cfg.get("max_change_20d", 25.0)))
    light_breakout_cfg["max_ema55_dist_pct"] = max(0.0, float(light_breakout_cfg.get("max_ema55_dist_pct", 0.25)))
    light_breakout_cfg["max_recent_drawdown_pct"] = max(0.0, float(light_breakout_cfg.get("max_recent_drawdown_pct", 0.18)))
    config["light_breakout_setup"] = light_breakout_cfg

    breakout_cfg = deep_merge(DEFAULT_CONFIG.get("anchored_breakout", {}), config.get("anchored_breakout", {}))
    breakout_cfg["enabled"] = bool(breakout_cfg.get("enabled", True))
    breakout_cfg["breakout_window"] = max(5, int(breakout_cfg.get("breakout_window", 15)))
    breakout_cfg["anchor_reference_window"] = max(0, int(breakout_cfg.get("anchor_reference_window", 0)))
    breakout_cfg["pivot_left"] = max(1, int(breakout_cfg.get("pivot_left", 3)))
    breakout_cfg["pivot_right"] = max(1, int(breakout_cfg.get("pivot_right", 3)))
    breakout_cfg["anchor_zone_pct"] = min(0.08, max(0.005, float(breakout_cfg.get("anchor_zone_pct", 0.03))))
    breakout_cfg["absolute_high_near_pct"] = min(
        0.08,
        max(0.005, float(breakout_cfg.get("absolute_high_near_pct", 0.02))),
    )
    breakout_cfg["active_touch_days"] = max(20, int(breakout_cfg.get("active_touch_days", 90)))
    breakout_cfg["anchor_min_gap"] = max(
        breakout_cfg["pivot_right"],
        int(breakout_cfg.get("anchor_min_gap", breakout_cfg["breakout_window"] + 1)),
    )
    breakout_cfg["min_pullback_pct"] = min(0.3, max(0.01, float(breakout_cfg.get("min_pullback_pct", 0.06))))
    breakout_cfg["min_base_days"] = max(3, int(breakout_cfg.get("min_base_days", 8)))
    breakout_cfg["close_break_buffer_pct"] = min(
        0.03,
        max(0.0, float(breakout_cfg.get("close_break_buffer_pct", 0.003))),
    )
    breakout_cfg["base_below_buffer_pct"] = min(
        0.05,
        max(0.0, float(breakout_cfg.get("base_below_buffer_pct", 0.005))),
    )
    breakout_cfg["volume_confirm_ratio"] = min(
        5.0,
        max(0.8, float(breakout_cfg.get("volume_confirm_ratio", 1.2))),
    )
    breakout_cfg["breakout_amount_ratio20_min"] = min(
        5.0,
        max(0.5, float(breakout_cfg.get("breakout_amount_ratio20_min", 1.5))),
    )
    breakout_cfg["breakout_amount_ratio20_max"] = min(
        10.0,
        max(
            breakout_cfg["breakout_amount_ratio20_min"],
            float(breakout_cfg.get("breakout_amount_ratio20_max", 4.0)),
        ),
    )
    breakout_cfg["breakout_close_position_min"] = min(
        1.0,
        max(0.0, float(breakout_cfg.get("breakout_close_position_min", 0.65))),
    )
    breakout_cfg["pullback_window"] = max(1, int(breakout_cfg.get("pullback_window", 7)))
    breakout_cfg["pullback_amount_vs_breakout_max"] = min(
        1.5,
        max(0.1, float(breakout_cfg.get("pullback_amount_vs_breakout_max", 0.65))),
    )
    breakout_cfg["pullback_amount_ratio20_max"] = min(
        2.0,
        max(0.1, float(breakout_cfg.get("pullback_amount_ratio20_max", 1.0))),
    )
    breakout_cfg["bad_down_amount_ratio20_min"] = min(
        5.0,
        max(0.5, float(breakout_cfg.get("bad_down_amount_ratio20_min", 1.5))),
    )
    breakout_cfg["bad_down_change_pct"] = min(
        0.0,
        float(breakout_cfg.get("bad_down_change_pct", -3.0)),
    )
    breakout_cfg["confirm_window"] = max(1, int(breakout_cfg.get("confirm_window", 5)))
    breakout_cfg["confirm_price_volume_up_days"] = max(
        1,
        int(breakout_cfg.get("confirm_price_volume_up_days", 2)),
    )
    breakout_cfg["confirm_amount_ratio20_min"] = min(
        5.0,
        max(0.1, float(breakout_cfg.get("confirm_amount_ratio20_min", 1.0))),
    )
    breakout_cfg["confirm_amount_ratio20_max"] = min(
        8.0,
        max(
            breakout_cfg["confirm_amount_ratio20_min"],
            float(breakout_cfg.get("confirm_amount_ratio20_max", 2.5)),
        ),
    )
    breakout_cfg["best_breakout_day"] = max(1, int(breakout_cfg.get("best_breakout_day", 11)))
    breakout_cfg["min_breakout_score"] = max(0, float(breakout_cfg.get("min_breakout_score", 10)))
    breakout_cfg["volume_price_weight"] = max(0.0, float(breakout_cfg.get("volume_price_weight", 10)))
    config["anchored_breakout"] = breakout_cfg

    risk_cfg = deep_merge(DEFAULT_CONFIG.get("risk_filters", {}), config.get("risk_filters", {}))
    risk_cfg["high_pe_warning"] = float(risk_cfg.get("high_pe_warning", 100))
    risk_cfg["exclude_negative_pe"] = bool(risk_cfg.get("exclude_negative_pe", False))
    risk_cfg["max_change_5d"] = max(0.0, float(risk_cfg.get("max_change_5d", 12.0)))
    risk_cfg["max_vol_ratio"] = max(0.0, float(risk_cfg.get("max_vol_ratio", 2.5)))
    risk_cfg["max_volatility_10d"] = max(0.0, float(risk_cfg.get("max_volatility_10d", 8.0)))
    risk_cfg["strict_mode"] = bool(risk_cfg.get("strict_mode", False))
    hard_filters = deep_merge(DEFAULT_CONFIG["risk_filters"].get("hard_filters", {}), risk_cfg.get("hard_filters", {}))
    hard_filters["exclude_negative_pe"] = bool(hard_filters.get("exclude_negative_pe", risk_cfg["exclude_negative_pe"]))
    soft_penalties = deep_merge(DEFAULT_CONFIG["risk_filters"].get("soft_penalties", {}), risk_cfg.get("soft_penalties", {}))
    risk_cfg["hard_filters"] = hard_filters
    risk_cfg["soft_penalties"] = {key: max(0.0, float(value)) for key, value in soft_penalties.items()}
    config["risk_filters"] = risk_cfg

    pool_cfg = deep_merge(DEFAULT_CONFIG.get("breakout_pool", {}), config.get("breakout_pool", {}))
    pool_cfg["enabled"] = bool(pool_cfg.get("enabled", True))
    pool_cfg["min_size"] = max(0, int(pool_cfg.get("min_size", 30)))
    pool_cfg["max_size"] = max(1, int(pool_cfg.get("max_size", 100)))
    pool_cfg["min_size"] = min(pool_cfg["min_size"], pool_cfg["max_size"])
    pool_cfg["seed_breakout_days"] = max(1, int(pool_cfg.get("seed_breakout_days", 5)))
    pool_cfg["track_limit_per_run"] = max(1, int(pool_cfg.get("track_limit_per_run", 100)))
    pool_cfg["pick_limit"] = max(0, int(pool_cfg.get("pick_limit", 10)))
    pool_cfg["full_market_scan_limit"] = max(0, int(pool_cfg.get("full_market_scan_limit", 600)))
    pool_cfg["sector_discovery_enabled"] = bool(pool_cfg.get("sector_discovery_enabled", True))
    pool_cfg["sector_discovery_limit_per_run"] = max(0, int(pool_cfg.get("sector_discovery_limit_per_run", 200)))
    pool_cfg["break_below_breakout_pct"] = min(
        0.5,
        max(0.0, float(pool_cfg.get("break_below_breakout_pct", 0.20))),
    )
    pool_cfg["ema55_break_days"] = max(1, int(pool_cfg.get("ema55_break_days", 2)))
    pool_cfg["max_watch_days"] = max(1, int(pool_cfg.get("max_watch_days", 20)))
    pool_cfg["min_amount"] = max(0.0, float(pool_cfg.get("min_amount", 0)))
    config["breakout_pool"] = pool_cfg

    backtest_cfg = config.get("backtest", {})
    holding_days = backtest_cfg.get("holding_days", [1, 3, 5])
    if not isinstance(holding_days, list) or not holding_days:
        holding_days = [1, 3, 5]
    backtest_cfg["holding_days"] = sorted({int(day) for day in holding_days if int(day) > 0})
    backtest_cfg["lookback_days"] = max(1, int(backtest_cfg.get("lookback_days", 30)))
    backtest_cfg["min_samples"] = max(1, int(backtest_cfg.get("min_samples", 5)))
    entry_price_basis = str(backtest_cfg.get("entry_price_basis", "signal_close")).strip().lower()
    if entry_price_basis not in {"signal_close", "next_open"}:
        entry_price_basis = "signal_close"
    backtest_cfg["entry_price_basis"] = entry_price_basis
    config["backtest"] = backtest_cfg
    return validate_config(config)


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
    parser.add_argument("--diagnose-screening", action="store_true", help="only output screening diagnostics")
    parser.add_argument("--output-json", help="custom json output path")
    parser.add_argument("--no-output-files", action="store_true", help="skip writing markdown/json report files")
    parser.add_argument("--skip-backtest", action="store_true", help="skip backtest summary")
    parser.add_argument("--update-breakout-pool", action="store_true", help="actively scan full market and add recent breakout stocks to the pool")
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


def resolve_signal_entry(history, signal_date, entry_price_basis="signal_close"):
    if history.empty or "date" not in history.columns or "close" not in history.columns:
        return None, None
    history = history.reset_index(drop=True)
    try:
        signal_index = history.index[history["date"] == signal_date][0]
    except IndexError:
        return None, None

    if entry_price_basis == "next_open":
        if "open" not in history.columns:
            return None, None
        entry_index = signal_index + 1
        if entry_index >= len(history):
            return None, None
        entry_price = to_float(history.iloc[entry_index]["open"], 0.0)
    else:
        entry_index = signal_index
        entry_price = to_float(history.iloc[entry_index]["close"], 0.0)
    if entry_price <= 0:
        return None, None
    return entry_index, entry_price


def json_default(value):
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.strftime("%Y-%m-%d")
    raise TypeError(f"Unsupported type: {type(value)!r}")


def clone_runtime_value(value):
    if isinstance(value, pd.DataFrame):
        return value.copy()
    return copy.deepcopy(value)


def runtime_cache_get(namespace, key, enabled=True):
    if not enabled or not RUNTIME_META.get("runtime_memory_cache_enabled", True):
        return None
    value = RUNTIME_CACHE.get((namespace, key))
    if value is None:
        return None
    return clone_runtime_value(value)


def runtime_cache_set(namespace, key, value, enabled=True):
    if not enabled or not RUNTIME_META.get("runtime_memory_cache_enabled", True):
        return value
    RUNTIME_CACHE[(namespace, key)] = clone_runtime_value(value)
    return value


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
    last_error = None
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
            last_error = str(exc)
            logger.warning("%s fetch failed (%s/%s): %s", name, attempt, MAX_RETRIES, exc)
            if attempt < MAX_RETRIES:
                time.sleep(sleep_seconds)

    if use_cache and cache_name:
        cached = load_cache(cache_name)
        if cached:
            record_source(name, "cache", records=len(cached.get("data", [])), cache_name=cache_name)
            if last_error:
                note(f"{name} 实时获取失败，已回退缓存；最后错误: {last_error}")
            bump_cache_stat(name, "cache")
            return cached.get("data", [])

    record_source(name, "empty", records=0, cache_name=cache_name)
    if last_error:
        note(f"{name} 实时获取失败且无可用缓存；最后错误: {last_error}")
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
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS breakout_stock_pool (
            stock_code TEXT PRIMARY KEY,
            stock_name TEXT NOT NULL,
            sector_name TEXT,
            source TEXT,
            status TEXT NOT NULL,
            first_breakout_date TEXT,
            last_seen_date TEXT,
            breakout_price REAL,
            current_price REAL,
            ema55 REAL,
            score REAL,
            signal TEXT,
            remove_reason TEXT,
            metadata TEXT,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_sector_ranking_date ON daily_sector_ranking(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_picks_date ON daily_stock_picks(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_market_snapshots_date ON market_snapshots(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_breakout_pool_status ON breakout_stock_pool(status)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_breakout_pool_seen ON breakout_stock_pool(last_seen_date)")
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


def normalize_stock_spot_row(row):
    code = str(row.get("代码", row.get("code", ""))).strip().zfill(6)
    return {
        "code": code,
        "name": str(row.get("名称", row.get("name", ""))).strip(),
        "price": to_float(row.get("最新价", row.get("price", 0))),
        "change_pct": to_float(row.get("涨跌幅", row.get("change_pct", 0))),
        "volume": to_float(row.get("成交量", row.get("volume", 0))),
        "amount": to_float(row.get("成交额", row.get("amount", 0))),
        "turnover": to_float(row.get("换手率", row.get("turnover", 0))),
        "pe": to_float(row.get("市盈率-动态", row.get("市盈率", row.get("pe", 0)))),
        "pb": to_float(row.get("市净率", row.get("pb", 0))),
    }


def is_tradeable_a_stock(stock):
    code = str(stock.get("code", ""))
    name = str(stock.get("name", ""))
    if not code or code.startswith(("4", "8")):
        return False
    if not code.startswith(("0", "3", "6")):
        return False
    if "ST" in name or stock.get("price", 0) <= 0:
        return False
    return True


def get_all_market_stocks(use_cache=True, cache_only=False, snapshot_date=None):
    snapshot_date = snapshot_date or datetime.now().strftime("%Y-%m-%d")
    cache_name = f"all_market_{snapshot_date.replace('-', '')}"
    memory_cached = runtime_cache_get("all_market_stocks", cache_name, enabled=use_cache)
    if memory_cached is not None:
        bump_cache_stat("all_market_stocks", "memory")
        return memory_cached
    if use_cache:
        cached = load_cache(cache_name)
        if cached:
            bump_cache_stat("all_market_stocks", "cache")
            return runtime_cache_set("all_market_stocks", cache_name, cached.get("data", []), enabled=use_cache)
    if cache_only:
        note("cache-only mode: no cache available for all market stocks")
        return []

    def fetch():
        df = ak.stock_zh_a_spot_em()
        result = []
        for _, row in df.iterrows():
            item = normalize_stock_spot_row(row)
            if is_tradeable_a_stock(item):
                result.append(item)
        return rank_sector_members(result)

    result = fetch_with_retry(
        "all_market_stocks",
        fetch,
        cache_name=cache_name,
        validator=lambda items: bool(items),
        sleep_seconds=2,
        use_cache=use_cache,
        cache_only=cache_only,
    )
    if not result:
        note(f"全市场股票列表获取失败，突破池全市场补池本轮跳过；已重试 {MAX_RETRIES} 次。")
    return runtime_cache_set("all_market_stocks", cache_name, result, enabled=use_cache)


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
    memory_cached = runtime_cache_get("sector_members", cache_name, enabled=use_cache)
    if memory_cached is not None:
        bump_cache_stat("sector_members", "memory")
        return memory_cached
    if use_cache:
        cached = load_cache(cache_name)
        if cached:
            bump_cache_stat("sector_members", "cache")
            return runtime_cache_set("sector_members", cache_name, cached.get("data", []), enabled=use_cache)
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
    runtime_cache_set("sector_members", cache_name, result, enabled=use_cache)
    bump_cache_stat("sector_members", "live")
    return result


def build_hot_concept_member_map(concept_sectors, config, analysis_date, use_cache=True, cache_only=False):
    hot_concepts = [
        item
        for item in concept_sectors[: config["concept_scan_limit"]]
        if item.get("change_pct", 0) > 2 and not is_excluded_sector(item, config)
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
    memory_cached = runtime_cache_get("stock_history", cache_name, enabled=use_cache)
    if memory_cached is not None:
        bump_cache_stat("stock_history", "memory")
        return memory_cached
    if use_cache:
        cached = load_cache(cache_name)
        if cached:
            bump_cache_stat("stock_history", "cache")
            df_cached = pd.DataFrame(cached.get("data", []))
            return runtime_cache_set("stock_history", cache_name, df_cached, enabled=use_cache).copy()
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
        runtime_cache_set("stock_history", cache_name, df, enabled=use_cache)
        bump_cache_stat("stock_history", "live")
        return df
    except Exception as exc:  # noqa: BLE001
        logger.error("get history failed [%s]: %s", code, exc)
        return pd.DataFrame()


def get_benchmark_history(name, bs_code, end_date=None, days=120, use_cache=True, cache_only=False):
    end_date = end_date or datetime.now().strftime("%Y-%m-%d")
    hashed = cache_key("benchmark", name, bs_code, end_date, days)
    cache_name = f"benchmark_{hashed}"
    memory_cached = runtime_cache_get("benchmark_history", cache_name, enabled=use_cache)
    if memory_cached is not None:
        bump_cache_stat("benchmark_history", "memory")
        return memory_cached
    if use_cache:
        cached = load_cache(cache_name)
        if cached:
            bump_cache_stat("benchmark_history", "cache")
            df_cached = pd.DataFrame(cached.get("data", []))
            return runtime_cache_set("benchmark_history", cache_name, df_cached, enabled=use_cache).copy()
    if cache_only:
        note(f"cache-only mode: no cache available for benchmark {name}")
        return pd.DataFrame()

    try:
        start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(days=days + 30)).strftime("%Y-%m-%d")
        rs = bs.query_history_k_data_plus(
            bs_code,
            "date,open,close",
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
        for column in ("open", "close"):
            df[column] = pd.to_numeric(df[column], errors="coerce")
        df = df.dropna(subset=["close"]).reset_index(drop=True)
        if use_cache:
            save_cache(cache_name, {"data": df.to_dict("records")})
        runtime_cache_set("benchmark_history", cache_name, df, enabled=use_cache)
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


def empty_anchored_breakout_state():
    return {
        "anchor_found": False,
        "anchor_type": None,
        "anchor_price": None,
        "anchor_date": None,
        "anchor_days_ago": None,
        "anchor_latest_touch_date": None,
        "anchor_latest_touch_days_ago": None,
        "anchor_zone_touches": 0,
        "anchor_visibility_score": 0.0,
        "anchor_pullback_pct": 0.0,
        "anchor_base_days": 0,
        "anchored_breakout_signal": False,
        "anchored_breakout_score": 0.0,
        "anchored_breakout_day": None,
        "anchored_breakout_volume_ratio": 0.0,
        "breakout_amount_ratio20": 0.0,
        "breakout_close_position": 0.0,
        "pullback_amount_ratio20": 0.0,
        "pullback_amount_vs_breakout": 0.0,
        "pullback_volume_score": 0.0,
        "bad_volume_down_day": False,
        "confirm_price_volume_up_days": 0,
        "confirm_amount_ratio20": 0.0,
        "volume_price_score": 0.0,
        "anchored_shape_score": 0.0,
        "anchored_volume_score": 0.0,
        "anchored_shape_grade": "D",
        "anchored_breakout_grade": "",
        "breakout_volume_pass": False,
        "pullback_shrink_pass": False,
        "confirm_volume_price_pass": False,
        "volume_price_summary": "",
    }


def get_anchor_type_label(anchor_type):
    mapping = {
        "absolute_high": "绝对高点",
        "resistance_zone": "阻力区",
        "swing_high": "摆动高点",
    }
    return mapping.get(anchor_type, anchor_type or "-")


def safe_series_ratio(numerator, denominator):
    result = numerator.astype(float) / denominator.replace(0, pd.NA).astype(float)
    return result.fillna(0.0)


def calc_close_position(close, high, low):
    price_range = (high - low).replace(0, pd.NA)
    return ((close - low) / price_range).fillna(0.5).clip(lower=0.0, upper=1.0)


def is_pivot_high(series, index, left, right):
    if index - left < 0 or index + right >= len(series):
        return False
    center = series.iloc[index]
    if pd.isna(center):
        return False
    left_max = series.iloc[index - left : index].max()
    right_max = series.iloc[index + 1 : index + 1 + right].max()
    return bool(center >= left_max and center >= right_max and (center > left_max or center > right_max))


def build_prior_high_zones(df, config):
    settings = config.get("anchored_breakout", {})
    breakout_window = settings["breakout_window"]
    reference_window = settings["anchor_reference_window"]
    min_gap = settings["anchor_min_gap"]
    left = settings["pivot_left"]
    right = settings["pivot_right"]
    zone_pct = settings["anchor_zone_pct"]
    absolute_high_near_pct = settings["absolute_high_near_pct"]
    min_pullback_pct = settings["min_pullback_pct"]
    min_base_days = settings["min_base_days"]
    base_below_buffer_pct = settings["base_below_buffer_pct"]

    min_history = max(60, breakout_window + min_gap + right * 4)
    if df.empty or len(df) < min_history:
        return None

    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    dates = df["date"] if "date" in df else None
    last_index = len(df) - 1
    breakout_start = max(0, len(df) - breakout_window)
    reference_end = breakout_start - 1
    if reference_end <= 0:
        return None
    if reference_window > 0:
        reference_start = max(0, reference_end - reference_window + 1)
    else:
        reference_start = 0

    candidates = []
    for idx in range(reference_start, reference_end + 1):
        if last_index - idx < min_gap:
            continue

        pivot_scale = 0
        for multiplier in (1, 2, 4):
            scale_left = left * multiplier
            scale_right = right * multiplier
            if idx - scale_left < reference_start or idx + scale_right > reference_end:
                continue
            if is_pivot_high(high, idx, scale_left, scale_right):
                pivot_scale = multiplier
        if pivot_scale <= 0:
            continue

        anchor_price = float(high.iloc[idx])
        post_close = close.iloc[idx + 1 : breakout_start]
        post_low = low.iloc[idx + 1 : breakout_start]
        if post_close.empty or post_low.empty or anchor_price <= 0:
            continue

        pullback_pct = float((anchor_price - float(post_low.min())) / anchor_price)
        base_days = int((post_close <= anchor_price * (1 - base_below_buffer_pct)).sum())
        if pullback_pct < min_pullback_pct and base_days < min_base_days:
            continue

        candidates.append(
            {
                "index": idx,
                "price": anchor_price,
                "date": str(dates.iloc[idx]) if dates is not None else None,
                "days_ago": last_index - idx,
                "pullback_pct": round(pullback_pct * 100, 2),
                "base_days": base_days,
                "pivot_scale": pivot_scale,
            }
        )

    if not candidates:
        return None

    pre_breakout_high = float(high.iloc[reference_start : reference_end + 1].max())
    zones = []
    for candidate in sorted(candidates, key=lambda item: item["price"], reverse=True):
        matched_zone = None
        for zone in zones:
            zone_top = zone["top_price"]
            if abs(candidate["price"] - zone_top) / max(candidate["price"], zone_top) <= zone_pct:
                matched_zone = zone
                break
        if matched_zone is None:
            matched_zone = {
                "top_price": candidate["price"],
                "members": [],
            }
            zones.append(matched_zone)
        matched_zone["members"].append(candidate)
        matched_zone["top_price"] = max(matched_zone["top_price"], candidate["price"])

    scored_zones = []
    for zone in zones:
        members = zone["members"]
        highest_member = max(members, key=lambda item: (item["price"], item["index"]))
        latest_member = max(members, key=lambda item: item["index"])
        touches = len(members)
        max_pullback_pct = max(item["pullback_pct"] for item in members)
        max_base_days = max(item["base_days"] for item in members)
        max_pivot_scale = max(item["pivot_scale"] for item in members)
        price_rank = zone["top_price"] / pre_breakout_high if pre_breakout_high > 0 else 0.0
        recency_norm = 1 - min(latest_member["days_ago"], 360) / 360
        touch_norm = min(touches, 4) / 4
        prominence_norm = min((max_pullback_pct / 100) / max(min_pullback_pct, 0.01), 2.0) / 2.0
        scale_norm = min(max_pivot_scale, 4) / 4
        is_recent_absolute_high = bool(price_rank >= 1 - absolute_high_near_pct)
        visibility_score = (
            30 * price_rank
            + 25 * recency_norm
            + 20 * touch_norm
            + 15 * prominence_norm
            + 10 * scale_norm
            + (10 if is_recent_absolute_high else 0)
        )
        anchor_type = "absolute_high" if is_recent_absolute_high else "resistance_zone" if touches >= 2 else "swing_high"
        scored_zones.append(
            {
                "index": highest_member["index"],
                "price": round(zone["top_price"], 2),
                "date": highest_member["date"],
                "days_ago": highest_member["days_ago"],
                "latest_touch_date": latest_member["date"],
                "latest_touch_days_ago": latest_member["days_ago"],
                "pullback_pct": round(max_pullback_pct, 2),
                "base_days": max_base_days,
                "touches": touches,
                "pivot_scale": max_pivot_scale,
                "anchor_type": anchor_type,
                "visibility_score": round(visibility_score, 2),
                "is_recent_absolute_high": is_recent_absolute_high,
            }
        )

    return scored_zones


def build_prior_high_cache_key(df, config, cache_key_id):
    if not cache_key_id or df.empty:
        return None
    settings = config.get("anchored_breakout", {})
    last_date = str(df["date"].iloc[-1]) if "date" in df else str(len(df))
    key_fields = (
        settings.get("breakout_window"),
        settings.get("anchor_reference_window"),
        settings.get("anchor_min_gap"),
        settings.get("pivot_left"),
        settings.get("pivot_right"),
        settings.get("anchor_zone_pct"),
        settings.get("absolute_high_near_pct"),
        settings.get("min_pullback_pct"),
        settings.get("min_base_days"),
        settings.get("base_below_buffer_pct"),
    )
    return (str(cache_key_id), last_date, len(df), key_fields)


def get_prior_high_zones(df, config, cache_key_id=None):
    cache_key_value = build_prior_high_cache_key(df, config, cache_key_id)
    if cache_key_value is not None and cache_key_value in PRIOR_HIGH_ZONE_CACHE:
        cached = PRIOR_HIGH_ZONE_CACHE[cache_key_value]
        return [dict(item) for item in cached] if cached else None

    zones = build_prior_high_zones(df, config)
    if cache_key_value is not None:
        PRIOR_HIGH_ZONE_CACHE[cache_key_value] = [dict(item) for item in zones] if zones else None
    return zones


def find_prior_high_anchor(df, config, scored_zones=None):
    settings = config.get("anchored_breakout", {})
    if scored_zones is None:
        scored_zones = build_prior_high_zones(df, config)
    if not scored_zones:
        return None

    active_touch_days = settings["active_touch_days"]
    preferred_zones = [item for item in scored_zones if item["latest_touch_days_ago"] <= active_touch_days] or scored_zones
    return max(
        preferred_zones,
        key=lambda item: (
            item["visibility_score"],
            item["latest_touch_days_ago"] * -1,
            item["price"],
            item["touches"],
        ),
    )


def apply_anchor_state(state, anchor):
    state.update(
        {
            "anchor_found": True,
            "anchor_type": anchor.get("anchor_type"),
            "anchor_price": round(anchor["price"], 2),
            "anchor_date": anchor["date"],
            "anchor_days_ago": anchor["days_ago"],
            "anchor_latest_touch_date": anchor.get("latest_touch_date"),
            "anchor_latest_touch_days_ago": anchor.get("latest_touch_days_ago"),
            "anchor_zone_touches": int(anchor.get("touches", 1)),
            "anchor_visibility_score": round(float(anchor.get("visibility_score", 0.0)), 2),
            "anchor_pullback_pct": anchor["pullback_pct"],
            "anchor_base_days": anchor["base_days"],
        }
    )


def build_breakout_context(df):
    close = df["close"].astype(float)
    open_ = df["open"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    volume = df["volume"].astype(float)
    amount = df["amount"].astype(float) if "amount" in df else pd.Series(0.0, index=df.index, dtype="float64")
    return {
        "close": close,
        "open": open_,
        "high": high,
        "low": low,
        "volume": volume,
        "amount": amount,
        "volume_ma5": calc_ma(volume, 5),
        "amount_ma5": calc_ma(amount, 5),
        "amount_ratio20": safe_series_ratio(amount, calc_ma(amount, 20)),
        "close_position": calc_close_position(close, high, low),
        "daily_change_pct": close.pct_change().fillna(0.0) * 100,
    }


def find_breakout_index(zone, context, settings, breakout_start, history_length):
    close = context["close"]
    volume = context["volume"]
    amount = context["amount"]
    volume_ma5 = context["volume_ma5"]
    amount_ma5 = context["amount_ma5"]
    threshold = zone["price"] * (1 + settings["close_break_buffer_pct"])
    for idx in range(breakout_start, history_length):
        prev_close = close.iloc[idx - 1] if idx > 0 else close.iloc[idx]
        if close.iloc[idx] <= threshold or prev_close > threshold:
            continue

        volume_base = volume_ma5.iloc[idx]
        amount_base = amount_ma5.iloc[idx]
        volume_ratio = float(volume.iloc[idx] / volume_base) if volume_base and not pd.isna(volume_base) else 0.0
        amount_ratio = float(amount.iloc[idx] / amount_base) if amount_base and not pd.isna(amount_base) else 0.0
        return idx, max(volume_ratio, amount_ratio)
    return None, 0.0


def score_breakout_day(breakout_index, history_length, settings):
    breakout_day = history_length - breakout_index
    best_day = settings["best_breakout_day"]
    breakout_score = 50 - 8 * max(0, breakout_day - best_day) - 5 * max(0, best_day - breakout_day)
    return breakout_day, max(0.0, float(breakout_score))


def evaluate_breakout_day_quality(breakout_index, breakout_volume_ratio, context, settings):
    breakout_amount_ratio20 = float(context["amount_ratio20"].iloc[breakout_index])
    breakout_close_position = float(context["close_position"].iloc[breakout_index])
    breakout_volume_pass = breakout_volume_ratio >= settings["volume_confirm_ratio"]
    breakout_amount_pass = (
        breakout_amount_ratio20 >= settings["breakout_amount_ratio20_min"]
        and breakout_amount_ratio20 <= settings["breakout_amount_ratio20_max"]
    )
    close_position_pass = breakout_close_position >= settings["breakout_close_position_min"]
    return {
        "breakout_amount_ratio20": breakout_amount_ratio20,
        "breakout_close_position": breakout_close_position,
        "breakout_volume_pass": bool(breakout_volume_pass and breakout_amount_pass and close_position_pass),
    }


def evaluate_pullback_quality(breakout_index, context, settings, history_length):
    post_start = breakout_index + 1
    post_end = min(history_length, breakout_index + settings["pullback_window"] + 1)
    result = {
        "pullback_amount_ratio20": 0.0,
        "pullback_amount_vs_breakout": 0.0,
        "pullback_volume_score": 0.0,
        "pullback_shrink_pass": False,
    }
    if post_start >= post_end:
        return result

    pullback_lows = context["low"].iloc[post_start:post_end].reset_index(drop=True)
    pullback_index = post_start + int(pullback_lows.idxmin())
    breakout_amount = float(context["amount"].iloc[breakout_index])
    pullback_amount = float(context["amount"].iloc[pullback_index])
    pullback_amount_vs_breakout = pullback_amount / breakout_amount if breakout_amount > 0 else 0.0
    pullback_amount_ratio20 = float(context["amount_ratio20"].iloc[pullback_index])
    pullback_shrink_pass = (
        pullback_amount_vs_breakout <= settings["pullback_amount_vs_breakout_max"]
        and pullback_amount_ratio20 <= settings["pullback_amount_ratio20_max"]
    )
    result.update(
        {
            "pullback_amount_ratio20": pullback_amount_ratio20,
            "pullback_amount_vs_breakout": pullback_amount_vs_breakout,
            "pullback_volume_score": round(10 * max(0.0, 1 - pullback_amount_vs_breakout), 2) if pullback_shrink_pass else 0.0,
            "pullback_shrink_pass": bool(pullback_shrink_pass),
        }
    )
    return result


def has_bad_volume_down_day(breakout_index, context, settings, history_length):
    post_slice_start = breakout_index + 1
    if post_slice_start >= history_length:
        return False
    post_slice = slice(post_slice_start, history_length)
    return bool(
        (
            (context["close"].iloc[post_slice] < context["open"].iloc[post_slice])
            & (context["daily_change_pct"].iloc[post_slice] <= settings["bad_down_change_pct"])
            & (context["amount_ratio20"].iloc[post_slice] >= settings["bad_down_amount_ratio20_min"])
        ).any()
    )


def evaluate_confirm_quality(context, settings, history_length):
    close = context["close"]
    amount = context["amount"]
    confirm_start = max(1, history_length - settings["confirm_window"])
    confirm_price_volume_up_days = int(
        ((close.iloc[confirm_start:] > close.shift(1).iloc[confirm_start:]) & (amount.iloc[confirm_start:] > amount.shift(1).iloc[confirm_start:])).sum()
    )
    confirm_amount_ratio20 = float(context["amount_ratio20"].iloc[-1])
    confirm_amount_ok = (
        confirm_amount_ratio20 >= settings["confirm_amount_ratio20_min"]
        and confirm_amount_ratio20 <= settings["confirm_amount_ratio20_max"]
    )
    confirm_volume_price_pass = confirm_price_volume_up_days >= settings["confirm_price_volume_up_days"] and confirm_amount_ok
    return {
        "confirm_price_volume_up_days": confirm_price_volume_up_days,
        "confirm_amount_ratio20": confirm_amount_ratio20,
        "confirm_volume_score": 5.0 if confirm_volume_price_pass else 0.0,
        "confirm_volume_price_pass": bool(confirm_volume_price_pass),
    }


def evaluate_breakout_candidate(zone, context, settings, breakout_start, history_length):
    breakout_index, breakout_volume_ratio = find_breakout_index(zone, context, settings, breakout_start, history_length)
    if breakout_index is None:
        return None

    breakout_day, breakout_score = score_breakout_day(breakout_index, history_length, settings)
    breakout_quality = evaluate_breakout_day_quality(breakout_index, breakout_volume_ratio, context, settings)
    pullback_quality = evaluate_pullback_quality(breakout_index, context, settings, history_length)
    confirm_quality = evaluate_confirm_quality(context, settings, history_length)
    bad_volume_down_day = has_bad_volume_down_day(breakout_index, context, settings, history_length)
    volume_price_score = round(pullback_quality["pullback_volume_score"] + confirm_quality["confirm_volume_score"], 2)
    return {
        "anchor": zone,
        "breakout_index": breakout_index,
        "breakout_day": breakout_day,
        "breakout_score": round(breakout_score, 2),
        "breakout_volume_ratio": round(breakout_volume_ratio, 2),
        "breakout_amount_ratio20": round(breakout_quality["breakout_amount_ratio20"], 2),
        "breakout_close_position": round(breakout_quality["breakout_close_position"], 2),
        "pullback_amount_ratio20": round(pullback_quality["pullback_amount_ratio20"], 2),
        "pullback_amount_vs_breakout": round(pullback_quality["pullback_amount_vs_breakout"], 2),
        "pullback_volume_score": pullback_quality["pullback_volume_score"],
        "bad_volume_down_day": bad_volume_down_day,
        "confirm_price_volume_up_days": confirm_quality["confirm_price_volume_up_days"],
        "confirm_amount_ratio20": round(confirm_quality["confirm_amount_ratio20"], 2),
        "volume_price_score": volume_price_score,
        "breakout_volume_pass": breakout_quality["breakout_volume_pass"],
        "pullback_shrink_pass": pullback_quality["pullback_shrink_pass"],
        "confirm_volume_price_pass": confirm_quality["confirm_volume_price_pass"],
        "breakout_signal": bool(
            breakout_score >= settings["min_breakout_score"]
            and breakout_quality["breakout_volume_pass"]
            and not bad_volume_down_day
        ),
    }


def select_breakout_candidate(breakout_candidates):
    selected_pool = [item for item in breakout_candidates if item["breakout_signal"]] or breakout_candidates
    return max(
        selected_pool,
        key=lambda item: (
            item["anchor"]["price"],
            item["anchor"]["visibility_score"],
            item["breakout_volume_ratio"],
            item["breakout_day"] * -1,
        ),
    )


def summarize_volume_price(selected_breakout):
    pullback_text = "回踩缩量" if selected_breakout["pullback_shrink_pass"] else "回踩缩量不足"
    confirm_text = "确认放量上行" if selected_breakout["confirm_volume_price_pass"] else "确认不足"
    return (
        f"突破额比{selected_breakout['breakout_amount_ratio20']:.2f}, "
        f"收盘位置{selected_breakout['breakout_close_position']:.2f}, "
        f"{pullback_text}, {confirm_text}{selected_breakout['confirm_price_volume_up_days']}天"
    )


def calc_anchor_shape_score(anchor):
    visibility = float(anchor.get("visibility_score", 0.0))
    touches = int(anchor.get("touches", 1))
    base_days = int(anchor.get("base_days", 0))
    pullback_pct = float(anchor.get("pullback_pct", 0.0))
    score = min(10.0, visibility / 10)
    score += min(3.0, max(0, touches - 1))
    score += min(3.0, base_days / 10)
    score += min(4.0, pullback_pct / 5)
    return round(min(20.0, score), 2)


def grade_from_score(score, thresholds):
    for grade, threshold in thresholds:
        if score >= threshold:
            return grade
    return "D"


def calc_anchored_breakout_grade(shape_score, selected_breakout):
    shape_grade = grade_from_score(shape_score, (("A", 16), ("B", 12), ("C", 8)))
    if selected_breakout.get("breakout_volume_pass") and selected_breakout.get("pullback_shrink_pass") and selected_breakout.get("confirm_volume_price_pass"):
        volume_grade = "A"
    elif selected_breakout.get("breakout_volume_pass") and selected_breakout.get("pullback_shrink_pass"):
        volume_grade = "B"
    elif selected_breakout.get("breakout_volume_pass"):
        volume_grade = "C"
    else:
        volume_grade = "D"
    combined_rank = min("ABCD".index(shape_grade), "ABCD".index(volume_grade))
    combined_grade = "ABCD"[combined_rank]
    suffix = "+" if shape_grade != volume_grade and combined_grade in {shape_grade, volume_grade} else ""
    return shape_grade, f"形态{shape_grade}+量价{volume_grade}={combined_grade}{suffix}"


def evaluate_anchored_breakout(df, config, scored_zones=None, cache_key_id=None):
    settings = config.get("anchored_breakout", {})
    state = empty_anchored_breakout_state()
    if not settings.get("enabled", True):
        return state
    min_history = max(60, settings["breakout_window"] + settings["anchor_min_gap"] + settings["pivot_right"] * 4)
    if df.empty or len(df) < min_history:
        return state

    if scored_zones is None:
        scored_zones = get_prior_high_zones(df, config, cache_key_id=cache_key_id)
    if not scored_zones:
        return state
    anchor = find_prior_high_anchor(df, config, scored_zones=scored_zones) or scored_zones[0]
    apply_anchor_state(state, anchor)
    context = build_breakout_context(df)

    breakout_start = max(0, len(df) - settings["breakout_window"])
    breakout_candidates = [
        candidate
        for candidate in (
            evaluate_breakout_candidate(zone, context, settings, breakout_start, len(df))
            for zone in scored_zones
        )
        if candidate is not None
    ]

    if not breakout_candidates:
        return state

    selected_breakout = select_breakout_candidate(breakout_candidates)
    selected_anchor = selected_breakout["anchor"]
    apply_anchor_state(state, selected_anchor)
    shape_score = calc_anchor_shape_score(selected_anchor)
    shape_grade, breakout_grade = calc_anchored_breakout_grade(shape_score, selected_breakout)

    state.update(
        {
            "anchored_breakout_signal": selected_breakout["breakout_signal"],
            "anchored_breakout_score": selected_breakout["breakout_score"],
            "anchored_breakout_day": selected_breakout["breakout_day"],
            "anchored_breakout_volume_ratio": selected_breakout["breakout_volume_ratio"],
            "breakout_amount_ratio20": selected_breakout["breakout_amount_ratio20"],
            "breakout_close_position": selected_breakout["breakout_close_position"],
            "pullback_amount_ratio20": selected_breakout["pullback_amount_ratio20"],
            "pullback_amount_vs_breakout": selected_breakout["pullback_amount_vs_breakout"],
            "pullback_volume_score": selected_breakout["pullback_volume_score"],
            "bad_volume_down_day": selected_breakout["bad_volume_down_day"],
            "confirm_price_volume_up_days": selected_breakout["confirm_price_volume_up_days"],
            "confirm_amount_ratio20": selected_breakout["confirm_amount_ratio20"],
            "volume_price_score": selected_breakout["volume_price_score"],
            "anchored_shape_score": shape_score,
            "anchored_volume_score": selected_breakout["volume_price_score"],
            "anchored_shape_grade": shape_grade,
            "anchored_breakout_grade": breakout_grade,
            "breakout_volume_pass": selected_breakout["breakout_volume_pass"],
            "pullback_shrink_pass": selected_breakout["pullback_shrink_pass"],
            "confirm_volume_price_pass": selected_breakout["confirm_volume_price_pass"],
            "volume_price_summary": summarize_volume_price(selected_breakout),
        }
    )
    return state


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
    latest["ema55_rising"] = (
        ema_long_series.iloc[-1] > ema_long_series.iloc[-3] > ema_long_series.iloc[-5]
        if len(ema_long_series) >= 5
        else False
    )
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
        "ema55_rising": bool(tech.get("ema55_rising", False)),
        "near_ema": abs(tech["ema_dist_pct"]) < threshold,
        "shrink_volume": tech["vol_ratio"] < 1.05,
        "positive_5d": tech["change_5d"] > 0,
    }
    is_pullback = (
        conditions["ema_bullish"]
        and conditions["ema55_rising"]
        and conditions["near_ema"]
        and sum(conditions.values()) >= 5
    )
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


def is_excluded_sector(sector, config):
    name = str(sector.get("name", ""))
    return any(keyword and keyword in name for keyword in config.get("excluded_sector_keywords", []))


def normalize_sector_family_name(name):
    value = re.sub(r"[（(].*?[）)]", "", str(name))
    value = re.sub(r"[ⅠⅡⅢⅣⅤⅥⅦⅧⅨⅩIVX]+$", "", value)
    value = re.sub(r"(行业|概念|板块)$", "", value)
    return re.sub(r"\s+", "", value)


def dedupe_similar_sectors(sectors, config):
    if not config.get("merge_similar_sectors", True):
        return sectors
    deduped = []
    seen = set()
    for sector in sectors:
        family = normalize_sector_family_name(sector.get("name", ""))
        if family in seen:
            continue
        seen.add(family)
        deduped.append(sector)
    return deduped


def classify_sectors(industry_sectors, concept_sectors, fund_flows, config):
    filtered_industries = [item for item in industry_sectors if not is_excluded_sector(item, config)]
    filtered_concepts = [item for item in concept_sectors if not is_excluded_sector(item, config)]
    scored_industries = [score_sector(dict(item), fund_flows, config) for item in filtered_industries]
    industry_sorted = dedupe_similar_sectors(
        sorted(scored_industries, key=lambda item: item["score"], reverse=True),
        config,
    )

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
    hot_concepts = [item for item in filtered_concepts if item["change_pct"] > 2][:10]
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
    soft_penalties = risk_cfg.get("soft_penalties", {})
    hard_filters = risk_cfg.get("hard_filters", {})
    notes = []
    penalty = 0
    blocked = False

    if stock["pe"] > risk_cfg["high_pe_warning"]:
        notes.append(f"PE {stock['pe']:.0f} 偏高")
        penalty += soft_penalties.get("high_pe", 6)
    if stock["pe"] < 0:
        notes.append(f"PE {stock['pe']:.0f}")
        penalty += soft_penalties.get("negative_pe", 0)
        if hard_filters.get("exclude_negative_pe", False) or risk_cfg.get("exclude_negative_pe", False):
            blocked = True
    if tech["change_5d"] > risk_cfg["max_change_5d"]:
        notes.append(f"5日涨幅过高 {tech['change_5d']:.1f}%")
        penalty += soft_penalties.get("change_5d", 8)
    if tech["vol_ratio"] > risk_cfg["max_vol_ratio"]:
        notes.append(f"量比过高 {tech['vol_ratio']:.2f}")
        penalty += soft_penalties.get("vol_ratio", 5)
    if tech["volatility_10d"] > risk_cfg["max_volatility_10d"]:
        notes.append(f"10日波动偏高 {tech['volatility_10d']:.1f}%")
        penalty += soft_penalties.get("volatility_10d", 5)
    return notes or ["正常"], penalty, blocked


def build_condition_summary(conditions):
    label_map = {
        "ema_bullish": "EMA多头",
        "ema_rising": "EMA向上",
        "ema55_rising": "EMA55上行",
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


def get_signal_priority(item):
    if item.get("anchored_breakout_signal") and item.get("confirm_volume_price_pass"):
        return 4
    if item.get("is_pullback"):
        return 3
    if item.get("breakout_signal"):
        return 2
    if item.get("turnaround_signal") or item.get("macd_ignite"):
        return 1
    return 0


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


def build_daily_strategy_state(df, stock, config=None):
    config = config or DEFAULT_CONFIG
    if df.empty or len(df) < 250:
        return {
            "ddx": 0.0,
            "ddx_positive_days_5": 0,
            "big_order_alert": False,
            "macd_ignite": False,
            "turnaround_signal": False,
            "breakout_signal": False,
            "stop_profit_line": None,
            "anchor_found": False,
            "anchor_type": None,
            "anchor_price": None,
            "anchor_date": None,
            "anchor_days_ago": None,
            "anchor_latest_touch_date": None,
            "anchor_latest_touch_days_ago": None,
            "anchor_zone_touches": 0,
            "anchor_visibility_score": 0.0,
            "anchor_pullback_pct": 0.0,
            "anchor_base_days": 0,
            "anchored_breakout_signal": False,
            "anchored_breakout_score": 0.0,
            "anchored_breakout_day": None,
            "anchored_breakout_volume_ratio": 0.0,
            "breakout_amount_ratio20": 0.0,
            "breakout_close_position": 0.0,
            "pullback_amount_ratio20": 0.0,
            "pullback_amount_vs_breakout": 0.0,
            "pullback_volume_score": 0.0,
            "bad_volume_down_day": False,
            "confirm_price_volume_up_days": 0,
            "confirm_amount_ratio20": 0.0,
            "volume_price_score": 0.0,
            "anchored_shape_score": 0.0,
            "anchored_volume_score": 0.0,
            "anchored_shape_grade": "D",
            "anchored_breakout_grade": "",
            "breakout_volume_pass": False,
            "pullback_shrink_pass": False,
            "confirm_volume_price_pass": False,
            "volume_price_summary": "",
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

    anchored_breakout = evaluate_anchored_breakout(df, config, cache_key_id=stock.get("code"))

    strategy_tags = []
    if breakout_signal:
        strategy_tags.append("趋势突破")
    if anchored_breakout.get("anchored_breakout_signal"):
        strategy_tags.append("前高突破")
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
        **anchored_breakout,
        "strategy_tags": strategy_tags,
        "strategy_summary": "、".join(strategy_tags) if strategy_tags else "无",
    }


def apply_sector_rps_metrics(candidates, config):
    if not candidates:
        return candidates

    periods = config["rps_periods"]
    threshold = config["rps_threshold"]
    sample_size = len(candidates)
    rps_sample_too_small = sample_size < config.get("rps_min_sample", 5)
    for period in periods:
        change_key = f"change_{period}d"
        ranked = sorted(candidates, key=lambda item: item.get(change_key, float("-inf")))
        total = len(ranked)
        for rank, item in enumerate(ranked, 1):
            rps_value = round(rank / total * 100, 2)
            item[f"rps{period}"] = rps_value
            item[f"sector_pool_rps{period}"] = rps_value

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
        if rps_sample_too_small:
            three_cycle_red = False
            four_cycle_red = False
            strong_periods = 0

        item["three_cycle_red"] = three_cycle_red
        item["four_cycle_red"] = four_cycle_red
        item["rps_strong_periods"] = strong_periods
        item["rps_scope"] = "sector_pool"
        item["rps_sample_size"] = sample_size
        item["rps_sample_too_small"] = rps_sample_too_small
        item["rps_summary"] = (
            f"板块池RPS10/20/50/120/250="
            f"{item.get('rps10', 0):.1f}/{item.get('rps20', 0):.1f}/{item.get('rps50', 0):.1f}/"
            f"{item.get('rps120', 0):.1f}/{item.get('rps250', 0):.1f}"
        )
        if four_cycle_red:
            item["rps_summary"] += " | 四周期红"
        elif three_cycle_red:
            item["rps_summary"] += " | 三周期红"
        if rps_sample_too_small:
            item["rps_summary"] += f" | 样本不足{sample_size}"
    return candidates


def get_light_history_days(config):
    return max(
        80,
        config["ema_long"] + max(config["analysis_days"]) + 30,
        max(config["rps_periods"]) + 30,
    )


def get_full_history_days(config):
    return max(get_light_history_days(config), config["history_window_days"])


def build_scan_diagnostics(track_candidates=True):
    return {
        "stage_counts": defaultdict(int),
        "reject_reasons": defaultdict(int),
        "candidates": [],
        "track_candidates": track_candidates,
    }


def bump_scan_count(scan_diagnostics, bucket, key, amount=1):
    if scan_diagnostics is None:
        return
    scan_diagnostics.setdefault(bucket, defaultdict(int))[key] += amount


def normalize_scan_diagnostics(scan_diagnostics):
    if not scan_diagnostics:
        return {"stage_counts": {}, "reject_reasons": {}, "candidates": []}
    return {
        "stage_counts": dict(scan_diagnostics.get("stage_counts", {})),
        "reject_reasons": dict(scan_diagnostics.get("reject_reasons", {})),
        "candidates": list(scan_diagnostics.get("candidates", [])),
    }


def sanitize_candidate_for_export(item):
    blocked_keys = {"_history", "risk_notes", "base_score"}
    return {
        key: value
        for key, value in item.items()
        if key not in blocked_keys and not key.startswith("_")
    }


def record_scan_candidate(scan_diagnostics, item, status, reason=""):
    if scan_diagnostics is None:
        return
    bump_scan_count(scan_diagnostics, "stage_counts", status)
    if reason:
        bump_scan_count(scan_diagnostics, "reject_reasons", reason)
    if not scan_diagnostics.get("track_candidates", True):
        return
    snapshot = sanitize_candidate_for_export(item)
    snapshot["scan_status"] = status
    snapshot["reject_reason"] = reason
    scan_diagnostics.setdefault("candidates", []).append(snapshot)


def record_member_reject(scan_diagnostics, sector, stock, reason):
    item = {
        "sector": sector["name"],
        "code": stock.get("code", ""),
        "name": stock.get("name", ""),
        "price": to_float(stock.get("price")),
        "pe": to_float(stock.get("pe")),
        "amount": round(to_float(stock.get("amount")), 2),
        "turnover": round(to_float(stock.get("turnover")), 2),
    }
    record_scan_candidate(scan_diagnostics, item, "rejected", reason)


def build_initial_signal_breakdown(tech):
    return {
        "trend_pass": bool(tech.get("ema_bullish") and tech.get("ema13_rising")),
        "ema55_pass": bool(tech.get("ema55_rising", False)),
        "rps_pass": False,
        "breakout_setup_pass": False,
        "anchored_breakout_pass": False,
        "volume_quality_pass": False,
        "breakout_volume_pass": False,
        "pullback_shrink_pass": False,
        "confirm_volume_price_pass": False,
    }


def update_signal_breakdown_summary(item):
    breakdown = item.get("signal_breakdown", {})
    label_map = {
        "trend_pass": "趋势",
        "rps_pass": "RPS",
        "ema55_pass": "EMA55",
        "breakout_setup_pass": "突破迹象",
        "anchored_breakout_pass": "前高突破",
        "volume_quality_pass": "量价质量",
        "breakout_volume_pass": "突破量价",
        "pullback_shrink_pass": "回踩缩量",
        "confirm_volume_price_pass": "确认放量",
    }
    item["signal_breakdown_summary"] = " / ".join(
        f"{label}{'通过' if breakdown.get(key) else '未过'}" for key, label in label_map.items()
    )
    return item


def has_light_breakout_setup(df, tech, config):
    if df.empty:
        return False
    price = to_float(tech.get("price"))
    if price <= 0:
        return False
    setup_cfg = config["light_breakout_setup"]
    high = df["high"].astype(float) if "high" in df else df["close"].astype(float)
    window = min(setup_cfg["near_high_window"], len(high))
    recent_high = to_float(high.tail(window).max())
    if recent_high <= 0:
        return False
    near_recent_high = price >= recent_high * setup_cfg["near_high_pct"]
    low = df["low"].astype(float) if "low" in df else df["close"].astype(float)
    recent_low = to_float(low.tail(window).min())
    recent_drawdown = (recent_high - recent_low) / recent_high if recent_high > 0 else 0.0
    ema55 = to_float(tech.get("ema55") or tech.get("ema_long"))
    ema55_dist = abs(price - ema55) / ema55 if ema55 > 0 else 0.0
    if tech.get("change_20d", 0) > setup_cfg["max_change_20d"]:
        return False
    if ema55_dist > setup_cfg["max_ema55_dist_pct"]:
        return False
    if recent_drawdown > setup_cfg["max_recent_drawdown_pct"]:
        return False
    positive_5d = tech.get("change_5d", 0) > 0 if setup_cfg["require_positive_5d"] else tech.get("change_5d", 0) >= 0
    volume_clue = tech.get("vol_ratio", 0) >= setup_cfg["min_vol_ratio"] and positive_5d
    momentum_clue = tech.get("change_10d", 0) > 0 and tech.get("change_5d", 0) >= 0
    return bool(near_recent_high and (volume_clue or momentum_clue))


def get_light_reject_reason(item):
    breakdown = item.get("signal_breakdown", {})
    if not breakdown.get("trend_pass"):
        return "趋势未通过"
    if not breakdown.get("ema55_pass"):
        return "EMA55未上行"
    if not (breakdown.get("rps_pass") or breakdown.get("breakout_setup_pass")):
        return "RPS和突破迹象均未通过"
    return ""


def build_volume_quality_state(item, weights, volume_price_weight):
    basic_score = weights.get("volume_confirm", 0) if item.get("basic_volume_pass") else 0
    anchored_score = (
        min(item.get("volume_price_score", 0), volume_price_weight)
        if item.get("anchored_breakout_signal")
        else 0
    )
    score = max(basic_score, anchored_score)
    if item.get("anchored_breakout_signal") and item.get("breakout_volume_pass") and item.get("pullback_shrink_pass") and item.get("confirm_volume_price_pass"):
        grade = "A"
        grade_scope = "突破"
    elif item.get("anchored_breakout_signal") and item.get("breakout_volume_pass") and item.get("pullback_shrink_pass"):
        grade = "B"
        grade_scope = "突破"
    elif item.get("anchored_breakout_signal") and item.get("breakout_volume_pass"):
        grade = "C"
        grade_scope = "突破"
    elif item.get("basic_volume_pass"):
        grade = "C"
        grade_scope = "普通"
    else:
        grade = "D"
        grade_scope = "普通"
    if anchored_score >= basic_score and anchored_score > 0:
        summary = f"{grade_scope}{grade}级，前高量价 +{anchored_score:.1f}"
    elif basic_score > 0:
        summary = f"{grade_scope}{grade}级，普通放量 +{basic_score:.1f}"
    else:
        summary = f"{grade_scope}{grade}级，量价未加分"
    return {
        "volume_quality_score": round(score, 2),
        "volume_quality_grade": grade,
        "volume_quality_grade_scope": grade_scope,
        "volume_quality_summary": summary,
        "volume_quality_pass": score > 0,
    }


def build_initial_score_components(tech, is_pullback, risk_notes, risk_penalty, is_sector_leader, weights):
    trend_score = 0.0
    if tech["ema_bullish"]:
        trend_score += weights["ema_bullish"]
    if tech["ema13_rising"]:
        trend_score += weights["ema_rising"]
    if tech["ma_bullish"]:
        trend_score += weights["ma_bullish"]

    components = {
        "trend_score": trend_score,
        "pullback_score": weights["pullback"] if is_pullback else 0.0,
        "risk_score": weights["low_risk"] if risk_notes == ["正常"] else 0.0,
        "sector_leader_score": weights.get("sector_leader", 0) if is_sector_leader else 0.0,
        "risk_penalty": risk_penalty,
        "rps_score": 0.0,
        "volume_quality_score": 0.0,
        "breakout_score": 0.0,
        "strategy_score": 0.0,
        "base_score": 0.0,
        "final_score": 0.0,
    }
    components["base_score"] = round(
        components["trend_score"]
        + components["pullback_score"]
        + components["risk_score"]
        + components["sector_leader_score"]
        - components["risk_penalty"],
        2,
    )
    return components


def finalize_score_components(components):
    final_score = (
        components.get("base_score", 0.0)
        + components.get("rps_score", 0.0)
        + components.get("volume_quality_score", 0.0)
        + components.get("breakout_score", 0.0)
        + components.get("strategy_score", 0.0)
    )
    components["final_score"] = round(final_score, 2)
    return components


def strip_internal_stock_fields(item):
    item.pop("risk_notes", None)
    item.pop("base_score", None)
    for key in list(item):
        if key.startswith("_"):
            item.pop(key, None)
    return item


def apply_strategy_state_to_candidate(item, df, config):
    if item.get("_strategy_state_evaluated"):
        return item
    strategy_state = build_daily_strategy_state(df, item, config)
    item.update(strategy_state)
    item["_strategy_state_evaluated"] = True
    item.setdefault("signal_breakdown", {}).update(
        {
            "anchored_breakout_pass": bool(item.get("anchored_breakout_signal")),
            "breakout_volume_pass": bool(item.get("breakout_volume_pass")),
            "pullback_shrink_pass": bool(item.get("pullback_shrink_pass")),
            "confirm_volume_price_pass": bool(item.get("confirm_volume_price_pass")),
        }
    )
    return item


def discover_breakouts_from_sector_candidates(
    candidates,
    config,
    analysis_date,
    use_cache=True,
    cache_only=False,
    scan_diagnostics=None,
):
    pool_cfg = config.get("breakout_pool", {})
    if not pool_cfg.get("enabled", True) or not pool_cfg.get("sector_discovery_enabled", True):
        return candidates

    limit = pool_cfg.get("sector_discovery_limit_per_run", 0)
    full_history_days = get_full_history_days(config)
    discovered = 0
    checked = 0
    for item in candidates:
        if limit and checked >= limit:
            break
        if not item.get("breakout_setup_pass"):
            continue
        checked += 1
        bump_scan_count(scan_diagnostics, "stage_counts", "breakout_discovery_checked")
        full_history = item.get("_history")
        if not item.get("_full_history_loaded"):
            full_history = get_stock_history(
                item["code"],
                end_date=analysis_date,
                days=full_history_days,
                use_cache=use_cache,
                cache_only=cache_only,
            )
            if full_history.empty:
                bump_scan_count(scan_diagnostics, "reject_reasons", "突破发现长历史缺失")
                continue
            item["_history"] = full_history
            item["_full_history_loaded"] = True
            item["change_pct"] = round(calc_latest_change_pct(full_history), 2)

        apply_strategy_state_to_candidate(item, item["_history"], config)
        breakout_day = to_int(item.get("anchored_breakout_day"))
        if item.get("anchored_breakout_signal") and 0 < breakout_day <= pool_cfg["seed_breakout_days"]:
            item["pool_source"] = "sector_discovery"
            item["pool_status"] = "watch"
            item["breakout_price"] = item.get("anchor_price")
            record_scan_candidate(scan_diagnostics, item, "breakout_discovered")
            discovered += 1

    if checked:
        bump_scan_count(scan_diagnostics, "stage_counts", "breakout_discovery_found", discovered)
    return candidates


def load_active_breakout_pool():
    if not DB_PATH.exists():
        return []
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT stock_code, stock_name, sector_name, source, status, first_breakout_date,
               last_seen_date, breakout_price, current_price, ema55, score, signal, metadata
        FROM breakout_stock_pool
        WHERE status != 'removed'
        ORDER BY updated_at DESC
        """
    )
    rows = cursor.fetchall()
    conn.close()
    result = []
    for row in rows:
        metadata = {}
        if row[12]:
            try:
                metadata = json.loads(row[12])
            except ValueError:
                metadata = {}
        result.append(
            {
                "code": row[0],
                "name": row[1],
                "sector": row[2] or "突破池",
                "source": row[3] or "breakout_pool",
                "pool_status": row[4],
                "first_breakout_date": row[5],
                "last_seen_date": row[6],
                "breakout_price": to_float(row[7]),
                "current_price": to_float(row[8]),
                "ema55": to_float(row[9]),
                "score": to_float(row[10]),
                "signal": row[11] or "观察",
                "metadata": metadata,
                "pe": to_float(metadata.get("pe")),
                "amount": to_float(metadata.get("amount")),
                "turnover": to_float(metadata.get("turnover")),
            }
        )
    return result


def upsert_breakout_pool_entries(entries, analysis_date):
    if not entries:
        return []
    init_db()
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    saved = []
    for item in entries:
        code = str(item.get("code", "")).zfill(6)
        if not code:
            continue
        metadata = {
            "pe": item.get("pe", 0),
            "amount": item.get("amount", 0),
            "turnover": item.get("turnover", 0),
            "anchored_breakout_day": item.get("anchored_breakout_day"),
            "anchor_date": item.get("anchor_date"),
            "volume_price_summary": item.get("volume_price_summary", ""),
            "signal_breakdown_summary": item.get("signal_breakdown_summary", ""),
        }
        cursor.execute("SELECT first_breakout_date, created_at FROM breakout_stock_pool WHERE stock_code = ?", (code,))
        existing = cursor.fetchone()
        first_breakout_date = (
            existing[0]
            if existing and existing[0]
            else item.get("breakout_date") or item.get("first_breakout_date") or analysis_date
        )
        created_at = existing[1] if existing and existing[1] else now
        cursor.execute(
            """
            INSERT OR REPLACE INTO breakout_stock_pool
            (stock_code, stock_name, sector_name, source, status, first_breakout_date, last_seen_date,
             breakout_price, current_price, ema55, score, signal, remove_reason, metadata, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                code,
                item.get("name", ""),
                item.get("sector", "突破池"),
                item.get("pool_source", item.get("source", "breakout_pool")),
                item.get("pool_status", "watch"),
                first_breakout_date,
                analysis_date,
                to_float(item.get("breakout_price") or item.get("anchor_price")),
                to_float(item.get("price") or item.get("current_price")),
                to_float(item.get("ema55")),
                to_float(item.get("score")),
                item.get("signal", "观察"),
                json.dumps(metadata, ensure_ascii=False, default=json_default),
                created_at,
                now,
            ),
        )
        saved.append(code)
    conn.commit()
    conn.close()
    return saved


def mark_breakout_pool_removed(code, reason, analysis_date):
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    cursor.execute(
        """
        UPDATE breakout_stock_pool
        SET status = 'removed', remove_reason = ?, last_seen_date = ?, updated_at = ?
        WHERE stock_code = ?
        """,
        (reason, analysis_date, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), code),
    )
    conn.commit()
    conn.close()


def get_pool_first_breakout_age(entry, analysis_date):
    first_date = entry.get("first_breakout_date")
    if not first_date:
        return 0
    try:
        return max(0, (datetime.strptime(analysis_date, "%Y-%m-%d") - datetime.strptime(first_date, "%Y-%m-%d")).days)
    except ValueError:
        return 0


def count_consecutive_below_ema55(df, config):
    if df.empty or len(df) < config["ema_long"]:
        return 0
    close = df["close"].astype(float)
    ema55 = calc_ema(close, config["ema_long"])
    count = 0
    for idx in range(len(close) - 1, -1, -1):
        if close.iloc[idx] < ema55.iloc[idx]:
            count += 1
        else:
            break
    return count


def build_pool_status(item):
    if item.get("is_pullback") and item.get("confirm_volume_price_pass"):
        return "confirm"
    if item.get("is_pullback"):
        return "pullback"
    if item.get("anchored_breakout_signal"):
        return "watch"
    return "tracking"


def evaluate_breakout_pool_stock(stock, config, analysis_date, use_cache=True, cache_only=False):
    df_hist = get_stock_history(
        stock["code"],
        end_date=analysis_date,
        days=get_full_history_days(config),
        use_cache=use_cache,
        cache_only=cache_only,
    )
    if df_hist.empty:
        return None, "长历史缺失"
    tech = analyze_stock_tech(df_hist, config)
    if not tech:
        return None, "技术指标不足"

    is_pullback, conditions = check_pullback_signal(tech, config)
    matched, failed = build_condition_summary(conditions)
    risk_notes, risk_penalty, blocked = build_risk_assessment(stock, tech, config)
    if blocked and config["risk_filters"]["strict_mode"]:
        return None, "风险过滤"

    weights = config["score_weights"]["stock"]
    score_components = build_initial_score_components(tech, is_pullback, risk_notes, risk_penalty, False, weights)
    item = {
        "sector": stock.get("sector") or "突破池",
        "code": stock["code"],
        "name": stock.get("name", ""),
        "price": round(tech["price"], 2),
        "change_pct": round(calc_latest_change_pct(df_hist), 2),
        "ema13": round(tech["ema13"], 2),
        "ema55": round(tech["ema55"], 2),
        "ema13_dist_pct": round(tech["ema13_dist_pct"] * 100, 2),
        "signal": get_signal_type(tech, is_pullback),
        "is_pullback": is_pullback,
        "score": 0.0,
        "pe": to_float(stock.get("pe")),
        "vol_ratio": round(tech["vol_ratio"], 2),
        "change_5d": round(tech["change_5d"], 2),
        "change_10d": round(tech["change_10d"], 2),
        "change_50d": round(tech.get("change_50d", 0), 2),
        "change_120d": round(tech.get("change_120d", 0), 2),
        "change_250d": round(tech.get("change_250d", 0), 2),
        "volatility_10d": round(tech["volatility_10d"], 2),
        "risk": "；".join(risk_notes),
        "risk_notes": risk_notes,
        "signal_grade": "C",
        "is_sector_leader": False,
        "amount": round(to_float(stock.get("amount")), 2),
        "turnover": round(to_float(stock.get("turnover")), 2),
        "hot_topic": "",
        "matched_conditions": matched,
        "failed_conditions": failed,
        "base_score": score_components["base_score"],
        "score_components": score_components,
        "signal_breakdown": build_initial_signal_breakdown(tech),
        "basic_volume_pass": bool(tech["vol_ratio"] > config["light_breakout_setup"]["min_vol_ratio"] and tech["change_5d"] > 0),
        "breakout_setup_pass": has_light_breakout_setup(df_hist, tech, config),
        "rps_summary": "突破池跟踪，RPS不作为入池前置条件",
        "three_cycle_red": False,
        "four_cycle_red": False,
        "rps_strong_periods": 0,
        "volume_quality_score": 0.0,
        "volume_quality_grade": "D",
        "volume_quality_summary": "",
        "_history": df_hist,
        "summary": "",
    }
    strategy_state = build_daily_strategy_state(df_hist, item, config)
    item.update(strategy_state)
    item["signal_breakdown"].update(
        {
            "rps_pass": False,
            "breakout_setup_pass": bool(item.get("breakout_setup_pass")),
            "anchored_breakout_pass": bool(item.get("anchored_breakout_signal")),
            "breakout_volume_pass": bool(item.get("breakout_volume_pass")),
            "pullback_shrink_pass": bool(item.get("pullback_shrink_pass")),
            "confirm_volume_price_pass": bool(item.get("confirm_volume_price_pass")),
        }
    )
    volume_quality = build_volume_quality_state(item, weights, config["anchored_breakout"]["volume_price_weight"])
    item.update(volume_quality)
    item["signal_breakdown"]["volume_quality_pass"] = volume_quality["volume_quality_pass"]
    score_components["volume_quality_score"] = volume_quality["volume_quality_score"]
    if item.get("anchored_breakout_signal"):
        score_components["breakout_score"] += weights.get("anchored_breakout", 0)
    if item.get("breakout_signal"):
        score_components["breakout_score"] += weights.get("breakout", 0)
        item["signal"] = "趋势突破"
    if item.get("turnaround_signal"):
        score_components["strategy_score"] += weights.get("turnaround", 0)
    if item.get("big_order_alert"):
        score_components["strategy_score"] += weights.get("big_order_alert", 0)
    if item.get("macd_ignite"):
        score_components["strategy_score"] += weights.get("macd_ignite", 0)
    if item.get("anchored_breakout_signal") and item["signal"] == "观察":
        item["signal"] = "前高突破"
    finalize_score_components(score_components)
    item["score_components"] = score_components
    item["score"] = score_components["final_score"]
    item["signal_grade"] = get_signal_grade(item["score"], item["is_pullback"], item["risk_notes"], item["is_sector_leader"])
    item["breakout_price"] = to_float(stock.get("breakout_price") or item.get("anchor_price"))
    breakout_day = to_int(item.get("anchored_breakout_day"))
    if breakout_day > 0 and len(df_hist) >= breakout_day and "date" in df_hist:
        item["breakout_date"] = str(df_hist["date"].iloc[len(df_hist) - breakout_day])
    item["first_breakout_date"] = stock.get("first_breakout_date") or item.get("breakout_date") or analysis_date
    item["pool_status"] = build_pool_status(item)
    item["pool_age_days"] = get_pool_first_breakout_age(item, analysis_date)
    item["below_ema55_days"] = count_consecutive_below_ema55(df_hist, config)
    update_signal_breakdown_summary(item)
    item["summary"] = (
        f"突破池状态: {format_pool_status(item['pool_status'])}; "
        f"风险: {'、'.join(item['risk_notes'])}; "
        f"量价: {item['volume_quality_summary']}; "
        f"策略: {item['strategy_summary']}"
    )
    return item, ""


def format_pool_status(status):
    return {
        "watch": "突破观察",
        "tracking": "持续跟踪",
        "pullback": "回踩候选",
        "confirm": "确认信号",
        "removed": "已剔除",
    }.get(status, status or "-")


def get_breakout_pool_remove_reason(item, config):
    pool_cfg = config["breakout_pool"]
    breakout_price = to_float(item.get("breakout_price"))
    price = to_float(item.get("price"))
    if breakout_price > 0 and price < breakout_price * (1 - pool_cfg["break_below_breakout_pct"]):
        return f"跌破突破价{pool_cfg['break_below_breakout_pct'] * 100:.0f}%"
    if item.get("below_ema55_days", 0) >= pool_cfg["ema55_break_days"]:
        return f"连续{pool_cfg['ema55_break_days']}日跌破EMA55"
    if item.get("pool_age_days", 0) > pool_cfg["max_watch_days"] and item.get("pool_status") not in {"pullback", "confirm"}:
        return f"超过{pool_cfg['max_watch_days']}日未形成回踩确认"
    return ""


def get_breakout_pool_keep_rank(item):
    status_priority = {
        "confirm": 4,
        "pullback": 3,
        "watch": 2,
        "tracking": 1,
    }.get(item.get("pool_status"), 0)
    return (
        status_priority,
        to_float(item.get("score")),
        to_float(item.get("amount")),
        to_float(item.get("current_price") or item.get("price")),
    )


def prune_breakout_pool_overflow(config, analysis_date):
    pool_cfg = config["breakout_pool"]
    active = load_active_breakout_pool()
    overflow = len(active) - pool_cfg["max_size"]
    if overflow <= 0:
        return []

    ranked = sorted(active, key=get_breakout_pool_keep_rank, reverse=True)
    removed = []
    for item in ranked[pool_cfg["max_size"] :]:
        item = dict(item)
        item["remove_reason"] = "池满末位淘汰"
        removed.append(item)
        mark_breakout_pool_removed(item["code"], item["remove_reason"], analysis_date)
    return removed


def collect_breakout_entries_from_scan(scan_diagnostics, analysis_date, config):
    normalized = normalize_scan_diagnostics(scan_diagnostics)
    entries = {}
    max_breakout_day = config["breakout_pool"]["seed_breakout_days"]
    for item in normalized.get("candidates", []):
        if not item.get("anchored_breakout_signal"):
            continue
        breakout_day = to_int(item.get("anchored_breakout_day"))
        if breakout_day <= 0 or breakout_day > max_breakout_day:
            continue
        code = item.get("code")
        if not code:
            continue
        pool_item = dict(item)
        pool_item["pool_source"] = "sector_scan"
        pool_item["pool_status"] = "watch"
        pool_item["first_breakout_date"] = analysis_date
        pool_item["breakout_price"] = item.get("anchor_price")
        entries[code] = pool_item
    return sorted(entries.values(), key=lambda item: (item.get("score", 0), item.get("amount", 0)), reverse=True)


def seed_breakout_pool_from_market(config, analysis_date, use_cache=True, cache_only=False, force=False):
    pool_cfg = config["breakout_pool"]
    active_codes = {item["code"] for item in load_active_breakout_pool()}
    free_slots = max(0, pool_cfg["max_size"] - len(active_codes))
    add_limit = pool_cfg["max_size"] if force else free_slots
    if add_limit <= 0 or pool_cfg["full_market_scan_limit"] <= 0:
        return []

    market_stocks = get_all_market_stocks(use_cache=use_cache, cache_only=cache_only, snapshot_date=analysis_date)
    seeded = []
    scanned = 0
    for stock in market_stocks:
        if stock["code"] in active_codes:
            continue
        if pool_cfg["min_amount"] and to_float(stock.get("amount")) < pool_cfg["min_amount"]:
            continue
        scanned += 1
        if scanned > pool_cfg["full_market_scan_limit"]:
            break
        candidate, _ = evaluate_breakout_pool_stock(
            {**stock, "sector": "全市场"},
            config,
            analysis_date,
            use_cache=use_cache,
            cache_only=cache_only,
        )
        if not candidate:
            continue
        breakout_day = to_int(candidate.get("anchored_breakout_day"))
        if candidate.get("anchored_breakout_signal") and 0 < breakout_day <= pool_cfg["seed_breakout_days"]:
            candidate["pool_source"] = "full_market_seed"
            candidate["pool_status"] = "watch"
            seeded.append(candidate)
        if len(seeded) >= add_limit:
            break

    seeded.sort(
        key=lambda item: (
            item.get("score", 0),
            item.get("volume_price_score", 0),
            item.get("amount", 0),
        ),
        reverse=True,
    )
    saved = seeded[:add_limit]
    upsert_breakout_pool_entries(saved, analysis_date)
    return saved


def update_breakout_pool(config, analysis_date, scan_diagnostics, use_cache=True, cache_only=False):
    if not config.get("breakout_pool", {}).get("enabled", True):
        return {"enabled": False, "active_count": 0, "new_entries": [], "tracked": [], "removed": [], "stale": []}

    new_entries = collect_breakout_entries_from_scan(scan_diagnostics, analysis_date, config)
    saved_scan_codes = set(upsert_breakout_pool_entries(new_entries, analysis_date))
    active_before_seed = load_active_breakout_pool()
    seeded_entries = []
    force_seed = bool(config.get("_force_breakout_pool_seed", False))
    if force_seed or len(active_before_seed) < config["breakout_pool"]["min_size"]:
        seeded_entries = seed_breakout_pool_from_market(
            config,
            analysis_date,
            use_cache=use_cache,
            cache_only=cache_only,
            force=force_seed,
        )

    tracked = []
    removed = []
    stale = []
    active_pool = load_active_breakout_pool()[: config["breakout_pool"]["track_limit_per_run"]]
    for entry in active_pool:
        candidate, reason = evaluate_breakout_pool_stock(
            entry,
            config,
            analysis_date,
            use_cache=use_cache,
            cache_only=cache_only,
        )
        if not candidate:
            stale.append({**entry, "update_note": reason or "无法更新"})
            continue
        remove_reason = get_breakout_pool_remove_reason(candidate, config)
        if remove_reason:
            candidate["remove_reason"] = remove_reason
            removed.append(candidate)
            mark_breakout_pool_removed(candidate["code"], remove_reason, analysis_date)
            continue
        tracked.append(candidate)
        upsert_breakout_pool_entries([candidate], analysis_date)

    overflow_removed = prune_breakout_pool_overflow(config, analysis_date)
    removed.extend(overflow_removed)
    overflow_codes = {item.get("code") for item in overflow_removed}
    if overflow_codes:
        tracked = [item for item in tracked if item.get("code") not in overflow_codes]

    tracked.sort(
        key=lambda item: (
            item.get("pool_status") == "confirm",
            item.get("pool_status") == "pullback",
            item.get("score", 0),
            item.get("amount", 0),
        ),
        reverse=True,
    )
    return {
        "enabled": True,
        "active_count": len(load_active_breakout_pool()),
        "new_entries": [item for item in new_entries if item.get("code") in saved_scan_codes] + seeded_entries,
        "tracked": tracked,
        "removed": removed,
        "stale": stale,
        "overflow_removed": overflow_removed,
    }


def select_breakout_pool_picks(pool_state, config):
    if not pool_state or not pool_state.get("enabled"):
        return []
    selected = [
        item
        for item in pool_state.get("tracked", [])
        if item.get("pool_status") in {"confirm", "pullback"} or item.get("score", 0) >= config["min_stock_score"]
    ]
    selected.sort(
        key=lambda item: (
            item.get("pool_status") == "confirm",
            item.get("pool_status") == "pullback",
            item.get("score", 0),
            item.get("amount", 0),
        ),
        reverse=True,
    )
    result = []
    for item in selected[: config["breakout_pool"]["pick_limit"]]:
        item = dict(item)
        item["sector"] = item.get("sector") or "突破池"
        item["source_sectors"] = sorted(set(item.get("source_sectors", []) + [item["sector"]]))
        result.append(strip_internal_stock_fields(item))
    return result


def screen_stocks_in_sector(sector, config, analysis_date, use_cache=True, cache_only=False, scan_diagnostics=None):
    members = get_sector_members(
        sector["name"],
        sector["type"],
        use_cache=use_cache,
        cache_only=cache_only,
        snapshot_date=analysis_date,
    )
    if not members:
        return []

    light_history_days = get_light_history_days(config)
    full_history_days = get_full_history_days(config)
    ranked_members = rank_sector_members(members)
    bump_scan_count(scan_diagnostics, "stage_counts", "members_seen", len(ranked_members))
    eligible_members = []
    for item in ranked_members:
        if "ST" in item["name"]:
            record_member_reject(scan_diagnostics, sector, item, "ST股票")
            continue
        if item["price"] <= 0:
            record_member_reject(scan_diagnostics, sector, item, "价格无效")
            continue
        if item["pe"] <= -100:
            record_member_reject(scan_diagnostics, sector, item, "PE异常")
            continue
        eligible_members.append(item)

    valid_members = eligible_members[: config["sector_member_limit"]]
    for item in eligible_members[config["sector_member_limit"] :]:
        record_member_reject(scan_diagnostics, sector, item, "超出板块成员上限")

    leader_codes = build_sector_leaders(valid_members)
    candidates = []
    weights = config["score_weights"]["stock"]
    min_stock_score = config["min_stock_score"]

    for stock in valid_members:
        bump_scan_count(scan_diagnostics, "stage_counts", "members_scanned")
        df_hist = get_stock_history(
            stock["code"],
            end_date=analysis_date,
            days=light_history_days,
            use_cache=use_cache,
            cache_only=cache_only,
        )
        if df_hist.empty:
            record_member_reject(scan_diagnostics, sector, stock, "短历史缺失")
            continue
        tech = analyze_stock_tech(df_hist, config)
        if not tech:
            record_member_reject(scan_diagnostics, sector, stock, "技术指标不足")
            continue

        is_pullback, conditions = check_pullback_signal(tech, config)
        matched, failed = build_condition_summary(conditions)
        signal = get_signal_type(tech, is_pullback)
        risk_notes, risk_penalty, blocked = build_risk_assessment(stock, tech, config)
        if blocked and config["risk_filters"]["strict_mode"]:
            record_member_reject(scan_diagnostics, sector, stock, "风险过滤")
            continue

        is_sector_leader = stock["code"] in leader_codes
        score_components = build_initial_score_components(tech, is_pullback, risk_notes, risk_penalty, is_sector_leader, weights)
        score = score_components["base_score"]
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
                "score_components": score_components,
                "signal_breakdown": build_initial_signal_breakdown(tech),
                "basic_volume_pass": bool(tech["vol_ratio"] > config["light_breakout_setup"]["min_vol_ratio"] and tech["change_5d"] > 0),
                "breakout_setup_pass": has_light_breakout_setup(df_hist, tech, config),
                "volume_quality_score": 0.0,
                "volume_quality_grade": "D",
                "volume_quality_summary": "",
                "_history": df_hist,
                "summary": "",
            }
        )

    discover_breakouts_from_sector_candidates(
        candidates,
        config,
        analysis_date,
        use_cache=use_cache,
        cache_only=cache_only,
        scan_diagnostics=scan_diagnostics,
    )

    apply_sector_rps_metrics(candidates, config)
    light_passed = []
    for item in candidates:
        item["signal_breakdown"]["rps_pass"] = item.get("rps_strong_periods", 0) > 0
        item["signal_breakdown"]["breakout_setup_pass"] = item.get("breakout_setup_pass", False)
        update_signal_breakdown_summary(item)
        if (
            item["signal_breakdown"]["trend_pass"]
            and item["signal_breakdown"]["ema55_pass"]
            and (item["signal_breakdown"]["rps_pass"] or item["signal_breakdown"]["breakout_setup_pass"])
        ):
            light_passed.append(item)
            bump_scan_count(scan_diagnostics, "stage_counts", "light_passed")
        else:
            record_scan_candidate(scan_diagnostics, item, "rejected", get_light_reject_reason(item))

    scored_candidates = []
    volume_price_weight = config["anchored_breakout"]["volume_price_weight"]
    for item in light_passed:
        full_history = item["_history"]
        if full_history_days > light_history_days and not item.get("_full_history_loaded"):
            full_history = get_stock_history(
                item["code"],
                end_date=analysis_date,
                days=full_history_days,
                use_cache=use_cache,
                cache_only=cache_only,
            )
            if full_history.empty:
                record_scan_candidate(scan_diagnostics, item, "rejected", "长历史缺失")
                continue
            item["_history"] = full_history
            item["_full_history_loaded"] = True
            item["change_pct"] = round(calc_latest_change_pct(full_history), 2)

        score_components = item.get("score_components", {})
        apply_strategy_state_to_candidate(item, item["_history"], config)
        volume_quality = build_volume_quality_state(item, weights, volume_price_weight)
        item.update(volume_quality)
        item["signal_breakdown"]["volume_quality_pass"] = volume_quality["volume_quality_pass"]
        score_components["volume_quality_score"] = volume_quality["volume_quality_score"]
        if item.get("four_cycle_red"):
            score_components["rps_score"] = weights.get("rps_four_cycle_red", 0)
        elif item.get("three_cycle_red"):
            score_components["rps_score"] = weights.get("rps_three_cycle_red", 0)
        if item.get("big_order_alert"):
            score_components["strategy_score"] += weights.get("big_order_alert", 0)
        if item.get("macd_ignite"):
            score_components["strategy_score"] += weights.get("macd_ignite", 0)
        if item.get("anchored_breakout_signal"):
            score_components["breakout_score"] += weights.get("anchored_breakout", 0)
        if item.get("turnaround_signal"):
            score_components["strategy_score"] += weights.get("turnaround", 0)
        if item.get("breakout_signal"):
            score_components["breakout_score"] += weights.get("breakout", 0)
            item["signal"] = "趋势突破"
        elif item.get("turnaround_signal") and item["signal"] != "回踩买点":
            item["signal"] = "翻身计划"
        if item.get("anchored_breakout_signal") and item["signal"] == "观察":
            item["signal"] = "前高突破"
        elif item.get("macd_ignite") and item["signal"] == "观察":
            item["signal"] = "点火预备"
        finalize_score_components(score_components)
        item["score_components"] = score_components
        item["score"] = score_components["final_score"]
        item["signal_grade"] = get_signal_grade(item["score"], item["is_pullback"], item["risk_notes"], item["is_sector_leader"])
        update_signal_breakdown_summary(item)
        item["summary"] = (
            f"满足: {'、'.join(item['matched_conditions']) if item['matched_conditions'] else '无'}; "
            f"未满足: {'、'.join(item['failed_conditions']) if item['failed_conditions'] else '无'}; "
            f"风险: {'、'.join(item['risk_notes'])}; "
            f"强度: {item['rps_summary']}; "
            f"量价: {item['volume_quality_summary']}; "
            f"策略: {item['strategy_summary']}"
        )
        scored_candidates.append(item)

    candidates = []
    for item in scored_candidates:
        if item["score"] >= min_stock_score:
            candidates.append(item)
            bump_scan_count(scan_diagnostics, "stage_counts", "score_passed")
        else:
            record_scan_candidate(scan_diagnostics, item, "rejected", "评分低于阈值")
    candidates.sort(
        key=lambda item: (
            item["score"],
            get_signal_priority(item),
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
    selected_keys = {(item["sector"], item["code"]) for item in result}
    for rank, item in enumerate(candidates, 1):
        item["sector_rank"] = rank
        item["sector_rank_gap"] = max(0, rank - config["stocks_per_sector"])
        if (item["sector"], item["code"]) in selected_keys:
            record_scan_candidate(scan_diagnostics, item, "selected")
            bump_scan_count(scan_diagnostics, "stage_counts", "sector_selected")
        else:
            record_scan_candidate(scan_diagnostics, item, "not_selected", "未进入每板块名额")
    for item in result:
        strip_internal_stock_fields(item)
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
    entry_price_basis = config["backtest"]["entry_price_basis"]
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
        entry_index, entry_price = resolve_signal_entry(history, row.date, entry_price_basis)
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
            benchmark_index, benchmark_entry = resolve_signal_entry(benchmark_history, row.date, entry_price_basis)
            if benchmark_index is None:
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
        "entry_price_basis": entry_price_basis,
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


def merge_stock_pick(picks_by_code, stock):
    code = stock["code"]
    existing = picks_by_code.get(code)
    if not existing:
        stock["source_sectors"] = [stock["sector"]]
        stock["first_sector"] = stock["sector"]
        stock["best_sector"] = stock["sector"]
        stock["sector_overlap_count"] = 1
        picks_by_code[code] = stock
        return

    source_sectors = sorted(set(existing.get("source_sectors", [existing["sector"]]) + [stock["sector"]]))
    if stock.get("score", 0) > existing.get("score", 0):
        first_sector = existing.get("first_sector", existing["sector"])
        stock["source_sectors"] = source_sectors
        stock["first_sector"] = first_sector
        stock["best_sector"] = stock["sector"]
        stock["sector_overlap_count"] = len(source_sectors)
        picks_by_code[code] = stock
    else:
        existing["source_sectors"] = source_sectors
        existing["sector_overlap_count"] = len(source_sectors)
        existing["best_sector"] = existing.get("best_sector", existing["sector"])


def screen_classified_sectors(classified, config, analysis_date, use_cache=True, cache_only=False):
    picks_by_code = {}
    scan_diagnostics = build_scan_diagnostics(track_candidates=config.get("track_candidates", True))
    for sector in classified["hot"] + classified["potential"]:
        logger.info("screen sector: %s", sector["name"])
        picks = screen_stocks_in_sector(
            sector,
            config,
            analysis_date,
            use_cache=use_cache,
            cache_only=cache_only,
            scan_diagnostics=scan_diagnostics,
        )
        for stock in picks:
            merge_stock_pick(picks_by_code, stock)
        sleep_seconds = config.get("sector_scan_sleep_seconds", 0.0)
        if not cache_only and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    all_stock_picks = sorted(picks_by_code.values(), key=lambda item: item.get("score", 0), reverse=True)
    bump_scan_count(scan_diagnostics, "stage_counts", "global_selected", len(all_stock_picks))
    sector_selected = scan_diagnostics.get("stage_counts", {}).get("sector_selected", 0)
    deduplicated_count = max(0, sector_selected - len(all_stock_picks))
    if deduplicated_count:
        bump_scan_count(scan_diagnostics, "stage_counts", "deduplicated_count", deduplicated_count)
    return all_stock_picks, scan_diagnostics


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
        active_stats = {key: value for key, value in stats.items() if value}
        if active_stats:
            lines.append(f"- {name}: " + ", ".join(f"{key}={value}" for key, value in sorted(active_stats.items())))
    for item in RUNTIME_META["notes"]:
        lines.append(f"- note: {item}")
    return lines


def build_edge_candidates(scan_diagnostics, config):
    normalized = normalize_scan_diagnostics(scan_diagnostics)
    candidates = []
    min_edge_score = config["min_stock_score"] - config["edge_candidate_score_gap"]
    selected_codes = {
        item.get("code")
        for item in normalized["candidates"]
        if item.get("scan_status") == "selected"
    }
    for item in normalized["candidates"]:
        code = item.get("code")
        if not code or code in selected_codes:
            continue
        score = to_float(item.get("score"))
        breakdown = item.get("signal_breakdown", {}) or {}
        reason = item.get("reject_reason", "")
        near_score = score >= min_edge_score
        strong_breakout_incomplete = bool(
            breakdown.get("anchored_breakout_pass")
            and not breakdown.get("confirm_volume_price_pass")
        )
        not_selected = item.get("scan_status") == "not_selected"
        if near_score or strong_breakout_incomplete or not_selected:
            edge_item = dict(item)
            edge_reasons = []
            if not_selected:
                edge_reasons.append("未进入每板块名额")
            if strong_breakout_incomplete:
                edge_reasons.append("前高突破但确认不足")
            if near_score and score < config["min_stock_score"]:
                edge_reasons.append("接近评分阈值")
            if breakdown.get("breakout_setup_pass") and not breakdown.get("rps_pass"):
                edge_reasons.append("RPS未过但突破迹象通过")
            if reason and reason not in edge_reasons:
                edge_reasons.append(reason)
            edge_item["edge_reasons"] = edge_reasons or [reason or "边缘候选"]
            edge_item["edge_reason"] = "、".join(edge_item["edge_reasons"])
            edge_item["score_gap_to_min"] = round(max(0.0, config["min_stock_score"] - score), 2)
            candidates.append(edge_item)

    candidates.sort(
        key=lambda item: (
            to_float(item.get("score")),
            bool((item.get("signal_breakdown") or {}).get("anchored_breakout_pass")),
            to_float(item.get("amount")),
        ),
        reverse=True,
    )
    return candidates[: config["edge_candidates_limit"]]


def filter_edge_candidates(edge_candidates, args, config):
    filtered = list(edge_candidates)
    sector_filters = normalize_sector_filters(args.sector)
    if sector_filters:
        allowed = set(sector_filters)
        filtered = [item for item in filtered if item.get("sector") in allowed]
    if args.only_pullback:
        filtered = [item for item in filtered if item.get("is_pullback")]
    if args.min_score is not None:
        filtered = [item for item in filtered if item.get("score", 0) >= args.min_score]
    if args.exclude_high_pe:
        threshold = config["risk_filters"]["high_pe_warning"]
        filtered = [item for item in filtered if item.get("pe", 0) <= threshold]
    return filtered


def build_payload(
    date_str,
    classified,
    picks,
    streaks,
    backtest,
    comparison,
    alerts,
    filters,
    config,
    scan_diagnostics=None,
    edge_candidates=None,
    breakout_pool=None,
):
    normalized_scan = normalize_scan_diagnostics(scan_diagnostics)
    if edge_candidates is None:
        edge_candidates = build_edge_candidates(scan_diagnostics, config)
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
        "scan_stats": {
            "stage_counts": normalized_scan["stage_counts"],
            "reject_reasons": normalized_scan["reject_reasons"],
        },
        "stock_candidates": normalized_scan["candidates"] if config.get("export_candidates", True) else [],
        "edge_candidates": edge_candidates,
        "breakout_pool": breakout_pool or {},
    }


def generate_alert_report(date_str, alerts, comparison):
    lines = [f"告警摘要 | {date_str}", "=" * 40, ""]
    if comparison.get("previous_date"):
        lines.append(f"对比基准: {comparison['previous_date']}")
        lines.append("")
    for item in alerts:
        lines.append(f"- [{item['level']}] {item['title']}: {item['detail']}")
    return "\n".join(lines)


def format_prior_high_detail(stock):
    if not stock.get("anchor_found"):
        return ""
    anchor_date = stock.get("anchor_date") or "-"
    anchor_type = get_anchor_type_label(stock.get("anchor_type"))
    anchor_price = stock.get("anchor_price")
    price_text = f"{anchor_price:.2f}" if anchor_price is not None else "-"
    volume_text = stock.get("volume_price_summary") or "暂无突破量价确认"
    grade_text = f"，等级: {stock.get('anchored_breakout_grade')}" if stock.get("anchored_breakout_grade") else ""
    return f"前高: {anchor_date} {anchor_type}，突破价 {price_text}，量价: {volume_text}{grade_text}"


def format_edge_candidate_line(item):
    score = to_float(item.get("score"))
    reason = item.get("edge_reason") or item.get("reject_reason") or "-"
    signal = item.get("signal", "-")
    sector = item.get("sector", "-")
    score_gap = to_float(item.get("score_gap_to_min"))
    rank_gap = to_int(item.get("sector_rank_gap"))
    gap_text = f" | 差最低分 {score_gap:.1f}" if score_gap > 0 else ""
    rank_text = f" | 差板块名额 {rank_gap}名" if rank_gap > 0 else ""
    return f"- {item.get('name', '-')} {item.get('code', '-')} [{sector}] | {signal} | 评分 {score:.2f}{gap_text}{rank_text} | {reason}"


def format_score_components(item):
    components = item.get("score_components") or {}
    if not components:
        return ""
    return (
        f"基础 {components.get('base_score', 0):.1f} | "
        f"RPS {components.get('rps_score', 0):.1f} | "
        f"量价 {components.get('volume_quality_score', 0):.1f} | "
        f"突破 {components.get('breakout_score', 0):.1f} | "
        f"策略 {components.get('strategy_score', 0):.1f} | "
        f"风险扣分 {components.get('risk_penalty', 0):.1f}"
    )


def format_config_snapshot(config):
    setup_cfg = config["light_breakout_setup"]
    pool_cfg = config.get("breakout_pool", {})
    return (
        f"- 最低个股分 {config['min_stock_score']} | RPS阈值 {config['rps_threshold']} | "
        f"RPS最小样本 {config['rps_min_sample']} | 长历史 {config['history_window_days']}天 | "
        f"轻筛接近前高 {setup_cfg['near_high_pct']:.2f} | 量比阈值 {setup_cfg['min_vol_ratio']:.2f} | "
        f"突破池上限 {pool_cfg.get('max_size', 0)} | 跌破突破价剔除 {pool_cfg.get('break_below_breakout_pct', 0) * 100:.0f}%"
    )


def build_resonance_stocks(picks):
    return sorted(
        [item for item in picks if item.get("sector_overlap_count", 1) > 1],
        key=lambda item: (item.get("sector_overlap_count", 1), item.get("score", 0)),
        reverse=True,
    )


def generate_screening_diagnostics_report(date_str, scan_stats, edge_candidates):
    stage_counts = scan_stats.get("stage_counts", {})
    reject_reasons = scan_stats.get("reject_reasons", {})
    lines = [f"筛选诊断 | {date_str}", "=" * 40, ""]
    lines.append(
        f"- 成员 {stage_counts.get('members_seen', 0)} | 扫描 {stage_counts.get('members_scanned', 0)} | 突破发现 {stage_counts.get('breakout_discovery_found', 0)}/{stage_counts.get('breakout_discovery_checked', 0)} | 轻筛通过 {stage_counts.get('light_passed', 0)} | 评分通过 {stage_counts.get('score_passed', 0)} | 板块入选 {stage_counts.get('sector_selected', 0)} | 全局入选 {stage_counts.get('global_selected', 0)} | 去重 {stage_counts.get('deduplicated_count', 0)}"
    )
    if reject_reasons:
        lines.append("")
        lines.extend(["主要淘汰原因", "-" * 40])
        for reason, count in sorted(reject_reasons.items(), key=lambda item: item[1], reverse=True)[:12]:
            lines.append(f"- {reason}: {count}")
    if edge_candidates:
        lines.append("")
        lines.extend(["边缘候选", "-" * 40])
        for item in edge_candidates:
            lines.append(format_edge_candidate_line(item))
    return "\n".join(lines)


def generate_report(
    date_str,
    classified,
    picks,
    streaks,
    backtest,
    comparison,
    alerts,
    config,
    alerts_only=False,
    scan_stats=None,
    edge_candidates=None,
    breakout_pool=None,
):
    if alerts_only:
        return generate_alert_report(date_str, alerts, comparison)

    hot_count = len(classified["hot"])
    potential_count = len(classified["potential"])
    lines = [f"板块跟踪日报 | {date_str}", "=" * 40, "", "数据状态", "-" * 40]
    lines.extend(format_source_lines() or ["- 无"])
    lines.append("")

    lines.extend(["配置快照", "-" * 40])
    lines.append(format_config_snapshot(config))
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
            score_detail = format_score_components(stock)
            if score_detail:
                lines.append(f"  分数 {score_detail}")
            if stock.get("rps_summary"):
                lines.append(f"  RPS {stock['rps_summary']}")
            if stock.get("strategy_summary") and stock["strategy_summary"] != "无":
                lines.append(f"  策略 {stock['strategy_summary']}")
            if stock.get("sector_overlap_count", 1) > 1:
                lines.append(f"  板块共振 {'、'.join(stock.get('source_sectors', []))}")
            if stock.get("stop_profit_line") is not None:
                lines.append(f"  参考止盈线 {stock['stop_profit_line']:.2f}")
            if stock.get("anchored_breakout_signal"):
                lines.append(
                    f"  锚点 {stock['anchor_price']:.2f} | 类型 {get_anchor_type_label(stock.get('anchor_type'))} | 触碰 {stock.get('anchor_zone_touches', 0)} 次 | 突破日距今 {stock['anchored_breakout_day']} 天 | 回撤 {stock['anchor_pullback_pct']:.1f}%"
                )
                if stock.get("anchored_breakout_grade"):
                    lines.append(f"  前高等级 {stock['anchored_breakout_grade']}")
                if stock.get("volume_price_summary"):
                    lines.append(f"  量价 {stock['volume_price_summary']}")
            if stock.get("signal_breakdown_summary"):
                lines.append(f"  信号拆解 {stock['signal_breakdown_summary']}")
            if stock["hot_topic"]:
                lines.append(f"  热点 {stock['hot_topic']}")
            lines.append("")
    else:
        lines.extend(["- 今日暂无回踩买点个股", ""])

    resonance_stocks = build_resonance_stocks(picks)
    if resonance_stocks:
        lines.extend(["共振股票", "-" * 40])
        for stock in resonance_stocks[:5]:
            lines.append(
                f"- {stock['name']} {stock['code']} | 评分 {stock.get('score', 0):.2f} | 板块 {'、'.join(stock.get('source_sectors', []))}"
            )
        lines.append("")

    if breakout_pool and breakout_pool.get("enabled"):
        tracked = breakout_pool.get("tracked", [])
        new_entries = breakout_pool.get("new_entries", [])
        removed = breakout_pool.get("removed", [])
        stale = breakout_pool.get("stale", [])
        confirm_count = sum(1 for item in tracked if item.get("pool_status") == "confirm")
        pullback_count = sum(1 for item in tracked if item.get("pool_status") == "pullback")
        lines.extend(["突破股票池", "-" * 40])
        lines.append(
            f"- 当前池 {breakout_pool.get('active_count', 0)} 只 | 新入池 {len(new_entries)} | 回踩候选 {pullback_count} | 确认信号 {confirm_count} | 今日剔除 {len(removed)} | 未更新 {len(stale)}"
        )
        for stock in tracked[: config["breakout_pool"]["pick_limit"]]:
            status = format_pool_status(stock.get("pool_status"))
            breakout_price = to_float(stock.get("breakout_price"))
            breakout_text = f"突破价 {breakout_price:.2f}" if breakout_price > 0 else "突破价 -"
            lines.append(
                f"- {stock['name']} {stock['code']} [{stock.get('sector', '突破池')}] | {status} | 评分 {stock.get('score', 0):.2f} | {breakout_text} | 价 {stock.get('price', 0):.2f}"
            )
            if stock.get("volume_price_summary"):
                lines.append(f"  量价 {stock['volume_price_summary']}")
            if stock.get("signal_breakdown_summary"):
                lines.append(f"  信号拆解 {stock['signal_breakdown_summary']}")
        if removed:
            lines.append("- 剔除: " + "；".join(f"{item.get('name', '-')}{item.get('code', '')}({item.get('remove_reason', '-')})" for item in removed[:5]))
        lines.append("")

    lines.extend([f"热门板块 TOP{hot_count}", "-" * 40])
    for index, sector in enumerate(classified["hot"], 1):
        flow = f"{sector.get('fund_flow', 0) / 1e8:.1f}亿" if sector.get("fund_flow", 0) > 0 else "-"
        lines.append(f"{index}. {sector['name']} | 评分 {sector['score']} | 涨幅 {sector['change_pct']:+.2f}% | 资金 {flow}")
        for stock in [item for item in picks if item["sector"] == sector["name"]]:
            leader_tag = " | 核心票" if stock.get("is_sector_leader") else ""
            lines.append(
                f"   - {stock['name']} {stock['code']} | {stock['signal']} | 等级 {stock.get('signal_grade', '-')}{leader_tag} | {stock['summary']}"
            )
            prior_high_detail = format_prior_high_detail(stock)
            if prior_high_detail:
                lines.append(f"     {prior_high_detail}")
            if stock.get("signal_breakdown_summary"):
                lines.append(f"     信号拆解: {stock['signal_breakdown_summary']}")
            score_detail = format_score_components(stock)
            if score_detail:
                lines.append(f"     分数: {score_detail}")
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
            prior_high_detail = format_prior_high_detail(stock)
            if prior_high_detail:
                lines.append(f"     {prior_high_detail}")
            if stock.get("signal_breakdown_summary"):
                lines.append(f"     信号拆解: {stock['signal_breakdown_summary']}")
            score_detail = format_score_components(stock)
            if score_detail:
                lines.append(f"     分数: {score_detail}")
        lines.append("")

    lines.extend(["板块轮动观察", "-" * 40])
    if streaks:
        for item in streaks[:5]:
            lines.append(f"- {item['name']} 连续上榜 {item['days']} 天 | 当前分类 {item['current_category']} | 历史分类 {item['categories']}")
    else:
        lines.append("- 暂无足够历史数据")
    lines.append("")

    if scan_stats:
        stage_counts = scan_stats.get("stage_counts", {})
        reject_reasons = scan_stats.get("reject_reasons", {})
        lines.extend(["筛选诊断", "-" * 40])
        lines.append(
            f"- 成员 {stage_counts.get('members_seen', 0)} | 扫描 {stage_counts.get('members_scanned', 0)} | 突破发现 {stage_counts.get('breakout_discovery_found', 0)}/{stage_counts.get('breakout_discovery_checked', 0)} | 轻筛通过 {stage_counts.get('light_passed', 0)} | 评分通过 {stage_counts.get('score_passed', 0)} | 板块入选 {stage_counts.get('sector_selected', 0)} | 全局入选 {stage_counts.get('global_selected', 0)} | 去重 {stage_counts.get('deduplicated_count', 0)}"
        )
        if reject_reasons:
            top_reasons = sorted(reject_reasons.items(), key=lambda item: item[1], reverse=True)[:8]
            lines.append("- 淘汰原因: " + "；".join(f"{reason} {count}" for reason, count in top_reasons))
        else:
            lines.append("- 暂无淘汰原因统计")
        lines.append("")

    if edge_candidates:
        lines.extend(["边缘候选", "-" * 40])
        for item in edge_candidates[: config["edge_candidates_limit"]]:
            lines.append(format_edge_candidate_line(item))
            if item.get("signal_breakdown_summary"):
                lines.append(f"  信号拆解: {item['signal_breakdown_summary']}")
        lines.append("")

    lines.extend(["回测摘要", "-" * 40])
    if backtest and backtest.get("metrics"):
        lines.append(f"- 评估窗口: 最近 {backtest['sample_days']} 天")
        entry_basis_label = "次日开盘" if backtest.get("entry_price_basis") == "next_open" else "信号日收盘"
        lines.append(f"- 买入价口径: {entry_basis_label}")
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
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2, default=json_default)


def write_report_files(report_path, json_path, report, payload):
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as file:
        file.write(report)
    write_json_report(json_path, payload)


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
    RUNTIME_META["runtime_memory_cache_enabled"] = config.get("runtime_memory_cache", True)
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
    config["_force_breakout_pool_seed"] = bool(getattr(args, "update_breakout_pool", False))
    if config["_force_breakout_pool_seed"]:
        note("已启用主动突破池更新：本轮会全市场扫描近期突破并尝试入池。")
    if args.diagnose_screening:
        args.skip_backtest = True
        note("当前仅输出筛选诊断，不写入数据库。")

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
    login_needed = not cache_only and bool(classified["hot"] or classified["potential"] or not args.skip_backtest)

    if login_needed:
        login_result = bs.login()
        if getattr(login_result, "error_code", "0") != "0":
            logger.error("BaoStock login failed: %s", getattr(login_result, "error_msg", "unknown"))
            return None

    backtest = None
    breakout_pool_state = None
    try:
        all_stock_picks, scan_diagnostics = screen_classified_sectors(
            classified,
            config,
            analysis_date,
            use_cache=use_cache,
            cache_only=cache_only,
        )
        breakout_pool_state = update_breakout_pool(
            config,
            analysis_date,
            scan_diagnostics,
            use_cache=use_cache,
            cache_only=cache_only,
        )
        if breakout_pool_state and breakout_pool_state.get("enabled"):
            picks_by_code = {item["code"]: item for item in all_stock_picks}
            for pool_pick in select_breakout_pool_picks(breakout_pool_state, config):
                merge_stock_pick(picks_by_code, pool_pick)
            all_stock_picks = sorted(picks_by_code.values(), key=lambda item: item.get("score", 0), reverse=True)
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

    allow_persist = (analysis_date == today or using_historical_snapshot) and not args.diagnose_screening
    if allow_persist:
        save_daily_results(analysis_date, classified, all_stock_picks)
        streaks = get_sector_streak(analysis_date)
    else:
        note("未使用历史快照，本次结果不写入数据库，避免污染历史记录。")
        streaks = []

    visible_classified = filter_classified_sectors(classified, sector_filters)
    visible_picks = apply_pick_filters(all_stock_picks, args, config)
    normalized_scan = normalize_scan_diagnostics(scan_diagnostics)
    edge_candidates = filter_edge_candidates(build_edge_candidates(scan_diagnostics, config), args, config)
    comparison = build_day_over_day_comparison(analysis_date, visible_classified, visible_picks, sector_filters=sector_filters)
    alerts = build_alerts(visible_classified, visible_picks, comparison, config)
    filters = {
        "sector": sector_filters,
        "only_pullback": args.only_pullback,
        "min_score": args.min_score,
        "exclude_high_pe": args.exclude_high_pe,
        "alerts_only": args.alerts_only,
        "diagnose_screening": args.diagnose_screening,
        "no_output_files": getattr(args, "no_output_files", False),
        "update_breakout_pool": getattr(args, "update_breakout_pool", False),
    }

    scan_stats = {
        "stage_counts": normalized_scan["stage_counts"],
        "reject_reasons": normalized_scan["reject_reasons"],
    }
    if args.diagnose_screening:
        report = generate_screening_diagnostics_report(analysis_date, scan_stats, edge_candidates)
    else:
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
            scan_stats=scan_stats,
            edge_candidates=edge_candidates,
            breakout_pool=breakout_pool_state,
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
        scan_diagnostics=scan_diagnostics,
        edge_candidates=edge_candidates,
        breakout_pool=breakout_pool_state,
    )

    if config.get("write_output_files", True) and not getattr(args, "no_output_files", False):
        report_path = REPORT_DIR / f"{analysis_date}.md"
        json_path = Path(args.output_json) if args.output_json else REPORT_DIR / f"{analysis_date}.json"
        write_report_files(report_path, json_path, report, payload)
    else:
        note("本次运行未写入报告文件。")

    print("===REPORT_START===")
    print(report)
    print("===REPORT_END===")
    return payload


if __name__ == "__main__":
    run_analysis(parse_args())
