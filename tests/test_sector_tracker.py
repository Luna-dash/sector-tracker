import sqlite3
import tempfile
import types
import unittest
from pathlib import Path

import pandas as pd

import sector_tracker as st


class SectorTrackerUnitTests(unittest.TestCase):
    def test_deep_merge_keeps_nested_defaults(self):
        merged = st.deep_merge(
            {"a": 1, "nested": {"x": 1, "y": 2}},
            {"nested": {"y": 9, "z": 3}, "b": 2},
        )
        self.assertEqual(merged["a"], 1)
        self.assertEqual(merged["b"], 2)
        self.assertEqual(merged["nested"], {"x": 1, "y": 9, "z": 3})

    def test_check_pullback_signal_uses_config_threshold(self):
        config = st.deep_merge(st.DEFAULT_CONFIG, {"pullback_threshold": 0.02})
        tech = {
            "ema_bullish": True,
            "ema13_rising": True,
            "ema_dist_pct": 0.015,
            "vol_ratio": 0.9,
            "change_5d": 1.5,
        }
        is_pullback, conditions = st.check_pullback_signal(tech, config)
        self.assertTrue(is_pullback)
        self.assertTrue(conditions["near_ema"])

    def test_build_risk_assessment_returns_penalty_and_block(self):
        config = st.deep_merge(
            st.DEFAULT_CONFIG,
            {
                "risk_filters": {
                    "high_pe_warning": 80,
                    "exclude_negative_pe": True,
                    "max_change_5d": 10.0,
                    "max_vol_ratio": 2.0,
                    "max_volatility_10d": 6.0,
                }
            },
        )
        stock = {"pe": -12}
        tech = {"change_5d": 12.5, "vol_ratio": 2.4, "volatility_10d": 7.2}
        notes, penalty, blocked = st.build_risk_assessment(stock, tech, config)
        self.assertTrue(blocked)
        self.assertGreaterEqual(penalty, 18)
        self.assertTrue(any("12.5" in note for note in notes))

    def test_apply_sector_concept_boost_uses_member_overlap(self):
        config = st.load_config()
        config["hot_sectors_count"] = 1
        config["potential_sectors_count"] = 2
        config["concept_scan_limit"] = 3
        config["concept_match_limit"] = 3
        config["concept_overlap_min_count"] = 2
        config["concept_overlap_min_ratio"] = 0.5
        industry = [
            {
                "name": "Cloud Service",
                "type": "industry",
                "change_pct": 6.2,
                "up_count": 18,
                "down_count": 2,
                "turnover": 4.1,
                "lead_change": 7.1,
            },
            {
                "name": "Robotics",
                "type": "industry",
                "change_pct": 2.3,
                "up_count": 15,
                "down_count": 5,
                "turnover": 2.2,
                "lead_change": 2.8,
            },
        ]
        concepts = [
            {"name": "AI Robotics", "change_pct": 3.5},
            {"name": "Low Altitude", "change_pct": 1.2},
        ]
        flows = {
            "Cloud Service": {"today_flow": 1.2e9},
            "Robotics": {"today_flow": 2e8},
        }
        classified = st.classify_sectors(industry, concepts, flows, config)

        original_get_sector_members = st.get_sector_members
        try:
            mapping = {
                ("concept", "AI Robotics"): [{"code": "000001"}, {"code": "000002"}, {"code": "000099"}],
                ("industry", "Robotics"): [{"code": "000001"}, {"code": "000002"}, {"code": "000003"}],
            }
            st.get_sector_members = lambda name, sector_type="industry", **kwargs: mapping.get((sector_type, name), [])
            _, _, concept_members = st.build_hot_concept_member_map(concepts, config, "2026-04-14")
            classified = st.apply_sector_concept_boost(classified, concept_members, config, "2026-04-14")
        finally:
            st.get_sector_members = original_get_sector_members

        self.assertEqual(len(classified["hot"]), 1)
        self.assertEqual(classified["hot"][0]["name"], "Cloud Service")
        self.assertEqual(len(classified["potential"]), 1)
        self.assertTrue(classified["potential"][0]["concept_boost"])
        self.assertEqual(classified["potential"][0]["related_concepts"], ["AI Robotics"])

    def test_apply_pick_filters_respects_flags(self):
        args = types.SimpleNamespace(
            sector=["Robotics"],
            only_pullback=True,
            min_score=70,
            exclude_high_pe=True,
        )
        config = st.load_config()
        picks = [
            {"sector": "Robotics", "is_pullback": True, "score": 82, "pe": 40},
            {"sector": "Robotics", "is_pullback": False, "score": 90, "pe": 40},
            {"sector": "Cloud Service", "is_pullback": True, "score": 88, "pe": 40},
            {"sector": "Robotics", "is_pullback": True, "score": 88, "pe": 150},
        ]
        filtered = st.apply_pick_filters(picks, args, config)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(filtered[0]["sector"], "Robotics")
        self.assertEqual(filtered[0]["score"], 82)

    def test_build_day_over_day_comparison_detects_changes(self):
        original_prev = st.get_previous_analysis_date
        original_load = st.load_saved_analysis
        try:
            pullback_signal = st.get_signal_type(
                {"ema_bullish": True, "ema13_rising": True, "ema_short": 10, "ema_long": 9},
                True,
            )
            st.get_previous_analysis_date = lambda _: "2026-04-13"
            st.load_saved_analysis = lambda _: {
                "classified": {
                    "hot": [{"name": "Cloud Service"}],
                    "potential": [{"name": "Low Altitude"}],
                    "hot_concepts": [],
                },
                "stock_picks": [
                    {"code": "000001", "name": "Old Pick", "sector": "Cloud Service", "signal": pullback_signal},
                ],
            }
            comparison = st.build_day_over_day_comparison(
                "2026-04-14",
                {"hot": [{"name": "Robotics"}], "potential": [{"name": "Low Altitude"}], "hot_concepts": []},
                [{"code": "000002", "name": "New Pick", "sector": "Robotics", "is_pullback": True}],
            )
            self.assertEqual(comparison["previous_date"], "2026-04-13")
            self.assertEqual(comparison["new_hot"], ["Robotics"])
            self.assertEqual(comparison["dropped_hot"], ["Cloud Service"])
            self.assertEqual(comparison["new_pullbacks"], ["New Pick(Robotics)"])
        finally:
            st.get_previous_analysis_date = original_prev
            st.load_saved_analysis = original_load

    def test_build_alerts_collects_core_items(self):
        config = st.load_config()
        comparison = {
            "new_hot": ["Robotics"],
            "dropped_hot": [],
            "new_potential": [],
            "dropped_potential": [],
            "new_pullbacks": ["New Pick(Robotics)"],
            "removed_pullbacks": [],
        }
        picks = [
            {"name": "New Pick", "sector": "Robotics", "signal_grade": "A", "pe": 50},
            {"name": "Expensive", "sector": "Robotics", "signal_grade": "C", "pe": 150},
        ]
        alerts = st.build_alerts({"hot": [], "potential": [], "hot_concepts": []}, picks, comparison, config)
        self.assertGreaterEqual(len(alerts), 4)
        self.assertIn("info", {item["level"] for item in alerts})
        self.assertIn("signal", {item["level"] for item in alerts})
        self.assertIn("warn", {item["level"] for item in alerts})

    def test_apply_sector_rps_metrics_marks_cycle_red(self):
        candidates = [
            {"code": "000001", "change_10d": 3, "change_20d": 5, "change_50d": 7, "change_120d": 9, "change_250d": 11},
            {"code": "000002", "change_10d": 10, "change_20d": 12, "change_50d": 15, "change_120d": 18, "change_250d": 20},
            {"code": "000003", "change_10d": 1, "change_20d": 2, "change_50d": 3, "change_120d": 4, "change_250d": 5},
        ]
        config = st.deep_merge(st.DEFAULT_CONFIG, {"rps_threshold": 90})
        st.apply_sector_rps_metrics(candidates, config)

        strongest = next(item for item in candidates if item["code"] == "000002")
        self.assertEqual(strongest["rps10"], 100.0)
        self.assertTrue(strongest["four_cycle_red"])
        self.assertIn("四周期红", strongest["rps_summary"])

    def test_screen_stocks_in_sector_uses_ranked_pool_and_min_score(self):
        original_get_sector_members = st.get_sector_members
        original_get_stock_history = st.get_stock_history
        original_analyze_stock_tech = st.analyze_stock_tech
        original_build_daily_strategy_state = st.build_daily_strategy_state
        try:
            config = st.deep_merge(
                st.DEFAULT_CONFIG,
                {
                    "sector_member_limit": 2,
                    "stocks_per_sector": 2,
                    "min_stock_score": 60,
                },
            )
            sector = {"name": "Robotics", "type": "industry"}
            st.get_sector_members = lambda *args, **kwargs: [
                {"code": "000001", "name": "LowAmountHighScore", "price": 10, "pe": 20, "amount": 10, "turnover": 1.0, "change_pct": 3.0},
                {"code": "000002", "name": "HighAmountQualified", "price": 10, "pe": 20, "amount": 300, "turnover": 5.0, "change_pct": 2.0},
                {"code": "000003", "name": "SecondAmountLowScore", "price": 10, "pe": 20, "amount": 200, "turnover": 4.0, "change_pct": 1.0},
            ]

            def fake_history(code, **kwargs):
                return pd.DataFrame(
                    [
                        {"date": "2026-04-13", "close": 10.0, "volume": 100.0, "code": code},
                        {"date": "2026-04-14", "close": 10.5, "volume": 120.0, "code": code},
                    ]
                )

            def fake_analyze(df, config):
                code = df["code"].iloc[0]
                mapping = {
                    "000001": {
                        "price": 10.5,
                        "ema_short": 10.2,
                        "ema_long": 10.0,
                        "ema13": 10.2,
                        "ema55": 10.0,
                        "ema_dist_pct": 0.01,
                        "ema13_dist_pct": 0.01,
                        "ema_bullish": True,
                        "ema13_rising": True,
                        "ma_bullish": True,
                        "vol_ratio": 1.3,
                        "volatility_10d": 2.0,
                        "change_5d": 2.5,
                        "change_10d": 4.0,
                    },
                    "000002": {
                        "price": 10.5,
                        "ema_short": 10.2,
                        "ema_long": 10.0,
                        "ema13": 10.2,
                        "ema55": 10.0,
                        "ema_dist_pct": 0.01,
                        "ema13_dist_pct": 0.01,
                        "ema_bullish": True,
                        "ema13_rising": True,
                        "ma_bullish": True,
                        "vol_ratio": 1.3,
                        "volatility_10d": 2.0,
                        "change_5d": 2.5,
                        "change_10d": 4.0,
                    },
                    "000003": {
                        "price": 10.5,
                        "ema_short": 10.0,
                        "ema_long": 10.1,
                        "ema13": 10.0,
                        "ema55": 10.1,
                        "ema_dist_pct": 0.05,
                        "ema13_dist_pct": 0.05,
                        "ema_bullish": False,
                        "ema13_rising": False,
                        "ma_bullish": False,
                        "vol_ratio": 0.8,
                        "volatility_10d": 2.0,
                        "change_5d": -1.0,
                        "change_10d": -2.0,
                    },
                }
                return mapping[code]

            def fake_strategy(df, stock):
                if stock["code"] == "000002":
                    return {
                        "ddx": 1.2,
                        "ddx_positive_days_5": 3,
                        "big_order_alert": True,
                        "macd_ignite": False,
                        "turnaround_signal": False,
                        "breakout_signal": True,
                        "stop_profit_line": 10.1,
                        "strategy_tags": ["趋势突破", "资金预警"],
                        "strategy_summary": "趋势突破、资金预警",
                    }
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

            st.get_stock_history = fake_history
            st.analyze_stock_tech = fake_analyze
            st.build_daily_strategy_state = fake_strategy

            picks = st.screen_stocks_in_sector(sector, config, "2026-04-14")

            self.assertEqual([item["code"] for item in picks], ["000002"])
            self.assertEqual(picks[0]["signal"], "趋势突破")
            self.assertIn("趋势突破", picks[0]["strategy_summary"])
        finally:
            st.get_sector_members = original_get_sector_members
            st.get_stock_history = original_get_stock_history
            st.analyze_stock_tech = original_analyze_stock_tech
            st.build_daily_strategy_state = original_build_daily_strategy_state

    def test_evaluate_saved_picks_uses_signal_close_entry_price(self):
        original_db_path = st.DB_PATH
        original_get_stock_history = st.get_stock_history
        original_get_benchmark_history = st.get_benchmark_history
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                st.DB_PATH = Path(temp_dir) / "sector_history.db"
                conn = sqlite3.connect(str(st.DB_PATH))
                conn.execute(
                    """
                    CREATE TABLE daily_stock_picks (
                        date TEXT NOT NULL,
                        stock_code TEXT NOT NULL,
                        stock_name TEXT NOT NULL,
                        price REAL,
                        signal TEXT
                    )
                    """
                )
                conn.execute(
                    "INSERT INTO daily_stock_picks (date, stock_code, stock_name, price, signal) VALUES (?, ?, ?, ?, ?)",
                    ("2026-04-10", "000001", "Alpha", 999.0, "回踩买点"),
                )
                conn.commit()
                conn.close()

                stock_history = pd.DataFrame(
                    [
                        {"date": "2026-04-10", "open": 9.8, "close": 10.0},
                        {"date": "2026-04-11", "open": 10.2, "close": 11.0},
                    ]
                )
                benchmark_history = pd.DataFrame(
                    [
                        {"date": "2026-04-10", "close": 100.0},
                        {"date": "2026-04-11", "close": 101.0},
                    ]
                )
                st.get_stock_history = lambda *args, **kwargs: stock_history.copy()
                st.get_benchmark_history = lambda *args, **kwargs: benchmark_history.copy()

                config = st.deep_merge(
                    st.DEFAULT_CONFIG,
                    {"backtest": {"lookback_days": 5, "holding_days": [1], "min_samples": 1}},
                )
                summary = st.evaluate_saved_picks("2026-04-11", config)

                self.assertEqual(summary["entry_price_basis"], "signal_close")
                self.assertEqual(summary["metrics"][1]["avg_return"], 10.0)
                self.assertEqual(summary["by_signal"]["回踩买点"][1]["avg_return"], 10.0)
                benchmark_name = next(iter(summary["benchmarks"]))
                self.assertEqual(summary["benchmarks"][benchmark_name][1]["excess_return"], 9.0)
        finally:
            st.DB_PATH = original_db_path
            st.get_stock_history = original_get_stock_history
            st.get_benchmark_history = original_get_benchmark_history

    def test_run_analysis_requires_historical_snapshot(self):
        original_ensure_dependencies = st.ensure_dependencies
        original_load_config = st.load_config
        original_init_db = st.init_db
        original_get_proxy_environment = st.get_proxy_environment
        original_load_market_snapshot = st.load_market_snapshot
        original_get_industry_sectors = st.get_industry_sectors
        try:
            st.ensure_dependencies = lambda: None
            st.load_config = lambda: st.deep_merge(st.DEFAULT_CONFIG, {})
            st.init_db = lambda: None
            st.get_proxy_environment = lambda: {}
            st.load_market_snapshot = lambda _: None
            st.get_industry_sectors = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected live fetch"))

            args = types.SimpleNamespace(
                date="2026-04-01",
                no_cache=False,
                cache_only=False,
                offline=False,
                diagnose_network=False,
                sector_limit=None,
                sector=None,
                only_pullback=False,
                min_score=None,
                exclude_high_pe=False,
                alerts_only=False,
                output_json=None,
                skip_backtest=True,
            )
            self.assertIsNone(st.run_analysis(args))
        finally:
            st.ensure_dependencies = original_ensure_dependencies
            st.load_config = original_load_config
            st.init_db = original_init_db
            st.get_proxy_environment = original_get_proxy_environment
            st.load_market_snapshot = original_load_market_snapshot
            st.get_industry_sectors = original_get_industry_sectors

    def test_generate_report_contains_core_sections(self):
        config = st.load_config()
        classified = {
            "hot": [{"name": "Robotics", "score": 90, "change_pct": 2.3, "fund_flow": 2e8}],
            "potential": [{"name": "Automation", "score": 78, "change_pct": 1.8, "fund_flow": 1.2e8, "related_concepts": ["AI Robotics"]}],
            "hot_concepts": [],
        }
        picks = [
            {
                "name": "Alpha",
                "code": "000001",
                "sector": "Robotics",
                "signal": "Trend",
                "summary": "ok",
                "price": 10.0,
                "ema13_dist_pct": 1.2,
                "vol_ratio": 1.1,
                "is_pullback": True,
                "signal_grade": "A",
                "hot_topic": "",
                "rps_summary": "RPS10/20/50/120/250=95/96/97/98/99 | 四周期红",
                "strategy_summary": "趋势突破、资金预警",
                "stop_profit_line": 9.8,
                "pe": 40,
            }
        ]
        streaks = []
        backtest = {
            "sample_days": 30,
            "entry_price_basis": "signal_close",
            "metrics": {1: {"samples": 5, "avg_return": 1.2, "win_rate": 60.0}},
            "by_signal": {"回踩买点": {1: {"samples": 3, "avg_return": 1.8, "win_rate": 66.67}}},
            "benchmarks": {"沪深300": {1: {"avg_return": 0.5, "excess_return": 0.7}}},
        }
        comparison = {
            "previous_date": "2026-04-13",
            "new_hot": ["Robotics"],
            "dropped_hot": [],
            "new_potential": [],
            "dropped_potential": [],
            "new_pullbacks": [],
            "removed_pullbacks": [],
        }
        alerts = [{"level": "info", "title": "Hot Sector", "detail": "Robotics"}]
        report = st.generate_report("2026-04-14", classified, picks, streaks, backtest, comparison, alerts, config)
        self.assertIn("2026-04-14", report)
        self.assertIn("2026-04-13", report)
        self.assertIn("Alpha", report)
        self.assertIn("Robotics", report)
        self.assertIn("关联概念", report)
        self.assertIn("信号 回踩买点", report)
        self.assertIn("四周期红", report)
        self.assertIn("参考止盈线", report)


if __name__ == "__main__":
    unittest.main()
