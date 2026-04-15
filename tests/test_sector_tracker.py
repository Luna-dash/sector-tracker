import json
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

    def test_runtime_cache_returns_independent_dataframe_copy(self):
        st.RUNTIME_CACHE.clear()
        df = pd.DataFrame([{"close": 10.0}])
        st.runtime_cache_set("history", "000001", df)
        cached = st.runtime_cache_get("history", "000001")
        cached.loc[0, "close"] = 99.0
        cached_again = st.runtime_cache_get("history", "000001")

        self.assertEqual(cached_again.loc[0, "close"], 10.0)

    def test_write_report_files_creates_parent_directories(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            base = Path(temp_dir)
            report_path = base / "nested" / "report.md"
            json_path = base / "nested" / "payload.json"
            st.write_report_files(report_path, json_path, "hello", {"ok": True})

            self.assertEqual(report_path.read_text(encoding="utf-8"), "hello")
            self.assertEqual(json.loads(json_path.read_text(encoding="utf-8"))["ok"], True)

    def test_check_pullback_signal_uses_config_threshold(self):
        config = st.deep_merge(st.DEFAULT_CONFIG, {"pullback_threshold": 0.02})
        tech = {
            "ema_bullish": True,
            "ema13_rising": True,
            "ema55_rising": True,
            "ema_dist_pct": 0.015,
            "vol_ratio": 0.9,
            "change_5d": 1.5,
        }
        is_pullback, conditions = st.check_pullback_signal(tech, config)
        self.assertTrue(is_pullback)
        self.assertTrue(conditions["near_ema"])

    def test_check_pullback_signal_requires_ema55_rising(self):
        config = st.deep_merge(st.DEFAULT_CONFIG, {"pullback_threshold": 0.02})
        tech = {
            "ema_bullish": True,
            "ema13_rising": True,
            "ema55_rising": False,
            "ema_dist_pct": 0.01,
            "vol_ratio": 0.9,
            "change_5d": 1.5,
        }
        is_pullback, conditions = st.check_pullback_signal(tech, config)
        self.assertFalse(is_pullback)
        self.assertFalse(conditions["ema55_rising"])

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

    def test_classify_sectors_excludes_data_boards_and_uses_config_counts(self):
        config = st.deep_merge(
            st.DEFAULT_CONFIG,
            {
                "hot_sectors_count": 8,
                "potential_sectors_count": 8,
                "excluded_sector_keywords": ["连板", "二板"],
            },
        )
        industries = []
        for index in range(18):
            industries.append(
                {
                    "name": f"行业{index}",
                    "change_pct": 2.0 - index * 0.05,
                    "up_count": 8,
                    "down_count": 2,
                    "turnover": 2.0,
                    "lead_change": 3.0,
                    "type": "industry",
                }
            )
        industries.insert(
            0,
            {
                "name": "昨日连板",
                "change_pct": 9.0,
                "up_count": 10,
                "down_count": 0,
                "turnover": 8.0,
                "lead_change": 10.0,
                "type": "industry",
            },
        )

        classified = st.classify_sectors(industries, [{"name": "二板", "change_pct": 8.0}], {}, config)

        names = [item["name"] for item in classified["hot"] + classified["potential"] + classified["hot_concepts"]]
        self.assertNotIn("昨日连板", names)
        self.assertNotIn("二板", names)
        self.assertEqual(len(classified["hot"]), 8)
        self.assertLessEqual(len(classified["potential"]), 8)

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
        config = st.deep_merge(st.DEFAULT_CONFIG, {"rps_threshold": 90, "rps_min_sample": 1})
        st.apply_sector_rps_metrics(candidates, config)

        strongest = next(item for item in candidates if item["code"] == "000002")
        self.assertEqual(strongest["rps10"], 100.0)
        self.assertTrue(strongest["four_cycle_red"])
        self.assertIn("四周期红", strongest["rps_summary"])

    def test_apply_sector_rps_metrics_requires_min_sample(self):
        candidates = [
            {"code": "000001", "change_10d": 3, "change_20d": 5, "change_50d": 7, "change_120d": 9, "change_250d": 11},
            {"code": "000002", "change_10d": 10, "change_20d": 12, "change_50d": 15, "change_120d": 18, "change_250d": 20},
        ]
        config = st.deep_merge(st.DEFAULT_CONFIG, {"rps_threshold": 90, "rps_min_sample": 5})
        st.apply_sector_rps_metrics(candidates, config)

        strongest = next(item for item in candidates if item["code"] == "000002")
        self.assertTrue(strongest["rps_sample_too_small"])
        self.assertFalse(strongest["four_cycle_red"])
        self.assertIn("样本不足", strongest["rps_summary"])

    def test_evaluate_anchored_breakout_uses_recent_pivot_anchor(self):
        rows = []
        for day in range(160):
            close = 100.0
            high = 101.0
            low = 99.0
            volume = 100.0
            amount = 1000.0

            if day == 30:
                close = 124.0
                high = 125.0
                low = 121.0
            elif 31 <= day <= 45:
                close = 96.0
                high = 98.0
                low = 94.0
            elif day == 80:
                close = 109.0
                high = 110.0
                low = 106.0
            elif 81 <= day <= 100:
                close = 102.0
                high = 104.0
                low = 100.0
            elif 101 <= day <= 149:
                close = 106.0
                high = 107.0
                low = 105.0
            elif day == 150:
                close = 111.0
                high = 112.0
                low = 108.0
                volume = 180.0
                amount = 2200.0
            elif 151 <= day <= 159:
                close = 112.0
                high = 113.0
                low = 109.0

            rows.append(
                {
                    "date": f"2026-01-{day + 1:02d}",
                    "open": close - 0.5,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "amount": amount,
                    "turn": 3.0,
                }
            )

        df = pd.DataFrame(rows)
        breakout = st.evaluate_anchored_breakout(df, st.load_config())

        self.assertTrue(breakout["anchor_found"])
        self.assertEqual(breakout["anchor_price"], 110.0)
        self.assertTrue(breakout["anchored_breakout_signal"])
        self.assertEqual(breakout["anchored_breakout_day"], 10)
        self.assertGreaterEqual(breakout["anchored_breakout_score"], 40)

    def test_evaluate_anchored_breakout_requires_breakout_amount(self):
        rows = []
        for day in range(160):
            close = 100.0
            high = 101.0
            low = 99.0
            volume = 100.0
            amount = 1000.0

            if day == 80:
                close = 109.0
                high = 110.0
                low = 106.0
            elif 81 <= day <= 100:
                close = 102.0
                high = 104.0
                low = 100.0
            elif 101 <= day <= 149:
                close = 106.0
                high = 107.0
                low = 105.0
            elif day == 150:
                close = 111.0
                high = 112.0
                low = 108.0
                volume = 125.0
                amount = 1200.0
            elif 151 <= day <= 159:
                close = 112.0
                high = 113.0
                low = 109.0

            rows.append(
                {
                    "date": f"2026-03-{(day % 28) + 1:02d}",
                    "open": close - 0.5,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "amount": amount,
                    "turn": 3.0,
                }
            )

        breakout = st.evaluate_anchored_breakout(pd.DataFrame(rows), st.load_config())

        self.assertTrue(breakout["anchor_found"])
        self.assertFalse(breakout["anchored_breakout_signal"])
        self.assertLess(breakout["breakout_amount_ratio20"], 1.5)

    def test_evaluate_anchored_breakout_can_use_anchor_older_than_120_days(self):
        rows = []
        for day in range(260):
            close = 100.0
            high = 101.0
            low = 99.0
            volume = 100.0
            amount = 1000.0

            if day == 20:
                close = 139.0
                high = 140.0
                low = 136.0
            elif 21 <= day <= 45:
                close = 108.0
                high = 110.0
                low = 105.0
            elif 46 <= day <= 229:
                close = 118.0
                high = 120.0
                low = 114.0
            elif 230 <= day <= 244:
                close = 126.0
                high = 129.0
                low = 122.0
            elif day == 250:
                close = 141.0
                high = 142.0
                low = 137.0
                volume = 220.0
                amount = 2600.0
            elif 251 <= day <= 259:
                close = 142.0
                high = 143.0
                low = 138.0

            rows.append(
                {
                    "date": f"2026-02-{(day % 28) + 1:02d}",
                    "open": close - 0.5,
                    "high": high,
                    "low": low,
                    "close": close,
                    "volume": volume,
                    "amount": amount,
                    "turn": 3.0,
                }
            )

        df = pd.DataFrame(rows)
        breakout = st.evaluate_anchored_breakout(df, st.load_config())

        self.assertTrue(breakout["anchor_found"])
        self.assertGreater(breakout["anchor_days_ago"], 120)
        self.assertEqual(breakout["anchor_price"], 140.0)
        self.assertTrue(breakout["anchored_breakout_signal"])

    def test_find_prior_high_anchor_reuses_scored_zones(self):
        config = st.load_config()
        zones = [
            {
                "latest_touch_days_ago": 12,
                "visibility_score": 80,
                "price": 10.0,
                "touches": 1,
            },
            {
                "latest_touch_days_ago": 100,
                "visibility_score": 95,
                "price": 12.0,
                "touches": 2,
            },
        ]
        original_build_prior_high_zones = st.build_prior_high_zones
        try:
            st.build_prior_high_zones = lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("unexpected rebuild"))
            anchor = st.find_prior_high_anchor(pd.DataFrame(), config, scored_zones=zones)
            self.assertEqual(anchor["price"], 10.0)
        finally:
            st.build_prior_high_zones = original_build_prior_high_zones

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
                        "ema55_rising": True,
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
                        "ema55_rising": True,
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
                        "ema55_rising": False,
                        "ma_bullish": False,
                        "vol_ratio": 0.8,
                        "volatility_10d": 2.0,
                        "change_5d": -1.0,
                        "change_10d": -2.0,
                    },
                }
                return mapping[code]

            def fake_strategy(df, stock, config=None):
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

    def test_screen_stocks_in_sector_fetches_full_history_after_light_pass(self):
        original_get_sector_members = st.get_sector_members
        original_get_stock_history = st.get_stock_history
        original_analyze_stock_tech = st.analyze_stock_tech
        original_build_daily_strategy_state = st.build_daily_strategy_state
        try:
            config = st.deep_merge(
                st.DEFAULT_CONFIG,
                {
                    "sector_member_limit": 1,
                    "stocks_per_sector": 1,
                    "min_stock_score": 0,
                    "history_window_days": 321,
                },
            )
            st.get_sector_members = lambda *args, **kwargs: [
                {"code": "000001", "name": "Alpha", "price": 10, "pe": 20, "amount": 300, "turnover": 5.0, "change_pct": 2.0},
            ]
            history_calls = []

            def fake_history(code, **kwargs):
                history_calls.append((code, kwargs["days"]))
                return pd.DataFrame(
                    [
                        {"date": "2026-04-13", "close": 10.0, "volume": 100.0, "code": code},
                        {"date": "2026-04-14", "close": 10.5, "volume": 120.0, "code": code},
                    ]
                )

            st.get_stock_history = fake_history
            st.analyze_stock_tech = lambda df, config: {
                "price": 10.5,
                "ema_short": 10.2,
                "ema_long": 10.0,
                "ema13": 10.2,
                "ema55": 10.0,
                "ema_dist_pct": 0.01,
                "ema13_dist_pct": 0.01,
                "ema_bullish": True,
                "ema13_rising": True,
                "ema55_rising": True,
                "ma_bullish": True,
                "vol_ratio": 1.3,
                "volatility_10d": 2.0,
                "change_5d": 2.5,
                "change_10d": 4.0,
                "change_50d": 6.0,
                "change_120d": 8.0,
                "change_250d": 10.0,
            }
            st.build_daily_strategy_state = lambda df, stock, config=None: {
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

            picks = st.screen_stocks_in_sector({"name": "Robotics", "type": "industry"}, config, "2026-04-14")

            self.assertEqual([item["code"] for item in picks], ["000001"])
            self.assertEqual(history_calls, [("000001", st.get_light_history_days(config)), ("000001", 321)])
            self.assertIn("趋势通过", picks[0]["signal_breakdown_summary"])
        finally:
            st.get_sector_members = original_get_sector_members
            st.get_stock_history = original_get_stock_history
            st.analyze_stock_tech = original_analyze_stock_tech
            st.build_daily_strategy_state = original_build_daily_strategy_state

    def test_screen_stocks_records_reject_reasons_and_candidate_export(self):
        original_get_sector_members = st.get_sector_members
        original_get_stock_history = st.get_stock_history
        original_analyze_stock_tech = st.analyze_stock_tech
        try:
            config = st.deep_merge(
                st.DEFAULT_CONFIG,
                {"sector_member_limit": 1, "stocks_per_sector": 1, "min_stock_score": 0},
            )
            st.get_sector_members = lambda *args, **kwargs: [
                {"code": "000001", "name": "Weak", "price": 10, "pe": 20, "amount": 300, "turnover": 5.0, "change_pct": 2.0},
            ]
            st.get_stock_history = lambda *args, **kwargs: pd.DataFrame(
                [
                    {"date": "2026-04-13", "close": 10.0, "volume": 100.0, "code": "000001"},
                    {"date": "2026-04-14", "close": 9.8, "volume": 90.0, "code": "000001"},
                ]
            )
            st.analyze_stock_tech = lambda df, config: {
                "price": 9.8,
                "ema_short": 9.8,
                "ema_long": 10.0,
                "ema13": 9.8,
                "ema55": 10.0,
                "ema_dist_pct": 0.0,
                "ema13_dist_pct": 0.0,
                "ema_bullish": False,
                "ema13_rising": False,
                "ema55_rising": False,
                "ma_bullish": False,
                "vol_ratio": 0.9,
                "volatility_10d": 2.0,
                "change_5d": -1.0,
                "change_10d": -2.0,
            }

            diagnostics = st.build_scan_diagnostics()
            picks = st.screen_stocks_in_sector(
                {"name": "Robotics", "type": "industry"},
                config,
                "2026-04-14",
                scan_diagnostics=diagnostics,
            )
            normalized = st.normalize_scan_diagnostics(diagnostics)

            self.assertEqual(picks, [])
            self.assertEqual(normalized["reject_reasons"]["趋势未通过"], 1)
            self.assertEqual(normalized["candidates"][0]["scan_status"], "rejected")
            self.assertIn("signal_breakdown_summary", normalized["candidates"][0])
        finally:
            st.get_sector_members = original_get_sector_members
            st.get_stock_history = original_get_stock_history
            st.analyze_stock_tech = original_analyze_stock_tech

    def test_light_screen_allows_breakout_setup_when_rps_not_passed(self):
        original_get_sector_members = st.get_sector_members
        original_get_stock_history = st.get_stock_history
        original_analyze_stock_tech = st.analyze_stock_tech
        original_build_daily_strategy_state = st.build_daily_strategy_state
        try:
            config = st.deep_merge(
                st.DEFAULT_CONFIG,
                {
                    "sector_member_limit": 1,
                    "stocks_per_sector": 1,
                    "min_stock_score": 0,
                    "rps_threshold": 101,
                    "history_window_days": 321,
                },
            )
            st.get_sector_members = lambda *args, **kwargs: [
                {"code": "000001", "name": "Alpha", "price": 10, "pe": 20, "amount": 300, "turnover": 5.0, "change_pct": 2.0},
            ]
            st.get_stock_history = lambda code, **kwargs: pd.DataFrame(
                [
                    {"date": "2026-04-13", "close": 10.0, "high": 10.0, "volume": 100.0, "code": code},
                    {"date": "2026-04-14", "close": 10.5, "high": 10.5, "volume": 140.0, "code": code},
                ]
            )
            st.analyze_stock_tech = lambda df, config: {
                "price": 10.5,
                "ema_short": 10.2,
                "ema_long": 10.0,
                "ema13": 10.2,
                "ema55": 10.0,
                "ema_dist_pct": 0.01,
                "ema13_dist_pct": 0.01,
                "ema_bullish": True,
                "ema13_rising": True,
                "ema55_rising": True,
                "ma_bullish": True,
                "vol_ratio": 1.4,
                "volatility_10d": 2.0,
                "change_5d": 2.5,
                "change_10d": 4.0,
                "change_50d": 6.0,
                "change_120d": 8.0,
                "change_250d": 10.0,
            }
            st.build_daily_strategy_state = lambda df, stock, config=None: {
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

            picks = st.screen_stocks_in_sector({"name": "Robotics", "type": "industry"}, config, "2026-04-14")

            self.assertEqual([item["code"] for item in picks], ["000001"])
            self.assertIn("RPS未过", picks[0]["signal_breakdown_summary"])
            self.assertIn("突破迹象通过", picks[0]["signal_breakdown_summary"])
        finally:
            st.get_sector_members = original_get_sector_members
            st.get_stock_history = original_get_stock_history
            st.analyze_stock_tech = original_analyze_stock_tech
            st.build_daily_strategy_state = original_build_daily_strategy_state

    def test_volume_quality_score_uses_single_capped_entry(self):
        state = st.build_volume_quality_state(
            {"basic_volume_pass": True, "anchored_breakout_signal": True, "volume_price_score": 7},
            {"volume_confirm": 10},
            10,
        )

        self.assertEqual(state["volume_quality_score"], 10)
        self.assertTrue(state["volume_quality_pass"])

    def test_volume_quality_grade_marks_full_breakout_confirmation(self):
        state = st.build_volume_quality_state(
            {
                "basic_volume_pass": False,
                "anchored_breakout_signal": True,
                "volume_price_score": 9,
                "breakout_volume_pass": True,
                "pullback_shrink_pass": True,
                "confirm_volume_price_pass": True,
            },
            {"volume_confirm": 10},
            10,
        )

        self.assertEqual(state["volume_quality_grade"], "A")
        self.assertIn("A级", state["volume_quality_summary"])

    def test_build_payload_can_disable_candidate_export_but_keep_edges(self):
        diagnostics = st.build_scan_diagnostics()
        candidate = {
            "sector": "Robotics",
            "code": "000001",
            "name": "Alpha",
            "score": 53,
            "signal": "观察",
            "signal_breakdown": {},
        }
        st.record_scan_candidate(diagnostics, candidate, "rejected", "评分低于阈值")
        config = st.deep_merge(st.DEFAULT_CONFIG, {"export_candidates": False, "min_stock_score": 55})
        payload = st.build_payload("2026-04-14", {"hot": [], "potential": []}, [], [], None, {}, [], {}, config, diagnostics)

        self.assertEqual(payload["stock_candidates"], [])
        self.assertEqual(payload["edge_candidates"][0]["code"], "000001")

    def test_scan_diagnostics_can_skip_candidate_tracking(self):
        diagnostics = st.build_scan_diagnostics(track_candidates=False)
        st.record_scan_candidate(diagnostics, {"code": "000001", "score": 54}, "rejected", "评分低于阈值")
        normalized = st.normalize_scan_diagnostics(diagnostics)

        self.assertEqual(normalized["stage_counts"]["rejected"], 1)
        self.assertEqual(normalized["reject_reasons"]["评分低于阈值"], 1)
        self.assertEqual(normalized["candidates"], [])

    def test_merge_stock_pick_keeps_best_scoring_sector_and_sources(self):
        picks_by_code = {}
        st.merge_stock_pick(picks_by_code, {"code": "000001", "name": "Alpha", "sector": "Robotics", "score": 70})
        st.merge_stock_pick(picks_by_code, {"code": "000001", "name": "Alpha", "sector": "Automation", "score": 80})

        pick = picks_by_code["000001"]
        self.assertEqual(pick["sector"], "Automation")
        self.assertEqual(pick["best_sector"], "Automation")
        self.assertEqual(pick["first_sector"], "Robotics")
        self.assertEqual(pick["sector_overlap_count"], 2)
        self.assertEqual(pick["source_sectors"], ["Automation", "Robotics"])

    def test_generate_screening_diagnostics_report_lists_edge_candidates(self):
        report = st.generate_screening_diagnostics_report(
            "2026-04-14",
            {"stage_counts": {"members_seen": 3, "members_scanned": 2, "light_passed": 1, "score_passed": 1, "selected": 0}, "reject_reasons": {"趋势未通过": 2}},
            [{"name": "Alpha", "code": "000001", "sector": "Robotics", "signal": "观察", "score": 54, "edge_reason": "接近评分阈值"}],
        )

        self.assertIn("筛选诊断", report)
        self.assertIn("趋势未通过", report)
        self.assertIn("边缘候选", report)
        self.assertIn("Alpha", report)

    def test_breakout_pool_persists_entries_and_uses_20pct_break_price_stop(self):
        original_db_path = st.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                st.DB_PATH = Path(temp_dir) / "sector_history.db"
                st.init_db()
                st.upsert_breakout_pool_entries(
                    [
                        {
                            "code": "000001",
                            "name": "Alpha",
                            "sector": "Robotics",
                            "pool_status": "watch",
                            "breakout_price": 10.0,
                            "price": 9.2,
                            "ema55": 8.8,
                            "score": 62,
                            "signal": "前高突破",
                        }
                    ],
                    "2026-04-14",
                )
                active = st.load_active_breakout_pool()
                self.assertEqual(active[0]["code"], "000001")

                config = st.deep_merge(st.DEFAULT_CONFIG, {"breakout_pool": {"break_below_breakout_pct": 0.20}})
                self.assertEqual(st.get_breakout_pool_remove_reason({"price": 8.1, "breakout_price": 10.0}, config), "")
                self.assertEqual(
                    st.get_breakout_pool_remove_reason({"price": 7.99, "breakout_price": 10.0}, config),
                    "跌破突破价20%",
                )
        finally:
            st.DB_PATH = original_db_path

    def test_prune_breakout_pool_overflow_keeps_status_score_and_amount(self):
        original_db_path = st.DB_PATH
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                st.DB_PATH = Path(temp_dir) / "sector_history.db"
                st.init_db()
                st.upsert_breakout_pool_entries(
                    [
                        {"code": "000001", "name": "Low", "sector": "Pool", "pool_status": "watch", "breakout_price": 10, "price": 10, "score": 60, "amount": 1},
                        {"code": "000002", "name": "HighAmount", "sector": "Pool", "pool_status": "watch", "breakout_price": 10, "price": 10, "score": 60, "amount": 9},
                        {"code": "000003", "name": "Pullback", "sector": "Pool", "pool_status": "pullback", "breakout_price": 10, "price": 10, "score": 50, "amount": 1},
                    ],
                    "2026-04-14",
                )
                config = st.deep_merge(st.DEFAULT_CONFIG, {"breakout_pool": {"max_size": 2}})
                removed = st.prune_breakout_pool_overflow(config, "2026-04-14")
                active_codes = {item["code"] for item in st.load_active_breakout_pool()}

                self.assertEqual([item["code"] for item in removed], ["000001"])
                self.assertEqual(active_codes, {"000002", "000003"})
        finally:
            st.DB_PATH = original_db_path

    def test_select_breakout_pool_picks_keeps_pullback_even_below_score(self):
        config = st.deep_merge(st.DEFAULT_CONFIG, {"breakout_pool": {"pick_limit": 2}, "min_stock_score": 70})
        pool_state = {
            "enabled": True,
            "tracked": [
                {"code": "000001", "name": "Alpha", "sector": "Robotics", "pool_status": "pullback", "score": 55},
                {"code": "000002", "name": "Beta", "sector": "Robotics", "pool_status": "tracking", "score": 72},
                {"code": "000003", "name": "Gamma", "sector": "Robotics", "pool_status": "tracking", "score": 50},
            ],
        }
        picks = st.select_breakout_pool_picks(pool_state, config)
        self.assertEqual([item["code"] for item in picks], ["000001", "000002"])

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

    def test_evaluate_saved_picks_uses_next_open_entry_price(self):
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
                        {"date": "2026-04-12", "open": 11.2, "close": 12.24},
                    ]
                )
                benchmark_history = pd.DataFrame(
                    [
                        {"date": "2026-04-10", "open": 99.0, "close": 100.0},
                        {"date": "2026-04-11", "open": 100.5, "close": 101.0},
                        {"date": "2026-04-12", "open": 102.0, "close": 103.515},
                    ]
                )
                st.get_stock_history = lambda *args, **kwargs: stock_history.copy()
                st.get_benchmark_history = lambda *args, **kwargs: benchmark_history.copy()

                config = st.deep_merge(
                    st.DEFAULT_CONFIG,
                    {"backtest": {"lookback_days": 5, "holding_days": [1], "min_samples": 1, "entry_price_basis": "next_open"}},
                )
                summary = st.evaluate_saved_picks("2026-04-12", config)

                self.assertEqual(summary["entry_price_basis"], "next_open")
                self.assertEqual(summary["metrics"][1]["avg_return"], 20.0)
                benchmark_name = next(iter(summary["benchmarks"]))
                self.assertEqual(summary["benchmarks"][benchmark_name][1]["excess_return"], 17.0)
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
                diagnose_screening=False,
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
        report = st.generate_report(
            "2026-04-14",
            classified,
            picks,
            streaks,
            backtest,
            comparison,
            alerts,
            config,
            scan_stats={"stage_counts": {"members_seen": 3, "members_scanned": 2, "light_passed": 1, "score_passed": 1, "selected": 1}, "reject_reasons": {"趋势未通过": 1}},
            breakout_pool={
                "enabled": True,
                "active_count": 3,
                "new_entries": [],
                "tracked": [{"name": "Beta", "code": "000002", "sector": "Automation", "pool_status": "pullback", "score": 66, "price": 12.3, "breakout_price": 11.8}],
                "removed": [],
            },
        )
        self.assertIn("2026-04-14", report)
        self.assertIn("2026-04-13", report)
        self.assertIn("Alpha", report)
        self.assertIn("Robotics", report)
        self.assertIn("关联概念", report)
        self.assertIn("信号 回踩买点", report)
        self.assertIn("四周期红", report)
        self.assertIn("参考止盈线", report)
        self.assertIn("筛选诊断", report)
        self.assertIn("趋势未通过", report)
        self.assertIn("突破股票池", report)
        self.assertIn("Beta", report)


if __name__ == "__main__":
    unittest.main()
