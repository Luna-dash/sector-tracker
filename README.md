# 板块跟踪分析脚本

一个轻量的 A 股板块轮动分析脚本，输出 Markdown 日报和 JSON 结构化结果。

## 当前功能

- 拉取行业板块、概念板块、行业资金流
- 生成热门板块和潜力板块排名
- 在目标板块内按 EMA、MA、量价关系筛股
- 为每只股票输出命中条件和未命中条件说明
- 对高估值、短期过热、量比过高、波动过高做风险过滤和降权
- 将每日结果保存到 SQLite
- 输出 `reports/YYYY-MM-DD.md` 和 `reports/YYYY-MM-DD.json`
- 为最近一段时间的历史推荐生成简单回测摘要
- 为板块成员和 K 线做本地缓存

## 依赖

```bash
pip install akshare baostock pandas
```

## 使用方式

默认运行：

```bash
python sector_tracker.py
```

禁用缓存：

```bash
python sector_tracker.py --no-cache
```

限制热门/潜力板块数量：

```bash
python sector_tracker.py --sector-limit 2
```

指定 JSON 输出路径：

```bash
python sector_tracker.py --output-json reports/custom.json
```

跳过回测：

```bash
python sector_tracker.py --skip-backtest
```

## 配置项

`config.json` 支持这些能力：

- 基础策略参数：`hot_sectors_count`、`potential_sectors_count`、`stocks_per_sector`
- 技术指标参数：`ema_short`、`ema_long`、`ma_periods`、`pullback_threshold`、`analysis_days`
- 数据行为：`use_cache`、`sector_member_limit`、`concept_scan_limit`、`concept_match_limit`
- 评分权重：`score_weights.sector`、`score_weights.stock`
- 风险过滤：`risk_filters`
- 回测配置：`backtest.lookback_days`、`backtest.holding_days`、`backtest.min_samples`

## 说明

- 板块和资金流接口本身是实时快照，不是完整历史快照
- 如果使用历史日期分析，个股 K 线会按该日期截断，但板块快照仍然来自当前接口
- `report_time` 目前只是保留字段，脚本本身不负责定时调度
- 当前没有内置飞书推送
