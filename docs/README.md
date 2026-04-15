# 功能介绍与用法

## 功能介绍

本项目用于做 A 股板块轮动的日线级扫描，核心目标是从行业板块、概念板块和板块成员中筛出更值得跟踪的候选股，并输出日报和结构化 JSON。

主要能力：

- 板块扫描：读取行业板块、概念板块、资金流，生成热门板块和潜力板块。
- 个股轻筛：先用较短历史做趋势、EMA55、板块池 RPS、突破迹象筛选，减少长历史请求。
- 个股精筛：对轻筛通过的股票读取长历史，识别前高突破、回踩缩量、确认放量等结构。
- 突破股票池：板块扫描阶段会先独立发现近期前高突破，不要求最终评分或回踩入选；符合条件的个股写入 SQLite，后续持续跟踪回踩、确认和剔除。
- 评分拆解：输出基础分、RPS 分、量价分、突破分、策略分、风险扣分和最终分。
- 筛选诊断：统计每一步通过数、淘汰原因、边缘候选和跨板块去重数量。
- 共振识别：同一股票被多个强势板块选中时，保留最高分来源并记录所有来源板块。
- 板块清理：默认过滤昨日连板、二板、涨停板等纯数据类板块，避免它们干扰主题判断。
- 缓存优化：支持磁盘缓存和运行期内存缓存，减少同一轮运行内的重复读取和重复请求。
- 输出控制：可生成 Markdown 日报和 JSON，也可只打印结果，避免调参时产生过多报告文件。

## 基本用法

安装依赖：

```bash
pip install akshare baostock pandas numpy
```

默认运行：

```bash
python sector_tracker.py
```

主动全市场扫描并更新突破股票池：

```bash
python sector_tracker.py --update-breakout-pool --skip-backtest
```

跳过回测：

```bash
python sector_tracker.py --skip-backtest
```

只看筛选诊断：

```bash
python sector_tracker.py --diagnose-screening --skip-backtest
```

不写入报告文件，只在终端输出：

```bash
python sector_tracker.py --no-output-files
```

指定分析日期：

```bash
python sector_tracker.py --date 2026-04-15
```

指定 JSON 输出路径：

```bash
python sector_tracker.py --output-json reports/custom.json
```

限制板块数量：

```bash
python sector_tracker.py --sector-limit 2
```

只看某些板块：

```bash
python sector_tracker.py --sector 机器人 --sector 半导体
```

只保留回踩买点：

```bash
python sector_tracker.py --only-pullback
```

设置最低分：

```bash
python sector_tracker.py --min-score 70
```

离线读取缓存：

```bash
python sector_tracker.py --offline
```

## 输出说明

默认输出：

- `reports/YYYY-MM-DD.md`：人读日报。
- `reports/YYYY-MM-DD.json`：结构化结果。
- `data/sector_history.db`：历史板块和入选个股记录。

JSON 里的重点字段：

- `classified`：热门板块、潜力板块、热点概念。
- `stock_picks`：最终入选股票。
- `breakout_pool`：突破股票池状态，包括新入池、跟踪、回踩候选、确认信号和剔除记录。
- `stock_candidates`：候选池明细，受 `export_candidates` 控制。
- `edge_candidates`：接近入选或有结构亮点但未入选的股票。
- `scan_stats`：扫描统计和淘汰原因。
- `backtest`：简单回测摘要。

## 常用配置

主要配置在 `config.json`：

- `stocks_per_sector`：每个板块保留的股票数量。
- `hot_sectors_count` / `potential_sectors_count`：日报默认展示的热门、潜力板块数量，当前默认 8/8。
- `sector_member_limit`：每个板块最多扫描多少成员。
- `min_stock_score`：最终入选最低分。
- `history_window_days`：长历史窗口。
- `runtime_memory_cache`：是否启用运行期内存缓存。
- `write_output_files`：是否写入报告文件。
- `track_candidates`：是否内部追踪候选池。
- `export_candidates`：是否把完整候选池写入 JSON。
- `rps_min_sample`：板块池 RPS 生效所需最小样本数。
- `excluded_sector_keywords`：需要剔除的纯数据类板块关键词。
- `light_breakout_setup`：轻筛突破迹象阈值。
- `anchored_breakout`：前高突破和量价确认阈值。
- `breakout_pool`：突破池配置；池子不足或使用 `--update-breakout-pool` 时可全市场补池，默认最多 100 只，最近 5 日突破可入池，跌破突破价 20% 或连续跌破 EMA55 会剔除。
- `breakout_pool.sector_discovery_enabled`：是否在板块扫描阶段独立发现突破入池候选。
- `breakout_pool.sector_discovery_limit_per_run`：单轮板块扫描最多做多少只长历史突破发现，避免请求过多。
- 全市场股票列表接口偶发不可用时会自动重试；全部失败且无缓存时，本轮只跳过全市场补池，并在报告“数据状态”里注明原因。

突破池剔除只来自两类情况：

- 规则剔除：跌破突破价阈值、连续跌破 EMA55、超过观察期仍未形成回踩确认。
- 池满淘汰：超过 `max_size` 后，优先保留确认信号、回踩候选、高分、高成交额股票，末尾标记为“池满末位淘汰”。
- `risk_filters`：风险硬过滤和软扣分。

## 使用注意

- 板块和资金流接口是快照数据，不是完整历史数据。
- 历史日期运行依赖已保存的市场快照；没有快照时不会用当天实时板块数据污染历史记录。
- RPS 是当前候选池内的相对强度，不等同全市场 RPS。
- 报告和评分只用于技术筛选和复盘，不构成投资建议。
