# 板块追踪分析脚本

一个面向 A 股板块轮动的日线分析与选股脚本，输出 Markdown 日报、JSON 结果，并保存历史快照用于回看和简单回测。

## 当前能力

- 拉取行业板块、概念板块、资金流和个股日线数据
- 生成热门板块、潜力板块和板块内候选个股，默认展示 8 个热门和 8 个潜力板块
- 支持趋势、回踩、RPS、多周期强度、日线大单代理、MACD 点火、翻身计划、结构突破等筛选逻辑
- 维护突破股票池，板块扫描阶段先独立发现近期前高突破，再跟踪回踩、确认和剔除
- 输出 `reports/YYYY-MM-DD.md` 和 `reports/YYYY-MM-DD.json`
- 将每日结果保存到 SQLite，支持历史结果回看和简单回测摘要
- 识别热点概念重合度，并补充到板块和个股摘要

## 安装

```bash
pip install akshare baostock pandas numpy
```

## 使用

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

## 配置

`config.json` 可调整：

- 板块筛选数量、每板块个股数量
- EMA、MA、RPS 等技术参数
- 前高突破识别窗口、量价阈值、长历史窗口
- 板块成员截取、概念匹配、最低分过滤
- 轻筛突破迹象、边缘候选数量、候选池导出开关
- 突破股票池规模、全市场补池数量、跌破突破价剔除比例、池满末位淘汰
- 板块扫描内的突破发现开关和单轮长历史发现上限
- RPS 最小样本、风险硬过滤/软扣分、轻筛高位震荡保护
- 运行期内存缓存、板块扫描间隔
- 是否写入报告文件
- 评分权重和风险过滤规则
- 回测窗口、持有天数、样本下限、买入价口径

JSON 输出会保留 `breakout_pool`、`stock_candidates`、`edge_candidates` 和 `scan_stats`，用于查看突破池、完整候选池、轻筛/精筛状态、边缘候选和淘汰原因。
如果只想看筛选诊断，可以使用：

```bash
python sector_tracker.py --diagnose-screening --skip-backtest
```

频繁调参时可避免生成报告文件：

```bash
python sector_tracker.py --no-output-files
```

主动全市场扫描并补充突破股票池：

```bash
python sector_tracker.py --update-breakout-pool --skip-backtest
```

## 功能与用法文档

详细功能介绍和用法见：

`docs/README.md`

## 说明

- 板块和资金流接口本质上是实时快照，不是完整历史快照
- 历史日期分析时，个股 K 线会按目标日期截断，板块快照仍取当下接口结果
- `report_time` 目前只是保留字段，脚本本身不负责定时调度
