# 板块追踪分析脚本

一个面向 A 股板块轮动的日线分析与选股脚本，输出 Markdown 日报、JSON 结果，并保存历史快照用于回看和简单回测。

## 当前能力

- 拉取行业板块、概念板块、资金流和个股日线数据
- 生成热门板块、潜力板块和板块内候选个股
- 支持趋势、回踩、RPS、多周期强度、日线大单代理、MACD 点火、翻身计划、结构突破等筛选逻辑
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
- 板块成员截取、概念匹配、最低分过滤
- 评分权重和风险过滤规则
- 回测窗口、持有天数、样本下限

## 策略说明

通达信日线策略整理见：

`docs/tdx_daily_strategy.md`

当前已接入的是日线可复现部分，盘中逻辑和依赖 Level-2 / 公告明细数据的部分暂未接入。

## 说明

- 板块和资金流接口本质上是实时快照，不是完整历史快照
- 历史日期分析时，个股 K 线会按目标日期截断，板块快照仍取当下接口结果
- `report_time` 目前只是保留字段，脚本本身不负责定时调度
