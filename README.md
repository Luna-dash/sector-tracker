# 板块追踪分析系统 📊

轻量化的A股板块轮动追踪 + EMA技术分析选股工具。

## 功能
- 每日自动分析行业板块和概念板块
- 筛选热门板块TOP4 + 潜力板块TOP7
- 板块内选股（EMA13/55趋势 + 回踩买点判断）
- 热点勾连分析（概念板块交叉验证）
- 历史数据存储（SQLite数据库）
- 定时任务自动生成报告

## 安装
```bash
pip install akshare baostock pandas numpy
```

## 使用
```bash
python3 sector_tracker.py
```

## 技术指标
- MA均线: 5/10/20/60日
- EMA13/55: 趋势判断 + 回踩买点
- 回踩条件: EMA13>EMA55 + 股价距EMA13<1.2% + 缩量 + 5日涨幅为正

## 数据源
- 行业板块/概念板块: AKShare (东方财富)
- 个股K线: BaoStock
- 资金流向: AKShare (东方财富)
