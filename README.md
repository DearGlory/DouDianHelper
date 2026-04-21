# DouDianHelper

DouDianHelper 是一个面向 Windows 桌面环境的抖店 / 飞鸽自动化工具，用于批量读取订单号、检查订单是否满足邀评条件，并通过飞鸽向买家发送消息。

它的核心目标是：**尽量复用本地已登录的 Edge 环境，在可控前提下批量处理订单任务，并在出现风控中断后自动暂停、自动恢复。**

---

## 功能概览

当前版本支持以下能力：

- 从 Excel 中批量读取订单号
- 复用本地 Microsoft Edge 登录态
- 复用已打开的订单管理页，避免每单重复请求订单列表页
- 在抖店订单管理页执行订单预检
- 从订单上下文中提取飞鸽直达链接
- 自动打开买家会话并发送邀评消息
- 发送完成后自动按 `Esc` 退出当前会话，恢复工作页状态
- 触发风控后自动保存进度、释放浏览器资源、定时恢复
- 可选生成运行复核文件
- 程序结束后批量删除 Excel 中已处理订单

---

## 处理规则

只有满足以下条件的订单，才会发送消息：

- 订单状态为 **已完成**
- 不是售后单
- 未评价
- 不命中当前实现中的其他拦截条件

如果订单不满足条件，程序会跳过该订单，并记录跳过原因。

---

## 项目结构

```text
DouDianHelper/
├─ main.py                     # 主入口与任务调度
├─ browser_worker.py           # 浏览器自动化核心逻辑
├─ capture_storage_state.py    # 登录态 / 预热流程
├─ launch_edge.py              # Edge 启动、CDP 处理、资源清理
├─ excel_reader.py             # Excel 读取与结果回写
├─ pause_state.py              # 风控暂停状态保存 / 恢复
├─ logger_utils.py             # 日志初始化
├─ config.example.json         # 配置模板
├─ requirements.txt            # 依赖列表
├─ README.md                   # 项目说明
├─ logs/run_history/           # 运行复核文件输出目录
└─ TempFile/                   # 临时文件目录
```

---

## 运行环境

- Windows
- Python 3.11 及以上
- Microsoft Edge
- 本地可正常访问抖店 / 飞鸽
- Edge 中已有可用登录态，或允许程序在启动阶段完成登录预热

---

## 安装

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

---

## 配置

先从模板复制本地配置：

```bash
copy config.example.json config.json
```

然后按需修改 `config.json`。

### 常用配置项

| 字段 | 说明 |
|---|---|
| `excel_path` | 订单 Excel 文件路径 |
| `message_template` | 发送给买家的消息模板 |
| `log_level` | 日志级别 |
| `max_retries` | 单个订单最大重试次数 |
| `parallel_workers` | 并发工作页数量 |
| `risk_control_pause_seconds` | 风控触发后的暂停时长（秒） |
| `export_processed_orders_review` | 是否导出本轮复核文件 |
| `browser.cdp_url` | CDP 地址，默认 `http://127.0.0.1:9222` |
| `browser.use_real_user_profile` | 是否使用本地真实 Edge Profile |
| `browser.user_data_dir` | Edge 用户数据目录 |
| `browser.profile_directory` | Edge Profile 目录 |
| `selectors.*` | 页面元素选择器 |

### 复核文件输出

当 `export_processed_orders_review = true` 时，程序会在项目相对目录下输出复核文件：

```text
logs/run_history/processed-orders-review-YYYYMMDD-HHMMSS.xlsx
```

---

## 启动方式

### 正常运行

```bash
python main.py --config config.json
```

### 限制本轮处理数量

```bash
python main.py --config config.json --limit 20
```

### 指定并发工作页数量

```bash
python main.py --config config.json --parallel-workers 10
```

### 模拟运行

```bash
python main.py --config config.json --dry-run
```

### 先强制刷新登录态再继续主流程

```bash
python main.py --config config.json --force-refresh-login
```

---

## 主流程说明

程序运行时的大致流程如下：

1. 准备浏览器登录态 / 预热状态
2. 启动工作页
3. 复用订单管理页
4. 覆盖搜索框内容，搜索当前订单号
5. 执行订单预检
6. 提取联系买家的飞鸽直达链接
7. 打开飞鸽会话
8. 发送消息
9. 按 `Esc` 退出当前会话
10. 继续下一单

当前版本已经改为：

- 不再每单重新请求订单管理页
- 不再切换“已完成标签”
- 优先复用已有订单页，减少多余请求与风控压力

---

## 风控暂停与自动恢复

当程序检测到风控时，不会直接整批退出，而是执行以下流程：

1. 当前批次停止接新单
2. 保存当前运行进度到暂停状态文件
3. 释放当前 Edge 浏览器资源
4. 按 `risk_control_pause_seconds` 等待一段时间
5. 定时结束后重新拉起浏览器链路
6. 从剩余订单继续执行

暂停状态中会保存以下信息：

- 待处理订单列表
- 已处理订单列表
- 已发送 / 已跳过 / 已失败统计
- 当前并发工作页数
- 有效 limit
- 已执行数量
- 预计恢复时间

---

## Excel 队列机制

`Order.xlsx` 本身就是待处理队列。

程序运行时会：

- 读取 Excel 中现有订单号
- 处理成功或跳过后，在内存中记录结果
- 在本轮程序结束时，统一批量删除已处理订单

这样做的好处是：

- 中途风控暂停时，不会因为 Excel 已提前删行而丢失上下文
- 恢复运行时仍能按保存状态继续处理剩余订单

---

## 日志与输出

### 运行日志

运行日志会输出到本地 `logs/` 目录以及终端。

### 复核文件

如果启用了复核导出，则每轮运行结束后会生成：

```text
logs/run_history/processed-orders-review-YYYYMMDD-HHMMSS.xlsx
```

文件中会带上本轮订单的运行状态与原因，便于人工复查。

---

## 已知限制

- 当前方案主要面向 Windows 本地桌面环境
- 强依赖真实浏览器状态与站点页面结构
- 抖店 / 飞鸽页面结构变化后，选择器可能需要重新校准
- 自动化稳定性受网络、浏览器、站点风控策略共同影响

---

## 使用提醒

请在符合平台规则、业务规则与自身风险可控的前提下使用本项目。

本项目更适合作为：

- 本地自动化工具
- 内部业务辅助脚本
- 自用批处理项目
