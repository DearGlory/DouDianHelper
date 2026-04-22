# DouDianHelper

抖店 / 飞鸽自动化邀评工具。批量读取订单号，检查邀评条件，通过飞鸽向买家发送消息。

核心目标：**复用本地 Edge 登录态，批量处理订单，风控中断后自动暂停与恢复。**

---

## 功能概览

- 从 Excel 批量读取订单号
- 复用本地 Microsoft Edge 登录态（CDP / Launch 双模式）
- 复用已打开的订单管理页，避免重复请求
- 抖店订单管理页预检（状态、售后、备注关键词）
- 从订单上下文提取飞鸽直达链接，直接打开买家会话
- 飞鸽页二次校验（评价按钮状态、抽奖标签）
- 发送邀评消息后按 ESC 释放会话归属（确保后续客户消息派发给在线客服）
- 验证码风控检测 → 自动暂停 → 定时恢复
- 登录态缺失 → 自动回退有头浏览器预热
- 可选生成运行复核文件
- 程序结束后批量删除 Excel 中已处理订单

---

## 处理规则

只有满足以下全部条件的订单才会发送消息：

- 订单状态为 **已完成**（排除待支付、待发货、已关闭、已取消）
- 无售后（排除售后中、退款中、退货、换货等）
- 备注不含排除关键词
- 飞鸽页评价按钮为未评价状态
- 不含抽奖标签

不满足条件的订单会被跳过并记录原因。

---

## 项目结构

```text
DouDianHelper/
├─ main.py                     # 主入口：任务调度、风控暂停恢复、资源监控
├─ browser_worker.py           # 浏览器自动化核心：订单预检、会话操作、消息发送
├─ capture_storage_state.py    # 登录态捕获 / 预热流程
├─ launch_edge.py              # Edge 启动、CDP 连接、进程清理
├─ excel_reader.py             # Excel 读取、结果回写、复核导出
├─ pause_state.py              # 风控暂停状态保存 / 恢复
├─ logger_utils.py             # 日志初始化
├─ config.json                 # 运行配置
├─ config.example.json         # 配置模板
├─ requirements.txt            # Python 依赖
├─ setup.bat                   # 环境初始化脚本
├─ start_edge.bat              # 手动启动 Edge（CDP 调试模式）
├─ docs/
│  └─ architecture-flowchart.md # 架构流程图
└─ logs/run_history/           # 运行复核文件输出目录
```

---

## 运行环境

- Windows
- Python 3.11+
- Microsoft Edge
- 本地可正常访问抖店 / 飞鸽
- Edge 中已有可用登录态，或允许程序启动时完成登录预热

---

## 安装

```bash
pip install -r requirements.txt
python -m playwright install chromium
```

或使用 `setup.bat` 一键初始化。

---

## 配置

从模板复制：

```bash
copy config.example.json config.json
```

### 常用配置项

| 字段 | 说明 |
|---|---|
| `excel_path` | 订单 Excel 文件路径 |
| `message_template` | 发送给买家的消息模板 |
| `log_level` | 日志级别 |
| `max_retries` | 单个订单最大重试次数 |
| `parallel_workers` | 并发工作页数量 |
| `risk_control_pause_seconds` | 风控触发后暂停时长（秒） |
| `export_processed_orders_review` | 是否导出本轮复核文件 |
| `browser.mode` | 浏览器模式：`cdp`（默认）或 `launch` |
| `browser.cdp_url` | CDP 地址，默认 `http://127.0.0.1:9222` |
| `browser.headless` | 无头模式（`true` 时自动切换为 launch 模式） |
| `browser.use_real_user_profile` | 是否使用本地真实 Edge Profile |
| `browser.user_data_dir` | Edge 用户数据目录 |
| `browser.profile_directory` | Edge Profile 目录 |
| `selectors.*` | 页面元素选择器 |

---

## 启动方式

```bash
# 正常运行
python main.py --config config.json

# 限制本轮处理数量
python main.py --config config.json --limit 20

# 指定并发工作页数量
python main.py --config config.json --parallel-workers 10

# 模拟运行（不操作浏览器）
python main.py --config config.json --dry-run

# 强制刷新登录态
python main.py --config config.json --force-refresh-login
```

---

## 主流程

1. 准备浏览器登录态 / 预热
2. 启动工作页，复用订单管理页
3. 搜索当前订单号，执行订单预检
4. 提取飞鸽直达链接
5. 新开页面打开飞鸽会话
6. 发送邀评消息
7. 按 ESC 释放会话归属（使后续客户消息派发给在线客服）
8. 关闭会话页面，继续下一单

详细架构流程图见 `docs/architecture-flowchart.md`。

---

## 风控暂停与自动恢复

检测到验证码风控时：

1. 当前批次停止接新单
2. 保存运行进度到暂停状态文件
3. 释放浏览器资源
4. 等待 `risk_control_pause_seconds`
5. 重新拉起浏览器，从剩余订单继续

---

## Excel 队列机制

`Order.xlsx` 即待处理队列。程序运行时读取订单号，处理完成后在程序结束时统一批量删除已处理行。中途暂停不会丢失上下文。

---

## 浏览器模式

| 模式 | 场景 | 说明 |
|------|------|------|
| CDP | `headless=false`（默认） | 连接已运行的 Edge，共享登录态，多 worker 共享浏览器 |
| Launch | `headless=true` | 程序自行启动 Edge，独立实例 |

`headless=true` 时自动切换为 Launch 模式，无需手动配置。

---

## 已知限制

- 面向 Windows 本地桌面环境
- 强依赖真实浏览器状态与站点页面结构
- 抖店 / 飞鸽页面结构变化后，选择器可能需要重新校准
- 稳定性受网络、浏览器、站点风控策略共同影响

---

## 使用提醒

请在符合平台规则、业务规则与自身风险可控的前提下使用本项目。
