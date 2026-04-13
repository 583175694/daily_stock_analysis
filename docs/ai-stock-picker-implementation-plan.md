# AI 选股实施方案

## 1. 文档定位

本文档承接以下两份文档：

- `docs/ai-stock-picker-prd.md`
- `docs/ai-stock-picker-design.md`

职责边界：

- `PRD`：定义做什么
- `Design`：定义为什么这样分层
- `Implementation Plan`：定义第一阶段具体怎么落地

本文档关注：

- 目录怎么拆
- API 怎么开
- 数据表怎么建
- 页面怎么挂
- 任务怎么拆
- 哪些文件改，哪些文件尽量不碰

## 2. 本轮实施目标

本轮只落地 V1 MVP，对应目标是：

- 用户可在 Web 页面手动运行一次 AI 选股
- 系统输出 Top N 候选股
- 每只候选股有基础评分和解释
- 用户可以从候选结果一键进入后续分析
- 历史选股结果可回看

本轮不做：

- 定时自动选股
- 结果后验跟踪
- 模板效果统计
- 组合风险联动
- 自动通知

## 3. 推荐目录结构

建议新增目录：

```text
src/stock_picker/
  __init__.py
  domain/
    __init__.py
    entities.py
    enums.py
    scoring.py
    filters.py
    templates.py
  application/
    __init__.py
    dto.py
    run_picker.py
    query_picker_result.py
    list_picker_tasks.py
  infrastructure/
    __init__.py
    market_data_adapter.py
    news_adapter.py
    llm_adapter.py
    repository.py
  interfaces/
    __init__.py
    api/
      __init__.py
      endpoints.py
      schemas.py
```

前端建议新增：

```text
apps/dsa-web/src/pages/StockPickerPage.tsx
apps/dsa-web/src/components/stock-picker/
  PickerRunPanel.tsx
  PickerTaskList.tsx
  PickerResultTable.tsx
  PickerResultDrawer.tsx
  PickerTemplateSelect.tsx
  PickerUniverseSelect.tsx
  index.ts
apps/dsa-web/src/api/stockPicker.ts
apps/dsa-web/src/types/stockPicker.ts
```

## 4. 推荐职责拆分

### 4.1 `domain`

只放纯业务规则，不依赖 FastAPI、数据库、前端或具体数据源实现。

主要职责：

- 定义选股任务、候选股、评分项、模板等实体
- 承载过滤规则
- 承载打分规则
- 定义模板元数据

### 4.2 `application`

承载用例编排。

主要职责：

- 接收选股请求
- 读取股票池
- 拉取需要的数据
- 执行过滤与评分
- 对前 N 候选调用 AI 做解释
- 保存结果
- 返回结果 DTO

### 4.3 `infrastructure`

负责与现有项目的公共能力对接。

主要职责：

- 复用现有行情与资讯能力
- 复用现有 LLM 能力
- 负责结果持久化

### 4.4 `interfaces`

只负责暴露 API，不承载核心选股逻辑。

## 5. 后端落地方案

### 5.1 第一阶段能力链路

推荐执行链路：

1. 接收用户请求
2. 解析股票池
3. 拉取候选股票基础数据
4. 执行硬过滤
5. 执行结构化评分
6. 选出 Top N
7. 对 Top N 做 AI 解释
8. 保存任务与结果
9. 返回结果

### 5.2 股票池来源

V1 先支持两类：

- 当前 `STOCK_LIST`
- 内置预设池

推荐先不要做：

- 用户自定义复杂股票池管理
- 全市场全量扫描
- 指数成分自动同步

原因：

- 这样可以先把链路跑通
- 避免一开始把范围拉太大

### 5.3 模板设计

V1 推荐只内置 3 到 5 个模板，每个模板本质是：

- 一组过滤条件
- 一组评分权重
- 一段模板说明

建议模板：

- `trend_breakout`
- `strong_pullback`
- `sector_leader`
- `event_driven`
- `balanced`

模板元数据建议包含：

- `id`
- `name`
- `description`
- `market_tags`
- `filter_profile`
- `score_weights`
- `max_candidates`
- `ai_review_top_n`

### 5.3.1 策略输入模式

V1、V2、V3 建议采用分阶段输入模式：

- V1：内置模板
- V2：内置模板 + 参数化覆盖
- V3：自然语言策略输入 -> 归一化为结构化模板

V1 的明确实现结论：

- 只实现内置模板
- 不开放用户自由输入策略直接执行
- 不让 LLM 决定模板结构

这样做的原因：

- API 契约更稳定
- 前端交互更简单
- 模板效果更容易对比
- 开发成本和返工风险更低

### 5.3.2 为后续开放输入预留的结构

建议即使在 V1，也为后续扩展预留下面两类结构：

- `template_id`
- `template_overrides`

其中：

- V1 可先要求 `template_overrides` 为空
- V2 再逐步开放受控字段
- V3 的自然语言输入不直接进入执行层，而是先转成 `template_id + template_overrides`

### 5.4 硬过滤建议

V1 只做确定性过滤。

建议过滤项：

- 无法获取基础行情数据的标的过滤
- 最近交易数据缺失过滤
- 日均成交额过低过滤
- 价格异常过滤
- 极端波动过滤
- 不满足模板最低趋势条件过滤

原则：

- 过滤条件尽量可解释
- 过滤结果可记录原因

### 5.5 打分建议

V1 建议使用结构化评分，不要直接让 LLM 排名。

建议评分维度：

- 趋势分
- 量价配合分
- 板块强度分
- 新闻催化分
- 风险扣分

建议结果字段：

- `total_score`
- `trend_score`
- `volume_score`
- `sector_score`
- `news_score`
- `risk_penalty`

### 5.6 AI 解释建议

AI 只作用于 Top N，而不是全量候选。

V1 推荐：

- 候选排序先由结构化评分完成
- AI 对前 10 到 20 只候选给出解释

建议 AI 输出：

- 入选理由
- 核心风险
- 观察点
- 一句话结论

## 6. API 草案

推荐新增命名空间：

- `/api/v1/picker`

### 6.1 获取模板列表

`GET /api/v1/picker/templates`

用途：

- 前端加载可选模板

响应建议：

- 模板 id
- 名称
- 简要说明
- 适用市场

### 6.2 获取股票池列表

`GET /api/v1/picker/universes`

用途：

- 前端加载可用股票池

V1 可以先返回：

- `watchlist`
- 若干内置预设池

### 6.3 运行选股

`POST /api/v1/picker/run`

请求建议：

- `template_id`
- `template_overrides`
- `universe_id`
- `limit`
- `force_refresh`

返回建议：

- `task_id`
- `status`
- `created_at`

V1 可直接同步执行并返回结果，也可设计成异步后端任务。

建议：

- 接口语义按异步任务设计
- 初期内部实现可先同步，后续再演进
- V1 可要求 `template_overrides` 为空或仅允许极少数白名单字段

### 6.4 获取任务列表

`GET /api/v1/picker/tasks`

用途：

- 展示历史选股任务

返回建议：

- 任务 id
- 模板名
- 股票池名
- 状态
- 生成时间
- 候选数量

### 6.5 获取任务详情

`GET /api/v1/picker/tasks/{task_id}`

用途：

- 获取某次选股结果详情

返回建议：

- 任务元信息
- 候选股列表
- 每只候选的评分与解释

## 7. 数据表草案

V1 推荐新增两张主表和一张扩展表。

### 7.1 `picker_tasks`

用途：

- 存一轮选股任务元数据

建议字段：

- `id`
- `task_id`
- `template_id`
- `template_name`
- `universe_id`
- `universe_name`
- `status`
- `candidate_count`
- `created_at`
- `completed_at`
- `error_message`
- `request_payload_json`

### 7.2 `picker_candidates`

用途：

- 存某次任务的候选股结果

建议字段：

- `id`
- `task_id`
- `rank_no`
- `stock_code`
- `stock_name`
- `total_score`
- `summary`
- `risk_summary`
- `review_status`
- `created_at`

### 7.3 `picker_candidate_scores`

用途：

- 存各候选股评分明细

建议字段：

- `id`
- `task_id`
- `stock_code`
- `trend_score`
- `volume_score`
- `sector_score`
- `news_score`
- `risk_penalty`
- `detail_json`

## 8. 页面实施方案

### 8.1 新页面

新增页面：

- `AI 选股`

推荐路由：

- `/picker`

### 8.2 页面结构

建议分为 4 个区域：

#### 区域 A：运行面板

包含：

- 股票池选择
- 模板选择
- 候选数量选择
- 运行按钮

#### 区域 B：任务列表

包含：

- 最近任务
- 状态
- 时间
- 模板
- 股票池

#### 区域 C：候选结果表

包含：

- 排名
- 股票代码
- 股票名称
- 总分
- 一句话理由
- 风险摘要
- 操作按钮

#### 区域 D：候选详情抽屉

包含：

- 各维度评分
- 入选理由
- 风险点
- 观察点
- 一键发起分析
- 一键跳转聊天

### 8.3 前端联动动作

V1 推荐支持：

- 跳转单股分析
- 跳转 Agent 聊天页
- 加入自选股

## 9. 薄集成改动点

建议只改这些现有区域：

### 后端

- API 路由注册文件

### 前端

- `App.tsx` 路由注册
- 侧边栏导航入口

### 可选

- 若需要统一风格，可新增少量公共组件复用，但不建议大改首页

## 10. 明确不碰的区域

本轮尽量不碰：

- 现有单股分析主流程
- 现有历史分析数据结构
- 现有回测接口
- 现有设置页的大规模配置渲染逻辑
- 主启动入口中的复杂调度分支

## 11. 建议开发顺序

### 阶段 0：骨架

产出：

- 后端目录骨架
- 前端页面骨架
- 占位 API
- 占位菜单入口

验收：

- 页面可打开
- API 路由可访问

### 阶段 1：后端主链路

产出：

- 股票池读取
- 模板定义
- 过滤与评分
- Top N 结果生成
- 任务与候选入库

验收：

- 用固定股票池能跑出候选结果

### 阶段 2：AI 解释

产出：

- 对 Top N 候选输出 AI 解释
- 解释结果落库

验收：

- 候选详情不再只是分数，有明确解释文本

### 阶段 2a：参数化模板准备

产出：

- `template_overrides` 数据结构
- 白名单参数校验
- 前端参数输入预留

验收：

- 不破坏 V1 契约前提下，为 V2 开放参数化模板打基础

### 阶段 3：前端可用化

产出：

- 可运行页面
- 任务列表
- 候选表格
- 候选详情抽屉

验收：

- 用户可以完整走通“运行 -> 查看 -> 发起后续分析”

### 阶段 4：联动动作

产出：

- 跳转单股分析
- 跳转 Agent 问股
- 加入自选股

验收：

- 候选结果能成为现有工作流的上游入口

### 阶段 5：自然语言策略输入预研

产出：

- 自然语言策略转模板的归一化方案
- 错误输入和模糊输入的兜底规则

验收：

- 能把自然语言稳定转成结构化模板摘要，而不是直接执行原始输入

## 12. 工单拆分建议

建议拆成以下任务包：

- 后端骨架与实体定义
- 模板与评分规则实现
- 股票池与数据适配器实现
- 任务持久化实现
- 选股 API 实现
- 前端页面与类型定义
- 结果表格与详情抽屉
- 跳转分析和加入自选股动作

## 13. 验收清单

V1 完成时，应满足：

- 有独立 `AI 选股` 页面
- 有独立 `/api/v1/picker/*` 接口
- 至少支持 1 个股票池和 3 个模板
- 能手动运行一次选股任务
- 能看到 Top N 结果
- 每只候选股有评分和解释
- 能从候选结果跳到单股分析或问股
- 历史任务可回看

## 14. 风险与控制

风险一：

- V1 范围不断膨胀，拖慢首版落地

控制：

- 强制只做手动运行，不接定时和通知

风险二：

- AI 解释耗时过高

控制：

- 只对 Top N 做解释

风险三：

- 结果和现有模块耦合太深

控制：

- 选股任务、候选结果、评分明细全部单独落表

风险四：

- 前端侵入首页过深

控制：

- 第一阶段坚持独立页面

## 15. 下一步建议

这份实施方案确定后，建议下一步直接进入“开发前清单”阶段：

- 确认 V1 模板列表
- 确认 V1 股票池来源
- 确认数据表字段
- 确认 API 返回结构
- 确认页面初稿

确认完之后，就可以正式进入编码阶段。
