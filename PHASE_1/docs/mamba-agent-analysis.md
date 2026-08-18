# Mamba Agent 方案分析报告

> 分析对象：https://github.com/Kosthi/kddcup2026-dataagents
> 项目代号：**Mamba Agent** ("Mamba Never Out")
> 分析日期：2026-08-18

---

## 一、仓库概况

| 项目 | 内容 |
|------|------|
| 目标 | KDD Cup 2026 DataAgent-Bench (Phase 1 & 2) |
| 架构风格 | ReAct + 原生 OpenAI function calling |
| 包管理器 | `uv` |
| CLI | `uv run dabench <command> --config <cfg>`（与官方接口兼容） |
| 评分脚本 | `scripts/score_predictions.py`（实现官方排行榜评分规则） |
| 代码量 | ~220 个 Python 文件，覆盖 agent/ETL/verification/tracing/dashboard |

---

## 二、整体架构

```
┌──────────────────────────────────────────────────────────────────────┐
│  CLI (Typer)                                                         │
│   dabench {status, inspect-task, run-task, run-benchmark, dashboard} │
└──────────────────────┬───────────────────────────────────────────────┘
                       │
              ┌────────▼────────┐
              │   AppConfig      │  Pydantic-validated YAML
              └────────┬────────┘
                       │
            ┌──────────▼──────────┐
            │  build_application   │  src/agents/application.py
            └──────────┬──────────┘
                       │
              ┌────────▼──────────┐
              │    ReActAgent      │  src/agents/agent.py
              │  ┌───────┐ ┌────┐ │
              │  │protocol│ │dispatcher│
              │  └───┬───┘ └──┬──┘ │
              │      └────┬───┘    │
              │      ┌────▼────┐   │
              │      │recorder │   │
              │      └────┬────┘   │
              └───────────┼────────┘
                          ▼
                 prediction.csv + summary.json
                 optional traces.db
```

### 与官方 Starter Kit 的流程对比

**官方 Starter Kit：**
```
任务 → YAML(API) → ReActAgent(JSON action) → 裸工具集 → answer → prediction.csv
```

**Mamba Agent：**
```
任务 → YAML(API + rate_limit + verifier + ...)
    → [ETL 预处理: 大文档→CSV]
    → ReActAgent(原生 function calling)
        → explore(全量扫描) → run_etl(按需) → preview_file
        → execute_sql/python → grep_context
        → answer verifier(校验) → answer(提交)
    → prediction.csv + summary.json + traces.db
```

---

## 三、核心差异化设计

### 3.1 ETL 预处理管线

**痛点：** 很多任务包含大段非结构化文本（Markdown/PDF），如基金说明书、公司简介、病历记录。原始 ReAct 循环无法高效处理这类数据。

**方案：** 在 agent 循环之前，自动检测并执行 Prose→CSV 的 ETL 管线。

**位置：** `src/agents/etl/` — 22 个 Python 文件

**管线流程：**
```
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│ detect   │───→│ compress │───→│ parse    │───→│ reconcile│───→│ merge    │
│ 发现散文 │    │ LLM压缩  │    │ 结构化    │    │ 修复字段名│    │ 合并记录  │
└──────────┘    └──────────┘    └──────────┘    └──────────┘    └──────────┘
                                                        │
                                                        ▼
┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐
│   CSV    │←───│ verify   │←───│ normalize│←───│ schema   │
│  落盘    │    │ 值级校验  │    │ 归一化    │    │ 推断schema│
└──────────┘    └──────────┘    └──────────┘    └──────────┘
```

| 子模块 | 功能 |
|--------|------|
| `_detect.py` | 检测 Markdown/PDF/文本文件，筛选需要 ETL 的大文件 |
| `_compress.py` | LLM 将散文段落压缩为 `key=value` 实体行，去掉叙述性填充 |
| `_record.py` | 解析压缩输出的 KV 文本为 EntityTable，支持多行实体片段 |
| `_reconcile.py` | 修复字段名变体，统一同义词，修复格式错误行 |
| `_merge.py` | ID 冲突解决，跨文件合并 |
| `_schema.py` | 从 `knowledge.md` 推断字段定义、单位、枚举值 |
| `_identity.py` | 修复数值型 ID（如 001 vs 1），CSV 校验 |
| `_verify.py` | 空值重试，交叉字段去重，值级验证 |
| `_units.py` | 单位检测与转换（比例约定、目标单位投票） |
| `_pdf.py` | PDF→Markdown 转换 |
| `_grouping.py` | 实体分组逻辑 |
| `_router.py` | 路由哪些文件需要 ETL 处理 |
| `knowledge.py` | 知识库字段提取 |

**效果：** 将非结构化数据转化为 agent 可直接用 SQL 查询的 CSV 表，大幅提升信息提取能力。

---

### 3.2 Explorer 一站式数据探索

**痛点：** 官方 starter 需要多次调用 `list_context` → `read_csv` → `read_json` → `inspect_sqlite_schema` 等才能全面了解数据，浪费轮次且容易出错。

**方案：** 一个 `explore`（原 `inspect_files`）工具调用即返回完整的上下文数据地图。

**位置：** `src/agents/tools/inspect_files.py`

**返回内容：**

```
explore 返回:
├── 文件清单 (path, size, format)
├── CSV 表概要 (columns, dtypes, row_count, profile: min/max/null_rate/distinct)
├── SQLite 表概要 (columns, row_count, 数据采样 profile)
├── JSON 概要 (keys, length)
├── PDF 实体组采样 (entity_groups_sample — 只读实体段落，不读全文)
├── knowledge.md 提取 (字段定义, 公式, 值映射, 单位约定)
├── ETL 来源清单 (etl_sources — 哪些文档需要 run_etl 工具)
├── 单位转换提示 (unit_conversion — 检测到字段单位不匹配时)
├── 值样本 (value_samples — 关键列的 DISTINCT 采样)
├── 列名转换 (column_conversions — 单位元数据可用时)
└── 警告/异常
```

**优势：** 一轮完成全部数据探查，agent 可以基于完整信息做出决策。

---

### 3.3 Answer Verifier（答案校验）

**痛点：** agent 经常提交列名不对、多列少列、单位未转换的错误答案，且无法在提交前发现。

**方案：** 提交前调用轻量 LLM 校验答案表，被拒则返回拒绝原因让 agent 修正。

**位置：** `src/agents/verification/answer.py`

**校验检查项：**

| 检查 | 内容 | 示例 |
|------|------|------|
| Check 1 — EXTRA 列 | question 没要求的列是否出现 | 多送了 CustomerID、rank、序号 |
| Check 2 — MISSING 列 | question 要求的列是否缺失 | 漏了 name、date |
| Check 3 — 行数合理 | 空结果 / How-many / 唯一 superlative 的行数 | How-many 应只有 1 行 |
| Check 4 — 聚合误判 | 数据列举任务不应有 GROUP BY | 查记录不应有 SUM/AVG |
| Check 5 — 单位转换 | 数值量级是否与转换因子匹配 | 万元 vs 元 差 10000 倍 |
| Check 6 — NULL 过滤 | 是否有隐含非空条件被忽略 | question 说"非空"但没过滤 |

**特殊机制：** `TerminalAnswerPolicy`

- 被拒答时，最新有效答案被保存为 `fallback_answer`
- 循环结束后若 agent 仍未提交有效答案，自动 `promote_fallback_answer` 作为最终答案
- 避免因 verifier 拒答导致得 0 分

**位置：** `src/agents/verification/terminal_policy.py`

---

### 3.4 多 Key 池 + 跨进程限速

**痛点：** 单 API key 跑 50 个任务，频繁遇到 429 / 配额不足，导致任务失败。

**方案：** 配置支持 `api_keys` 数组 + `rate_limit` 配置。

**位置：** `src/agents/llm/rate_limit.py` + `token_bucket.py`

**配置示例：**
```yaml
agent:
  api_keys:
    - YOUR_API_KEY_1
    - YOUR_API_KEY_2
  rate_limit:
    rpm_per_key: 1000
    tpm_per_key: 1000000
    safety_factor: 0.95
    reserve_output_tokens: 16384
    max_retries: 6
    backoff_initial_seconds: 1.0
    backoff_max_seconds: 30.0
```

**机制：**
- 任务按 `index % len(api_keys)` 固定分配到 key（确定性 pinning）
- 每 key 独立 RPM+TPM 双令牌桶（`filelock` 保护，跨进程安全）
- 遇到 429 自动退避重试，最多 6 次
- `safety_factor` 预留缓冲空间，避免突刺

---

### 3.5 增强提示词策略

**位置：** `src/agents/prompts/`

**System Prompt 行为规则：**

| 规则 | 说明 |
|------|------|
| 每个 turn 必须有 tool_call | 纯文本回复会被丢弃，导致任务失败 |
| 证据优先 | 单位/字段语义/格式必须通过采样验证，不能假设 |
| 过滤完整性检查 | 提交前重新读题，逐条核对每个 WHERE 条件是否都应用了 |
| 数据列举规则 | 查/看/找/列出 类问题 → 返回原始粒度行，不能聚合 |
| 列名镜像 | 不要合并/拆分/重命名源列，保持原样 |
| 日期格式 | 用 `SELECT DISTINCT col LIMIT 3` 先检查实际格式，列名不是证据 |
| 分类过滤 | 用 `=` 精确匹配，不用 `LIKE '%X%'` |
| 格式容差 | question 和数据的格式可能不同（如 `0:01:54` vs `1:54.455`），需归一化匹配 |
| 假设证伪 | 假设被证伪后丢弃，从观测值重新推导，不能回退到原始猜测 |
| 无结果不提交 | 找不到结果时不能提交空表，需重新检查列映射、格式、单位、连接和时态粒度 |
| 多条件实体范围 | 当要求"在满足条件 A 且 B 的实体中计数 Y"，先找出同时满足 A 和 B 的实体，再在那范围内聚合 |
| 单位后缀 | 数值列不能带单位后缀（如 `48.706%` 错误，`48.706` 正确） |

**Data Output Rules（数据输出规则）** — `src/agents/prompts/rules.py`：
- 单位转换：`explore` 返回 `unit_conversion` 时必须在 `execute_python` 中应用
- 默认单位：`knowledge` 声明默认存储单位（如百万元）且 `explore` 未返回转换时，数据已在那个单位中
- 列名镜像：永远不要合并/拆分/重命名源列
- 数据列举触发器：查/看/找/搜/显示/列出 等关键词 → 数据列举任务
- 聚合触发器：只有包含 total/sum/average/count/how many 等关键词时才聚合

**Disambiguation Rules（消歧规则）：**
- "大于/deceeds" 语义 → `deceeds X` = `< X`
- 格式容差 → 归一化到最粗粒度再匹配
- 分类过滤 → 精确匹配，不用 LIKE
- 日期格式 → 先采样验证再操作

---

### 3.6 扩展工具集

| 工具 | 作用 | 输入 | 相比官方 |
|------|------|------|---------|
| `explore` (原名 `inspect_files`) | 一次性全量数据探索 | `(none)` | **新增** — 替代 list_context + 多次 read_* |
| `preview_file` | 预览单个文件内容 | `path` | 增强版 read_csv/read_json/read_doc |
| `run_etl` | 按需将散文文档→CSV | `paths` | **新增** |
| `execute_context_sql` | 对 SQLite / 虚拟上下文执行 SQL | `path`, `sql`, `limit` | 同官方 |
| `grep_context` | 跨文件正则搜索 | `path`, `pattern` | **新增** |
| `execute_python` | 执行 Python 代码 | `code` | 同官方，增强：支持 `DABENCH_ANSWER_DIR` 环境变量 |
| `answer` | 提交答案 | `columns`, `rows`, `from_csv` | 增强版 — 支持 `from_csv` 大文件路径参数 |

**answer 工具增强：**
- `from_csv` 参数：当答案超过 10 行或 50 个单元格时，必须通过 CSV 文件路径提交
- 自动类型推断（int/float/bool/string）
- 全角逗号规范化
- `answer_artifact_expression` 宏：`{{ answer_csv }}` 自动展开为 `answer_dir/final.csv`
- 空列修剪（全空列自动移除）

---

### 3.7 评分脚本

**位置：** `scripts/score_predictions.py`

**评分规则：**
```
recall  = matched_columns / gold_columns
penalty = lambda * (extra_columns / predicted_columns)   # lambda 默认 0.1
score   = max(0, recall - penalty)
```

**每列处理：**
1. 列名被忽略（不参与匹配）
2. 列顺序被忽略
3. 行顺序被忽略
4. 每列作为无序列值向量处理（"column multiset"）
5. 列间贪心匹配（最大化匹配列数）
6. 多余列受 `lambda * (extra / total)` 惩罚

**归一化规则：**
- 数字：`Decimal` 解析后 `ROUND_HALF_UP` 量化到 2 位小数（如 `18` → `"18.00"`）
- 日期：多种格式归一化后标准化
- 空值：统一为 `""`
- 所有值 SHA256 哈希后比较

**使用方式：**
```bash
uv run python scripts/score_predictions.py \
    --pred artifacts/runs/<run_id> \
    --gold data/public/output \
    --lambda 0.1 \
    --output artifacts/runs/<run_id>/scores.json \
    --verbose
```

---

### 3.8 其他增强

| 特性 | 位置 | 说明 |
|------|------|------|
| **原生 Function Calling** | `agents/llm/protocol.py` | 使用 OpenAI `tools=[...]`，而非 JSON action 协议 |
| **子进程隔离** | `agents/runs/subprocess.py` | 每个任务在独立子进程运行，超时/崩溃不影响其他任务 |
| **SQLite 追踪** | `agents/tracing/` | 可选写入 SQLite spans，通过 dashboard 可视化管理 |
| **Dashboard** | `agents/dashboard/` + `dashboard/frontend/` | FastAPI 后端 + Vite 前端，可视化追踪数据 |
| **Pre-act 规划** | `agents/agent.py` → `enable_preact: true` | 第一轮前先输出计划，再开始探索 |
| **Budget Guardrails** | `agents/runtime/budget.py` | 步骤预算监控，final-step 阻止非 answer 调用 |
| **视频支持** | `agents/video/` | Phase 2 视频帧处理管线 |
| **Frozen Pydantic 配置** | `agents/config.py` | 强类型校验，YAML 类型错误提前暴露 |
| **Blocklist** | `agents/runs/blocklist.py` | 跳过已知有问题的任务（歧义 / gold 错误 / 数据缺失） |
| **Key Pinning** | `agents/runs/runner.py` | 任务到 API key 的确定性固定，保证可复现性 |
| **CLAUDE.md** | 项目根目录 | AI 编码助手指导文件，规范 uv run / dabench CLI 使用 |

---

## 四、主要模块速查

| 模块 | 责任 |
|------|------|
| `src/agents/agent.py` | ReAct 主循环驱动器 |
| `src/agents/config.py` | 应用配置（Pydantic dataclass 校验） |
| `src/agents/cli.py` | `dabench` CLI 入口 |
| `src/agents/application.py` | App 工厂，组装 dataset/registry/model/verifier |
| `src/agents/tools/registry.py` | 不可变工具注册表 + 默认工具集工厂 |
| `src/agents/tools/dispatcher.py` | 工具调用派发 |
| `src/agents/tools/inspect_files.py` | 一站式数据探索工具 |
| `src/agents/tools/answer.py` | 终端答案提交工具（支持 from_csv） |
| `src/agents/tools/run_etl.py` | 按需 ETL 工具 |
| `src/agents/tools/preview.py` | 文件预览工具 |
| `src/agents/tools/execute.py` | 执行 Python/SQL 工具 |
| `src/agents/tools/grep_context.py` | 跨文件正则搜索工具 |
| `src/agents/etl/` | ETL 预处理管线（22 个子模块） |
| `src/agents/llm/openai.py` | OpenAI 兼容适配器 |
| `src/agents/llm/protocol.py` | 原生 function calling 协议适配 |
| `src/agents/llm/rate_limit.py` | 跨进程限速 |
| `src/agents/llm/token_bucket.py` | 令牌桶实现 |
| `src/agents/prompts/system.py` | 系统提示词模板 |
| `src/agents/prompts/task.py` | 任务提示词构建 |
| `src/agents/prompts/rules.py` | 数据输出规则 + 消歧规则 |
| `src/agents/prompts/recovery.py` | 错误恢复提示词 |
| `src/agents/runtime/` | 运行状态、消息回放、媒体附件、预算监控 |
| `src/agents/runs/runner.py` | 子进程任务运行器 |
| `src/agents/verification/answer.py` | 答案校验子 agent |
| `src/agents/verification/terminal_policy.py` | 终端答案策略（拒绝/回退） |
| `src/agents/explorer/` | Explorer 子 agent |
| `src/agents/tracing/` | SQLite 追踪 |
| `src/agents/dashboard/` | 追踪 dashboard 后端 |
| `dashboard/frontend/` | Vite 前端 |
| `scripts/score_predictions.py` | 官方评分规则实现 |

---

## 五、可复用的改进点（按优先级）

如果你希望在本项目（PHASE_1）中提升 baseline 准确率（当前 5/50 = 10%），以下是借鉴 Mamba Agent 的可落地改进：

| 优先级 | 改进项 | 预期效果 | 工作量 |
|--------|--------|---------|--------|
| 🔴 P0 | **增强系统提示词**：加入证据优先、列名镜像、过滤完整性检查、格式容差等规则 | 减少列名/格式/过滤错误，预计提升 10-20% | 小 |
| 🔴 P0 | **Inspect files 一站式探索**：将 `list_context` + 多次 `read_*` 合并为一次全量扫描 | 减少探索轮次，降低出错概率 | 中 |
| 🔴 P0 | **Answer 支持 `from_csv`**：大答案表通过文件路径提交，避免 inline JSON 超限 | 避免大结果集截断 | 小 |
| 🟡 P1 | **ETL 预处理**：将 Markdown/PDF 文档提前转为 CSV | 大幅提升非结构化数据处理能力 | 大 |
| 🟡 P1 | **多 Key 池 + 限速**：避免 429/配额不足 | 提升任务完成率 10-20% | 中 |
| 🟢 P2 | **Answer Verifier**：提交前校验答案表 | 减少低级错误 | 中 |
| 🟢 P2 | **评分脚本**：本地评估，方便迭代 | 量化改进效果 | 小 |
| 🔵 P3 | **子进程隔离**：每个任务独立子进程，超时不影响其他任务 | 提升稳定性 | 中 |
| 🔵 P3 | **Pre-act 规划**：第一轮前先输出计划 | 减少走弯路 | 小 |

---

## 六、总结

**Mamba Agent 相比官方 baseline 的核心提升在于：** 将原本"裸 ReAct + 简陋工具集"的架构，进化为包含 **ETL 预处理管线 + 一站式探索 + 答案校验 + 多 Key 限速 + 精细提示词工程** 的完整数据代理系统。

这些改进针对 DABench 任务的特点（多源异构数据、非结构化散文、严格评分规则）做了精准优化，是该团队在竞赛中取得优势的关键。