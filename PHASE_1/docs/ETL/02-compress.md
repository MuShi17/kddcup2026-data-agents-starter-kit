# Phase 1 - Compress：散文文本压缩为结构化 Key-Value 行

## 1. 概述

### 1.1 在 ETL 管线中的位置

Compress 阶段是 ETL 管线的核心步骤之一，位于 **Schema 推断** 之后、**Key-Value 行归一化与合并** 之前。其输入是原始散文文本（prose text，如 `.md` / `.txt` / `.pdf` 提取的段落），输出是 `key: value | key: value` 格式的结构化文本行。

```
[Raw Text] → Schema Inference → **Compress** → KV Normalization → Merge → CSV
```

### 1.2 目标

将非结构化的散文描述压缩为每行一个实体、管道分隔的键值对格式：

```
record_id: 29 | field_a: value_a | field_b: value_b | ...
```

整个过程包含以下子步骤：

1. **实体感知分块**（Entity-Aware Chunking）：将段落按所属的实体（record ID）分组，确保同一实体的所有上下文在同一个 chunk 中，避免跨 chunk 分裂。
2. **压缩指南生成**（Compress Guide）：用 LLM 分析文档结构，生成一份可复用的 extraction system prompt，指导后续压缩。
3. **逐块压缩**（Chunk Compression）：对每个 chunk 调用 LLM，提取结构化 key-value 行。
4. **实体完整性校验**（Entity Integrity Check）：验证每个 chunk 的输出是否包含了所有预期的实体，对缺失的实体进行定向恢复。
5. **并行处理**：使用 `ThreadPoolExecutor` 并行处理所有 chunk。

---

## 2. 实体感知分块（Entity-Aware Chunking）

实体感知分块的目标是**确保每个实体的所有相关段落出现在同一个 chunk 中**，避免因跨 chunk 分裂导致 LLM 丢失上下文。

### 2.1 `group_paragraphs_by_entity()` — 按实体分组段落

**源文件：** `_grouping.py`，第 334–388 行

```python
def group_paragraphs_by_entity(
    text: str,
    adapter: ModelAdapter | None = None,
) -> tuple[dict[str, list[str]], list[str]] | None:
```

#### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `text` | `str` | 完整的散文文本 |
| `adapter` | `ModelAdapter \| None` | LLM 适配器。为 `None` 时仅使用确定性 pattern 匹配 |

#### 返回值

- `tuple[dict[str, list[str]], list[str]] | None`：
  - `dict[str, list[str]]`：`{record_id: [paragraph_text, ...]}` 的字典，每个 record_id 对应一组段落
  - `list[str]`：未被分配任何实体的剩余段落（仅包含含独立数字的段落）
  - 返回值 `None`：表示分组失败（段落太少或无法可靠分组）

#### 核心逻辑步骤

1. **拆分段落**：调用 `split_prose_paragraphs(text)`，按 `\n\n` 分隔，过滤空段落和长度 ≤ 30 字符的段落

2. **发现锚点模式**：调用 `discover_record_id_patterns(paras)` 发现文档中的记录 ID 模式（含已知的 `RECORD_ID_INLINE` 正则和自动发现的模式）

3. **LLM 分组**（当 `adapter` 不为 `None` 时）：
   - 调用 `group_paragraphs_by_llm()` 执行 LLM 分组
   - 对 LLM 返回的结果进行**确定性验证**（`verify_paragraph_groups`）
   - 验证通过后，执行**确定性恢复**：扫描每个段落，将 LLM 遗漏的段落按锚点模式补充回对应实体
   - 未分配段落中只保留包含独立数字的段落，过滤纯叙事段落

4. **纯 Pattern 回退**（当 LLM 分组不可用或失败时）：
   - 仅使用 `paragraph_record_ids()` 进行确定性匹配
   - 如果被标记的段落数 < 3 或没有分组，返回 `None`，触发 positional chunking 回退

#### 关键代码片段

```python
# 确定性恢复：LLM 遗漏的段落按锚点模式补充
recovered = 0
for i, para in enumerate(paras, start=1):
    for record_id in paragraph_record_ids(para, anchor_patterns):
        if record_id in verified and i not in verified[record_id]:
            verified[record_id].append(i)
            recovered += 1

# 纯 Pattern 回退
groups: dict[str, list[str]] = {}
for para in paras:
    record_ids = paragraph_record_ids(para, anchor_patterns)
    if record_ids:
        for record_id in record_ids:
            groups.setdefault(record_id, []).append(para)
```

---

### 2.2 `entity_chunks_by_token_budget()` — 按 Token 预算打包 Chunk

**源文件：** `_grouping.py`，第 391–431 行

```python
def entity_chunks_by_token_budget(
    entity_groups: dict[str, list[str]],
    token_budget: int = ENTITY_CHUNK_TOKEN_BUDGET,  # 默认 2500
) -> list[tuple[str, list[str]]]:
```

#### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `entity_groups` | `dict[str, list[str]]` | 实体分组的段落字典 |
| `token_budget` | `int` | 每个 chunk 的 token 预算上限，默认 `ENTITY_CHUNK_TOKEN_BUDGET = 2500` |

#### 返回值

- `list[tuple[str, list[str]]]`：每个元素为 `(chunk_text, [record_id1, record_id2, ...])`
  - `chunk_text`：多个实体用 `ENTITY_SEPARATOR`（`\n\n---\n\n`）连接的文本
  - `record_id` 列表：该 chunk 包含的所有 record ID

#### 核心逻辑步骤

1. 为每个实体构建完整文本：`[RECORD_ID: {rid}]\n` + `\n\n`.join(paras)
2. 使用 `count_qwen_tokens()` 计算每个实体的 token 数
3. 贪心打包：按 `entity_groups` 的迭代顺序逐个添加实体
   - 如果单个实体 >= token_budget，单独作为一个 chunk（不拆分该实体）
   - 如果当前 chunk 累积 token + 下一个实体 token > token_budget，则 flush 当前 chunk，开始新 chunk
4. 返回所有 chunk

#### 关键代码片段

```python
for rid, full, toks in entity_texts:
    if toks >= token_budget:
        _flush()
        chunks.append((full, [rid]))
        continue
    if current_tokens + toks > token_budget and current_parts:
        _flush()
    current_parts.append(full)
    current_rids.append(rid)
    current_tokens += toks
```

---

### 2.3 Positional Chunking 回退方案

**源文件：** `_compress.py`，第 766–784 行

```python
def _positional_chunks(text: str) -> list[tuple[str | None, str]]:
```

当实体感知分组失败时，使用原始的位置分块作为回退：

1. 调用 `detect_sections(text)` 检测文档章节
2. 如果章节数 < 3：将整个文档按段落平铺，每 `COMPRESS_CHUNK_SIZE`（30）段一个 chunk
3. 如果章节数 ≥ 3：按章节分别分块，每个章节内每 30 段一个 chunk，第一个 chunk 带章节标题

---

## 3. 压缩指南生成（Compress Guide）

### `_generate_compress_guide()` — 生成可复用的提取指南

**源文件：** `_compress.py`，第 219–455 行

```python
def _generate_compress_guide(
    adapter: ModelAdapter,
    prose_text: str,
    schema_columns: list[str] | None = None,
    schema_field_defs: dict[str, str] | None = None,
    schema_field_types: dict[str, str] | None = None,
) -> str | None:
```

#### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `adapter` | `ModelAdapter` | LLM 适配器 |
| `prose_text` | `str` | 完整的散文文本 |
| `schema_columns` | `list[str] \| None` | 目标 schema 的字段名列表 |
| `schema_field_defs` | `dict[str, str] \| None` | 字段语义定义字典 |
| `schema_field_types` | `dict[str, str] \| None` | 字段类型字典 |

#### 返回值

- `str | None`：生成的压缩指南文本，失败时返回 `None`

#### 核心逻辑步骤

1. **前置条件**：如果 `schema_columns` 为空，直接返回 `None`

2. **段落采样**：过滤掉长度 ≤ 50 字符的段落，从均匀间隔位置取最多 20 段作为样本

3. **构建字段定义块**：用 `metadata_by_name()` 获取字段的定义和类型，格式化为 `字段名 | type=类型 | 定义` 的列表

4. **构建 LLM Prompt**：要求 LLM 分析文档样本，生成一份 system prompt，必须覆盖以下内容：

   | 编号 | 主题 | 说明 |
   |------|------|------|
   | 1 | **DOCUMENT LAYOUT** | 文档的组织方式——所有字段在同一段还是分散在多段 |
   | 2 | **FIELD-TO-PROSE MAPPING** | 每个字段在原文中的精确表达模板 |
   | 3 | **CRITICAL DISAMBIGUATION** | 相似字段的区分规则，特别是百分比/比率字段 |
   | 4 | **MULTIPLE NAME FORMS** | 同一实体的多名称形式，选择最短形式的规则 |
   | 5 | **FUZZY NUMBER MARKING** | 近似值的标记规则（添加 `~` 前缀） |

5. Prompt 还包含 **10 条不可协商的硬规则**（A–K），指南必须原文包含：

   - **A. 百分比**：去除 `%` 后缀，不除以 100 转为小数。
     `"2.0%" → 2.0`, `"-1.55%" → -1.55`。

   - **B. 缩写字段三级命名体系**：
     - (1) 全称（全称/正式名称）→ `ChiName` / `fund_name`
     - (2) 市场简称（市场简称/常用名/通用简称/官方简称/公开简称）→ `ChiNameAbbr` / `fund_name_short`
     - (3) 交易简称（证券简称/交易简称）→ `SecuAbbr` / `secuabbr`
     - `ChiNameAbbr` 和 `SecuAbbr` 是不同的字段。当交易简称存在时，绝不将市场简称填入 `SecuAbbr`。
     - 示例：`"市场简称为富国天丰强化收益债券(LOF)…交易简称为富国天丰"` → `ChiNameAbbr = "富国天丰强化收益债券(LOF)"`, `SecuAbbr = "富国天丰"`。

   - **C. 修正值**：只输出最终修正值。`"initially X, confirmed/corrected to Y"` → 输出 Y，不输出 X。

   - **D. 字段类型纪律**：数值 ID/代码不填入文本名称/缩写字段，反之亦然。
     `"证券代码为 512200...交易简称为南华杭州湾区ETF"` → `SecuCode=512200`, `SecuAbbr=南华杭州湾区ETF`（`SecuAbbr=512200` 是错误的）。

   - **E. 语言一致性**：输出值保持源文档语言，不翻译。

   - **F. 值格式保留**：复合值如 `"20/166"`（排名格式），完整输出分子和分母。
     `"在包含 162 家机构的同业评比中，其取得了第20位的排名"` → `"20/162"`。
     `"在37家可比机构中位列第9名"` → `"9/37"`。
     仅在段落中扫描，将分子和分母组合。

   - **G. 模糊日期转换为 `YYYY-MM-DD`**：
     `"2021年终" / "2021日历年结束" / "2021年末"` → `2021-12-31`
     `"2021年第三季度末" / "截至2021年Q3"` → `2021-09-30`
     `"2021年第二季度收官日"` → `2021-06-30`
     `"2021年第一季度末"` → `2021-03-31`
     `"基于2020日历年结束时的数据"` → `2020-12-31`
     仅当文本明确说数据缺失时才留空。

   - **H. 基金字段路由**：`Type` / `FundType` / `InvestmentType` / `InvestStyle` / `FundNature` / `FloatType` / `IfFOF` 的区分规则。
     `Type` 用于产品结构（ETF/LOF/契约型封闭式等），`FundType` 用于基金分类（股票型/混合型等），`InvestmentType` 用于投资类型（指数型/成长型等），`InvestStyle` 用于投资风格（大盘价值/灵活配置型等），`FundNature` 用于基金性质（常规基金/QDII基金），`FloatType` 用于交易渠道（仅场内/场内和场外），`IfFOF` 为布尔 FOF 标志。

   - **I. 共享实体段落**：多个 record ID 共享同一实体时，每个 ID 输出一行，所有共享标识字段复制到每行。

   - **J. 基金字段消歧**：使用 `FUND_FIELD_DISAMBIGUATION_PROMPT` 常量。

   - **K. 输出格式**：必须使用 `field_name: value | field_name: value` 格式，绝不省略字段名。

6. 调用 `adapter.complete(messages)` 获取指南，限制 1200 词以内

#### 关键代码片段

```python
prompt = textwrap.dedent(f"""\
    Below is a representative sample from a large prose data document.
    ...
    Output ONLY the system prompt text (under 1200 words), no wrapping or commentary.""")
messages = [
    ModelMessage(role="system",
        content="You analyze document structures and write precise data extraction instructions."),
    ModelMessage(role="user", content=prompt),
]
response = adapter.complete(messages)
guide = response.content.strip()
```

---

## 4. 逐块压缩（Compress Chunk）

### `compress_chunk()` — 核心压缩函数

**源文件：** `_compress.py`，第 474–763 行

```python
def compress_chunk(
    adapter: ModelAdapter,
    chunk_text: str,
    schema_columns: list[str] | None = None,
    primary_key: str | None = None,
    anchor_keys: list[str] | None = None,
    schema_field_types: dict[str, str] | None = None,
    schema_field_defs: dict[str, str] | None = None,
    compress_guide: str | None = None,
    entity_aware: bool = False,
) -> str:
```

#### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `adapter` | `ModelAdapter` | LLM 适配器 |
| `chunk_text` | `str` | 待压缩的 chunk 文本 |
| `schema_columns` | `list[str] \| None` | 目标 schema 字段名列表 |
| `primary_key` | `str \| None` | 主键字段名 |
| `anchor_keys` | `list[str] \| None` | 锚定字段列表（标识字段） |
| `schema_field_types` | `dict[str, str] \| None` | 字段类型字典 |
| `schema_field_defs` | `dict[str, str] \| None` | 字段语义定义字典 |
| `compress_guide` | `str \| None` | 可选的压缩指南（来自 `_generate_compress_guide`） |
| `entity_aware` | `bool` | 是否为实体感知模式 |

#### 返回值

- `str`：LLM 返回的压缩后文本。行格式为 `field_name: value | field_name: value | ...`

#### 核心逻辑步骤

**Step 1：构建 Schema 提示**

当 `schema_columns` 和 `primary_key` 都存在时：

- `schema_hint`：列出允许的字段名，强调不得改名、缩写或释义
- `type_hint`：调用 `_format_schema_type_hint()` 生成字段类型约束
- `hard_type_rules`：调用 `_format_hard_type_rules()` 生成硬类型规则
- `format_hint`：根据 `entity_aware` 选择不同的输出格式说明
  - **实体感知模式**：每个实体组输出一行，从 `[RECORD_ID: N]` 标记获取主键，合并所有段落
  - **非感知模式**：每段输出一行，主键从段落自己的记录标签获取
- `anchor_rule`：锚定字段规则，强调必须包含标识字段

当 schema 不存在时，使用默认的字段名 `ID` 和 `entity_name`。

**Step 2：构建稀疏字段规则**

- 如果字段数 > 10（稀疏 schema）：未提及的字段直接跳过，不输出
- 如果字段数 ≤ 10（密集 schema）：值为 `0.0` / `None` / `NaN` / placeholder 时输出 `(placeholder)` 标记

**Step 3：构建完整压缩规则块**

无论 schema 是否存在，均包含以下硬编码规则：

| 规则 | 内容 |
|------|------|
| **语言一致性** | 输出值保持原文档语言，不翻译 |
| **值格式保留** | 复合值保持完整格式（如 `20/166`） |
| **共享实体** | 多个 record ID 共享实体时，每个 ID 输出一行，所有共享标识字段复制 |
| **字段标签** | 每个值前必须有描述性字段标签 |
| **无值留空** | 无合适值时省略该字段，不填充背景描述 |
| **限定符绑定** | 不将全局值填入分类限定字段 |
| **记录标签处理** | 本地记录标签只取数字部分，不输出范围（如 `197至217`） |
| **PersonalCode** | 不填入姓名或本地记录号 |
| **单位后缀** | 去除单位后缀（如 `cm`） |
| **百分比处理** | 去除 `%`；比值形式 `0.001910` 表示比例时乘 100 转为百分比 |
| **数值一致性校验** | 持仓量和百分比交叉校验；"前"百分比 > "后"百分比；段落中与持股数同句的百分比才是对应持仓百分比 |
| **中文数字** | 中文数字转阿拉伯数字（两=2，亿=1e8，万=1e4） |
| **模糊数字** | 约数加 `~` 前缀（如 `~6970000`），不尝试算术精确化 |
| **修正值** | 只输出最终修正值 |
| **URL/名称/日期保留** | 保留原始值 |
| **空响应** | 无提取内容时返回空响应 |

**Step 4：调用 LLM**

```python
system_content = compress_guide or "You are a data extraction preprocessor."
messages = [
    ModelMessage(role="system", content=system_content),
    ModelMessage(role="user", content=prompt),
]
return adapter.complete(messages).content.strip()
```

#### 压缩规则详解

##### 百分比处理

```
# 源码中通过 prompt 规则实现
# 去除 "%" 后缀，按原样输出数字
# "2.0%" → 2.0, "-1.55%" → -1.55
# 比值形式（如 0.001910 表示比例）→ 乘以 100 → 0.191
```

##### 数字归一化

```
# 中文数字转阿拉伯数字（prompt 规则）
# 两=2, 一点五=1.5, 亿=1e8, 万=1e4
# "两亿元" → 200000000
# "一点五亿元" → 150000000
```

##### 日期格式

```
# 自然语言日期转 YYYY-MM-DD（prompt 规则）
# "2021年终" / "2021日历年结束" / "2021年末" → 2021-12-31
# "2021年第三季度末" / "截至2021年Q3" → 2021-09-30
# "2021年第二季度收官日" → 2021-06-30
# "2021年第一季度末" → 2021-03-31
# "基于2020日历年结束时的数据" → 2020-12-31
```

##### 模糊数字标记

```
# 约数加 ~ 前缀
# "约六百九十七万股" → ~6970000
# 精确数字不加前缀
```

---

## 5. 实体完整性校验

### 5.1 校验逻辑

**源文件：** `_compress.py`，第 880–970 行

实体完整性校验发生在 `compress_prose()` 主函数中，在所有 chunk 压缩完成后执行。

#### 核心逻辑步骤

1. **提取 PK**：对每个 chunk 的输出文本，调用 `_extracted_pks()` 提取所有主键值
2. **对比预期**：将提取的 PK 集合与 chunk 的预期 record ID 集合对比
3. **分类处理**：
   - `bogus = got - expected`：模型伪造的 PK（忽略 `[RECORD_ID]` 标记，自行顺序编号），记录 warning
   - `missing = expected - got`：缺失的实体，记录 warning 并加入恢复队列

#### 关键代码片段

```python
def _extracted_pks(chunk_text: str) -> set[str]:
    from agents.etl._record import normalize_local_record_ids, parse_kv_text
    table = parse_kv_text(
        chunk_text,
        columns=schema_columns or [],
        primary_key=primary_key,
        anchor_keys=anchor_keys,
        field_types=schema_field_types,
    )
    normalize_local_record_ids(table)
    return {record.pk.strip() for record in table.records if record.pk}
```

### 5.2 定向恢复机制

当发现缺失实体时，执行**定向恢复传递**（targeted recovery pass）：

1. 收集所有缺失实体的段落（`subset = {rid: entity_groups[rid] for rid in to_recover}`）
2. 对缺失实体子集调用 `entity_chunks_by_token_budget()` 重新打包 chunk
3. 对每个恢复 chunk 调用 `compress_chunk(adapter, ..., entity_aware=True)`
4. 记录恢复结果，分别统计成功恢复和仍然缺失的实体

#### 关键代码片段

```python
if to_recover:
    subset = {rid: entity_groups[rid] for rid in sorted(to_recover) if rid in entity_groups}
    for retry_text, _ in entity_chunks_by_token_budget(subset):
        result = compress_chunk(adapter, retry_text, ..., entity_aware=True)
        if result:
            recovered_parts.append(result)
```

---

## 6. 并行处理

### `compress_prose()` 中的并行架构

**源文件：** `_compress.py`，第 787–980 行

```python
def compress_prose(
    adapter: ModelAdapter,
    text: str,
    schema_columns: list[str] | None = None,
    primary_key: str | None = None,
    anchor_keys: list[str] | None = None,
    schema_field_types: dict[str, str] | None = None,
    schema_field_defs: dict[str, str] | None = None,
) -> tuple[str, dict[str, list[str]] | None, str | None]:
```

#### 返回值

- `tuple[str, dict[str, list[str]] | None, str | None]`：
  - `compressed_text`：压缩后的完整文本
  - `entity_groups`：实体分组映射（positional chunking 时为 `None`）
  - `compress_guide`：生成的压缩指南（无 schema 时为 `None`）

#### `ThreadPoolExecutor` 的使用

```python
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:  # MAX_WORKERS = 4
    futs = {
        submit_in_context(pool, compress_chunk, adapter, item[1], ...): idx
        for idx, item in enumerate(work)
    }
```

#### 超时处理

```python
# 总超时 = LLM_CALL_TIMEOUT * chunk 数（LLM_CALL_TIMEOUT = 180 秒）
for fut in as_completed(futs, timeout=LLM_CALL_TIMEOUT * len(work)):
    try:
        compressed[idx] = fut.result(timeout=LLM_CALL_TIMEOUT)
    except TimeoutError:
        logger.warning("Compress chunk %d timed out", idx)
    except Exception as exc:
        logger.warning("Compress chunk %d failed: %s", idx, exc)
```

#### 部分结果处理策略

- **单个 chunk 超时**：记录 warning，`compressed[idx]` 保持 `None`，不中断整体流程
- **整个 pool 超时**：`as_completed` 抛出 `TimeoutError`，记录 warning，用已完成的 partial results 继续
- **失败 chunk**：由实体完整性校验阶段覆盖，缺失实体进入恢复传递

#### `submit_in_context()` — ContextVar 传播

**源文件：** `_threading.py`

```python
def submit_in_context(
    pool: ThreadPoolExecutor, fn: Callable[..., Any], *args: Any, **kwargs: Any
) -> Future[Any]:
    ctx = contextvars.copy_context()
    return pool.submit(ctx.run, fn, *args, **kwargs)
```

确保线程池中的每个 worker 线程继承当前上下文的 `ContextVar` 值（如日志上下文、请求追踪 ID 等）。

---

## 7. 辅助函数

### 7.1 `sample_paragraphs()` — 段落采样

**源文件：** `_compress.py`，第 138–156 行

```python
def sample_paragraphs(text: str, first_n: int = 3, stride_n: int = 10) -> str:
    """Sample paragraphs: first few + evenly spaced from the rest of the file.
    This ensures the LLM sees all paragraph types (identity, physical stats,
    demographics, classification, etc.) rather than only the first section."""
```

#### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `text` | `str` | — | 完整的散文文本 |
| `first_n` | `int` | `3` | 从开头取的段落数 |
| `stride_n` | `int` | `10` | 从剩余段落中均匀采样的目标段落数 |

#### 返回值

- `str`：采样后的段落文本，仍用 `\n\n` 连接

#### 核心逻辑

1. 按 `\n\n` 拆分为段落
2. 如果段落数 ≤ `first_n + stride_n`，返回全部段落
3. 否则：
   - 取前 `first_n` 段
   - 从剩余段落中均匀采样，按步长 `step = max(1, len(remaining) // stride_n)` 取段
   - 最多取 `first_n + stride_n` 段

---

### 7.2 `sample_paragraphs_by_section()` — 按章节采样

**源文件：** `_compress.py`，第 159–216 行

```python
def sample_paragraphs_by_section(
    text: str, token_limit: int = SCHEMA_SAMPLE_TOKEN_BUDGET  # 默认 8000
) -> str:
    """Sample whole paragraphs for schema field discovery.
    Small documents pass through IN FULL — complete evidence beats any
    sample. Larger documents get a token budget spread across sections in
    proportion to their size, with CENTERED systematic sampling inside each
    section (k=1 picks the middle paragraph)."""
```

#### 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `text` | `str` | — | 完整的散文文本 |
| `token_limit` | `int` | `8000` | token 预算上限 |

#### 返回值

- `str`：采样后的段落文本，仍用 `\n\n` 连接

#### 核心逻辑

1. 调用 `detect_sections(text)` 检测章节
2. 如果无章节或章节数 < 3，将整个文档视为一个章节
3. 从每个章节的中心位置进行**系统采样**（centered systematic sampling），公式为：
   ```python
   indices = sorted({round((j + 0.5) * len(paras) / k - 0.5) for j in range(k)})
   ```
   `k=1` 时取中间段落，避免边界段落（intro/outro）的弱信号
4. 先用 `scale = 1.0` 尝试全部采样，如果 token 超出预算，通过 `scale = token_limit / total_tokens` 逐步降低采样比例（最多 8 次迭代，每次 `scale *= 0.8`）
5. 如果每章节一段仍超出预算，将整个文档展平做均匀采样

---

### 7.3 `_field_type_instruction()` — 字段类型指令生成

**源文件：** `_compress.py`，第 32–58 行

```python
def _field_type_instruction(field: str, field_type: str) -> str:
```

#### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `field` | `str` | 字段名 |
| `field_type` | `str` | 字段类型字符串 |

#### 返回值

- `str`：单条字段类型指令文本

#### 类型匹配规则

| 字段类型特征 | 指令内容 |
|-------------|---------|
| `local_record_id` 或 `integer_scalar` | 提取单个标量整数，拒绝范围 `"197至217"` 和分组/列表 |
| `number` 或 `integer`（不含 `rank`） | 提取显式数字标量，去除单位后缀 |
| `date` | 提取显式报告日期/期间，修正值取最终值 |
| `time` | 提取显式的时钟/时间戳值 |
| `id` 或 `identity` | 提取单个标量标识值，不使用范围、分组标签或叙事描述 |
| 其他 | 提取该类型的显式值 |

---

### 7.4 `_format_schema_type_hint()` — 类型提示格式化

**源文件：** `_compress.py`，第 61–79 行

```python
def _format_schema_type_hint(
    schema_columns: list[str] | None,
    schema_field_types: dict[str, str] | None,
) -> str:
```

#### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `schema_columns` | `list[str] \| None` | 字段名列表 |
| `schema_field_types` | `dict[str, str] \| None` | 字段类型字典 |

#### 返回值

- `str`：格式化的类型约束文本，空列表时返回 `""`

#### 核心逻辑

1. 调用 `metadata_by_name(schema_columns, schema_field_types)` 获取大小写不敏感的字段类型映射
2. 对每个字段调用 `_field_type_instruction()` 生成类型指令
3. 以 `"\nField type constraints (obey strictly; omit values that violate the type):\n"` 为前缀连接所有指令

#### 输出示例

```
Field type constraints (obey strictly; omit values that violate the type):
- record_id: integer_scalar. Extract one scalar integer only. Convert labels like "档案 <N>" or "Record <N>" to "<N>". Reject ranges/groups/lists such as "197至217"; if the paragraph only states missing values for such a range/group, skip the paragraph.
- holdingshares: number. Extract only an explicit numeric scalar that matches this exact field; remove unit suffixes.
```

---

### 7.5 `_format_hard_type_rules()` — 硬类型规则格式化

**源文件：** `_compress.py`，第 82–135 行

```python
def _format_hard_type_rules(
    schema_columns: list[str] | None,
    schema_field_types: dict[str, str] | None,
) -> str:
```

#### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `schema_columns` | `list[str] \| None` | 字段名列表 |
| `schema_field_types` | `dict[str, str] \| None` | 字段类型字典 |

#### 返回值

- `str`：格式化的硬类型规则文本

#### 核心逻辑

1. 始终包含两条通用规则：
   - 数字值中不加千位分隔符（`7031834` 而非 `7,031,834`）
   - 类型不匹配时留空，名称/描述不填入数字/日期/布尔字段

2. 根据字段类型分类生成针对性规则：
   - **数字字段**（`number`/`integer`，不含 `rank`）：仅数字值，去除单位后缀和千位符，允许 `~` 前缀
   - **日期字段**：统一转为 `YYYY-MM-DD`（月/日未知时用 `YYYY-MM` 或 `YYYY`）
   - **布尔字段**：归一化为 `true` 或 `false`

---

### 7.6 `_format_schema_def_hint()` — 字段定义提示格式化

**源文件：** `_compress.py`，第 458–471 行

```python
def _format_schema_def_hint(
    schema_columns: list[str] | None,
    schema_field_defs: dict[str, str] | None,
) -> str:
```

#### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `schema_columns` | `list[str] \| None` | 字段名列表 |
| `schema_field_defs` | `dict[str, str] \| None` | 字段语义定义字典 |

#### 返回值

- `str`：格式化的字段定义提示文本，空列表时返回 `""`

#### 输出格式

```
Field semantic definitions — match values strictly by these meanings, NOT by proximity or order of appearance in the text:
  - field_a: 字段定义
  - field_b: 字段定义
```

---

### 7.7 `metadata_by_name()` — 元数据大小写不敏感映射

**源文件：** `_columns.py`，第 11–24 行

```python
def metadata_by_name(
    names: Iterable[str],
    metadata: Mapping[str, T] | None,
) -> dict[str, T]:
    """Return metadata remapped to the spelling used by *names*."""
```

#### 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `names` | `Iterable[str]` | 目标字段名列表（使用 active schema 的拼写） |
| `metadata` | `Mapping[str, T] \| None` | 原始元数据字典（可能使用不同的大小写） |

#### 返回值

- `dict[str, T]`：以 `names` 中拼写为键的元数据字典

#### 核心逻辑

1. 将元数据字典的键全部转为小写
2. 对每个 name，以其小写形式在转换后的字典中查找
3. 以 name 的原始拼写（来自 `names`）作为输出的键

---

## 8. 完整压缩流程

### `compress_prose()` 整体流程

```
输入：prose_text + schema 信息
  │
  ├─ 1. 实体感知分组
  │   ├─ group_paragraphs_by_entity(text, adapter)
  │   │   ├─ 成功 → entity_groups, leftover
  │   │   └─ 失败 → _positional_chunks(text) [回退]
  │   └─ entity_chunks_by_token_budget(entity_groups) → chunks
  │
  ├─ 2. 生成压缩指南
  │   └─ _generate_compress_guide(adapter, text, ...) → guide
  │
  ├─ 3. 并行压缩所有 chunk
  │   └─ ThreadPoolExecutor(max_workers=4)
  │       └─ 每个 chunk → compress_chunk(adapter, chunk_text, ...)
  │
  ├─ 4. 实体完整性校验
  │   ├─ _extracted_pks(chunk_result) → got PKs
  │   ├─ expected vs got → missing / bogus
  │   └─ 定向恢复（缺失实体）
  │       └─ compress_chunk(adapter, retry_text, entity_aware=True)
  │
  └─ 输出：压缩文本 + entity_groups + compress_guide
```

---

## 9. 常量定义

### 9.1 压缩相关常量

| 常量名 | 值 | 说明 |
|--------|-----|------|
| `COMPRESS_CHUNK_SIZE` | `30` | 位置分块每段数 |
| `MAX_WORKERS` | `4` | 并行线程数 |
| `LLM_CALL_TIMEOUT` | `180`（秒） | 单次 LLM 调用超时 |
| `ENTITY_CHUNK_TOKEN_BUDGET` | `2500` | 实体打包 token 预算 |
| `ENTITY_SEPARATOR` | `\n\n---\n\n` | 实体间分隔符 |
| `SCHEMA_SAMPLE_TOKEN_BUDGET` | `8000` | Schema 采样 token 预算 |

### 9.2 分组相关常量

| 常量名 | 值 | 说明 |
|--------|-----|------|
| `GROUPING_BATCH_SIZE` | `40` | 每批发送给 LLM 的段落数 |
| `GROUPING_TRUNCATE_CHARS` | `300` | 每个段落截断的字符数 |
| `RECORD_ID_KEYWORDS` | `"档案\|记录\|条目\|战略单元\|Record\|File\|Case"` | 记录 ID 关键词 |
| `RECORD_ID_SEPARATOR` | `\s*(?:\(?(?:ID\|id\|Id)[：:]\s*)?[：:#\-]?\s*` | 记录 ID 分隔符 |
| `RECORD_ID_INLINE` | 编译后的正则 | 行内记录 ID 匹配 |
| `RECORD_ID_LABEL` | 编译后的正则 | 独立记录标签匹配 |
| `RECORD_ID_RANGE` | 编译后的正则 | 记录 ID 范围匹配 |

### 9.3 实体标记常量

| 常量名 | 值 | 说明 |
|--------|-----|------|
| `AIRTABLE_ID_RE` | `\brec[A-Z0-9][A-Za-z0-9]{9,19}\b` | Airtable 风格 ID |
| `CODE_ID_RE` | `\b([A-Z]{2,4})(\d{3,6})\b` | 代码风格 ID |
| `APPROX_PREFIXES` | `("~", "～")` | 模糊数字前缀 |
| `PLACEHOLDER_VALS` | `{"", "-", "null", "none", "nan", "n/a", "placeholder", ...}` | 占位符值集合 |
| `BOOL_TRUE` | `{"true", "yes", "1", "是"}` | 布尔真值集合 |
| `BOOL_FALSE` | `{"false", "no", "0", "否"}` | 布尔假值集合 |
| `NUMERIC_SCALAR` | `^[+-]?(?:\d+(?:\.\d+)?\|\.\d+)(?:[eE][+-]?\d+)?$` | 数字标量验证 |
| `DATE_SCALAR` | `^\d{4}(?:[-/]\d{1,2}(?:[-/]\d{1,2})?)?$` | 日期标量验证 |

---

## 10. 依赖关系

```
_compress.py
  ├── _columns.py        → metadata_by_name()
  ├── _constants.py      → COMPRESS_CHUNK_SIZE, MAX_WORKERS, LLM_CALL_TIMEOUT, etc.
  ├── _grouping.py       → entity_chunks_by_token_budget(), group_paragraphs_by_entity()
  ├── _detect.py         → detect_sections()
  ├── _threading.py      → submit_in_context()
  ├── _record.py         → parse_kv_text(), normalize_local_record_ids()
  ├── _anchors.py        → paragraph_record_ids(), discover_record_id_patterns()
  ├── _types.py          → contains_standalone_number(), has_standalone_id(),
  │                        split_approx_tag(), is_placeholder(), normalize_boolean()
  └── agents/llm/        → ModelAdapter, ModelMessage, count_qwen_tokens()
```

---

## 11. 复现指南

要复现 Compress 阶段的行为，需要：

1. **准备 LLM 适配器**：实现 `ModelAdapter` 接口，包含 `complete(messages) → ModelResponse` 方法
2. **准备 Token 计数器**：实现 `count_qwen_tokens(text) → int` 函数（使用 Qwen 分词器）
3. **准备 schema 信息**：字段名列表、字段类型字典、字段定义字典
4. **调用入口**：`compress_prose(adapter, text, schema_columns, primary_key, anchor_keys, schema_field_types, schema_field_defs)`
5. **解析输出**：从返回的压缩文本中解析 `key: value | key: value` 行

### 最小复现示例

```python
from agents.etl._compress import compress_prose
from agents.llm.types import ModelAdapter

# 准备 LLM 适配器
adapter = ModelAdapter(...)

# 原始散文文本
text = """
### 档案 29
持有 244 万股，占总股本 1.55%。涉及 30 万股，占总股本 0.00191%。
"""

# Schema 信息
schema_columns = ["record_id", "holdingshares", "holdingpct", "transfershares"]
primary_key = "record_id"
anchor_keys = ["record_id"]
schema_field_types = {
    "record_id": "integer_scalar",
    "holdingshares": "number",
    "holdingpct": "number",
    "transfershares": "number",
}

# 执行压缩
compressed_text, entity_groups, guide = compress_prose(
    adapter=adapter,
    text=text,
    schema_columns=schema_columns,
    primary_key=primary_key,
    anchor_keys=anchor_keys,
    schema_field_types=schema_field_types,
)

# 预期输出格式
# record_id: 29 | holdingshares: 2440000 | holdingpct: 1.55 | transfershares: 300000
```

### 复现注意事项

1. **实体感知分组依赖 LLM**：如果不提供 `adapter`，`group_paragraphs_by_entity()` 会退化为纯 pattern 匹配，可能降低分组质量
2. **Token 预算**：`ENTITY_CHUNK_TOKEN_BUDGET = 2500` 和 `COMPRESS_CHUNK_SIZE = 30` 可根据目标 LLM 的上下文窗口调整
3. **超时设置**：`LLM_CALL_TIMEOUT = 180` 秒是单次 LLM 调用的超时；pool 总超时 = `LLM_CALL_TIMEOUT * chunk_count`
4. **压缩指南**：`compress_guide` 为可选参数，但提供后能显著提升不同 chunk 间的提取一致性
5. **字段名大小写**：`metadata_by_name()` 使用大小写不敏感匹配，schema 字段名拼写差异会被自动处理