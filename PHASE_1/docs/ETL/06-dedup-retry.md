# Phase 5 - Dedup（去重）与 Phase 6 - Retry（空值重试）

## 1. 概述

### 1.1 管线位置

Dedup（去重）和 Retry（空值重试）是 ETL 管线中 **Phase 5** 和 **Phase 6** 两个后续修正阶段。它们运行在压缩（Compress）→ 分组（Grouping）→ 合并（Merge）→ 归一化（Normalize）之后，在最终验证（Verify）阶段之前。整个管线阶段顺序如下：

```
Compress → Grouping → Merge → Normalize → Dedup → Retry → Verify
```

这两个阶段直接操作 `EntityTable`（**in place** 修改），不重建表结构。

### 1.2 核心数据结构

两个阶段均操作 `EntityTable` 内存表，其定义位于 `_record.py`：

```python
@dataclass(slots=True)
class EntityTable:
    columns: list[str]          # 所有列名
    primary_key: str | None     # 主键列名
    anchor_keys: list[str]      # 锚点列名列表
    field_types: dict[str, str] # 字段名 → 类型名
    records: list[Record]       # 所有记录
    rejects: list[Reject]       # 解析失败的记录
    conflicts: int              # 行内重复键（值不一致）计数

@dataclass(slots=True)
class Record:
    fields: dict[str, str]      # 列名 → 值
    pk: str | None              # 主键值
    approx: set[str]            # 带 ~ 模糊标记的字段名
    provenance: dict[str, str]  # 字段 → 最后写入者
    source_chunk: int | None    # 来源块编号
```

### 1.3 值落地路径

所有 LLM 修正值经 `_apply_llm_value()` 进入 `Record.fields`，路径为：

```
LLM 原始文本 → sanitize_llm_value() → clean_cell_for_type() → set_record_field()
```

不存在正则回填，值内容不可能破坏行结构。

---

## 2. Dedup - `dedup_cross_field_copies()`

### 2.1 问题描述

压缩阶段可能将同一数值错误地复制到同一实体的不同字段中。例如，`reserveassets`（储备资产）的值被错误地填入 `totalassets`（总资产）字段。Dedup 阶段检测此类跨字段重复，并用 LLM 判定每个重复值实际属于哪个字段，然后清除错误字段的值，供后续 Retry 阶段重新提取。

### 2.2 函数签名

```python
def dedup_cross_field_copies(
    adapter: ModelAdapter,
    table: EntityTable,
    source_text: str | None,
) -> dict[str, list[str]]:
```

### 2.3 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `adapter` | `ModelAdapter` | LLM 模型适配器，提供 `complete(messages)` 方法 |
| `table` | `EntityTable` | 内存表，**会被原地修改** |
| `source_text` | `str \| None` | 原始源文本全文，用于提取值上下文片段。为 `None` 时直接返回空字典 |

### 2.4 返回值

```python
dict[str, list[str]]  # {pk_val: [cleared_col, ...]}
```

- 键：被清除字段的实体主键值
- 值：被清空的列名列表
- 空字典表示没有发现需要清除的重复

### 2.5 核心逻辑步骤

#### 步骤 1：边界检查

```python
primary_key = table.primary_key
if not primary_key or not source_text or not table.records:
    return {}
```

主键、源文本、记录三者任一缺失则直接返回空。

#### 步骤 2：构建数据列列表

跳过锚点列（anchor keys），只检查数据列：

```python
skip_lower = {a.lower() for a in table.anchor_keys}
data_cols = [c for c in table.columns if c.lower() not in skip_lower]
```

#### 步骤 3：收集跨字段重复值对

遍历每条记录，对每个数据列提取数值（去逗号后尝试转为 float），按值分组：

```python
dupes: list[tuple[str, str, list[str], str]] = []
# 格式: (pk_val, value, [col1, col2, ...], context_snippet)
```

关键逻辑：
- 值去逗号（`replace(",", "")`）后为空则跳过
- 只处理能转为 float 的数值
- 同一值出现在 2 个及以上列时才视为重复候选
- 对每个重复候选，调用 `_extract_value_context()` 从源文本中提取上下文片段

#### 步骤 4：构建 LLM 提示

将所有重复项拼成带编号的列表，每个条目提供主键、重复值、涉及的列名和源上下文：

```
The same numeric value appears in multiple columns for each entity
below. Based on the source context, determine whether the value
genuinely belongs to ALL listed columns or only ONE of them.

<item_number>:BOTH   — if the source text independently states this value for each column
<item_number>:<column_to_keep>  — if only one column should have this value
```

#### 步骤 5：调用 LLM 获取判定

```python
response = adapter.complete(messages)
```

系统角色消息要求 LLM 输出格式为 `item_number:column_to_keep` 或 `item_number:BOTH`。

#### 步骤 6：解析 LLM 响应

```python
raw = response.content.strip()
raw = re.sub(r"  thinking.*? response", "", raw, flags=re.DOTALL).strip()
```

先移除思维链内容（如果模型包含 `  thinking... response` 格式的推理过程），然后逐行解析：

- 每行格式：`<编号>:<列名>` 或 `<编号>:BOTH`
- 编号从 1 开始，转为 0-based 索引
- `keep_col` 小写化后比较
- 如果为 `both`，跳过（保留所有字段）
- 如果指定的列名在 `cols` 中匹配到唯一一个，则清除其他所有列

#### 步骤 7：清除错误字段

```python
by_pk = _records_by_pk(table)
for pk_val, cols in to_clear.items():
    for record in by_pk.get(pk_val, []):
        for col in cols:
            if not record.fields.get(col, "").strip():
                continue
            set_record_field(record, col, "", "dedup")
```

只清除当前非空的字段，已经为空的字段跳过。

### 2.6 关键代码片段

```python
# 重复值检测
for record in table.records:
    if not record.pk:
        continue
    val_to_cols: dict[str, list[str]] = {}
    for c in data_cols:
        v = record.fields.get(c, "").strip().replace(",", "")
        if not v:
            continue
        try:
            float(v)
        except ValueError:
            continue
        val_to_cols.setdefault(v, []).append(c)
    for val, cols in val_to_cols.items():
        if len(cols) < 2:
            continue
        ctx = _extract_value_context(val, source_text)
        dupes.append((record.pk, val, cols, ctx))
```

---

## 3. 辅助函数

### 3.1 `_records_by_pk()`

按主键分组记录。

```python
def _records_by_pk(table: EntityTable) -> dict[str, list[Record]]:
```

**返回：** `{pk_value: [Record, ...]}`

**逻辑：** 遍历所有记录，对有 `pk` 的记录按主键值分组。一个主键值可能对应多条记录（合并前状态）。

### 3.2 `_apply_llm_value()`

LLM 修正值落地的唯一路径。

```python
def _apply_llm_value(
    table: EntityTable,
    record: Record,
    col: str,
    val: str,
    writer: str,
) -> None:
```

**参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | `EntityTable` | 内存表，用于获取字段类型元数据 |
| `record` | `Record` | 要修改的记录 |
| `col` | `str` | 目标列名 |
| `val` | `str` | LLM 返回的原始值文本 |
| `writer` | `str` | 写入者标识（如 `"retry"`、`"verify"`） |

**处理流程：**

```
val → sanitize_llm_value(val) → clean_cell_for_type(..., field_types.get(col), field_name=col) → set_record_field(record, col, cleaned, writer)
```

- `sanitize_llm_value()`：推理泄漏截断 + 控制字符清理 + 长度上限
- `clean_cell_for_type()`：根据字段类型做类型清洗（数值校验、日期格式校验、布尔值归一化、名称字段数字拦截等）
- `set_record_field()`：将清洗后的值写入 Record，维护 `approx` 和 `provenance`

### 3.3 `sanitize_llm_value()`

LLM 文本进入 `Record.fields` 的唯一收口函数。

```python
def sanitize_llm_value(val: str) -> str:
```

**输入：** LLM 返回的原始文本字符串

**返回值：** 清洗后的安全字符串

**处理流程：**

1. **推理泄漏截断：** 调用 `strip_reasoning_leak(val)`，检测并截断 LLM 推理尾巴
2. **控制字符清理：** 用正则 `[\x00-\x1f\x7f]` 将所有控制字符替换为空格
3. **空白折叠：** `" ".join(cleaned.split())` 将连续空白折叠为单个空格
4. **长度截断：** 取前 `MAX_LLM_VALUE_CHARS`（500）字符，尾部 rstrip

```python
def sanitize_llm_value(val: str) -> str:
    cleaned = strip_reasoning_leak(val)
    cleaned = _CONTROL_CHARS.sub(" ", cleaned)
    cleaned = " ".join(cleaned.split())
    return cleaned[:MAX_LLM_VALUE_CHARS].rstrip()
```

### 3.4 `strip_reasoning_leak()`

检测并截断 LLM 泄漏到字段值中的推理文本。

```python
def strip_reasoning_leak(val: str) -> str:
```

**正则模式 `_REASONING_LEAK`：**

```python
_REASONING_LEAK = re.compile(
    r"\s*\(?"
    r"(?:"
    # English patterns
    r"Source text|However,|Wait,|Let me|Let's|Note:|The (?:text|context|extraction|source)"
    r"|strictly following|implies the|this (?:is|means|suggests)"
    # Chinese patterns
    r"|或根据|原文指代|原文(?:是|写|说|提到)|通常指|然而，|但仔细|但是，"
    r"|如果源文本|这看起来|根据文档|根据上下文|仔细阅读"
    r")"
    r".*",
    re.IGNORECASE | re.DOTALL,
)
```

**逻辑：** 如果正则匹配到推理泄漏模式且匹配位置不在字符串开头（`m.start() > 0`），则在匹配位置截断，并去掉尾部多余的 ` ,;|` 字符。

例如：`"23890000 然而，根据上下文这里应该是"` → `"23890000"`

### 3.5 `clean_cell_for_type()`

按字段类型清洗单元格值。

```python
def clean_cell_for_type(
    val: str,
    field_type: str | None = None,
    field_name: str | None = None,
) -> str:
```

**处理流程：**

1. 剥离 `~` 近似标记
2. 调用 `clean_cell()` 处理占位符和单位后缀
3. 调用 `strip_reasoning_leak()` 截断推理泄漏
4. 如果是 fund_type 字段，调用 `_normalize_fund_type()` 归一化基金类型后缀
5. 如果是名称/缩写字段（`_NAME_ABBR_FIELDS`），拦截纯数字值
6. 按类型清洗：
   - **number/integer 类型（非 rank）：** 去逗号、去 `%` 后缀，用 `NUMERIC_SCALAR` 正则校验
   - **date 类型：** 用 `DATE_SCALAR` 正则校验
   - **boolean 类型：** 调用 `normalize_boolean()` 归一化
   - **其他类型：** 保留原始值

### 3.6 `set_record_field()`

在 Record 上存储清洗后的单元格值。

```python
def set_record_field(record: Record, col: str, val: str, writer: str) -> None:
```

**逻辑：**
1. 调用 `split_approx_tag(val)` 分离值和 `~` 标记
2. 将裸值存入 `record.fields[col]`
3. 如果值有 `~` 标记且非空，加入 `record.approx`；否则从中移除
4. 设置 `record.provenance[col] = writer` 记录写入者

### 3.7 `_value_search_needles()`

为数值搜索生成关键词变体。

```python
def _value_search_needles(value_str: str) -> list[str]:
```

**输入：** 数值字符串，如 `"1234567"`

**输出：** 搜索关键词列表，如 `["1234567", "1,234,567"]`

**逻辑：**
1. 去掉逗号得到 `plain` 形式
2. 如果整数部分超过 3 位，从右向左每 3 位添加逗号，生成 `comma_form`
3. 返回 `[plain]` 或 `[plain, comma_form]`（如果逗号形式不同）

### 3.8 `_extract_value_context()`

从源文本中提取数值上下文片段。

```python
def _extract_value_context(
    value_str: str,
    source_text: str,
    *,
    max_hits: int = 3,
) -> str:
```

**参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `value_str` | `str` | - | 要查找的数值字符串 |
| `source_text` | `str` | - | 源文本全文 |
| `max_hits` | `int` | `3` | 最多提取的上下文片段数 |

**返回值：** 用 ` [...] ` 连接的上下文片段字符串。

**逻辑：**
1. 调用 `_value_search_needles(value_str)` 生成搜索关键词
2. 对每个关键词在源文本中正则搜索（`re.escape(needle)`）
3. 对每个匹配位置，取前后各 `DEDUP_CONTEXT_WINDOW`（300）字符作为上下文
4. 去重（按起始位置），最多返回 `max_hits` 个片段

### 3.9 `metadata_by_name()`

元数据按列名拼写重映射。

```python
def metadata_by_name(
    names: Iterable[str],
    metadata: Mapping[str, T] | None,
) -> dict[str, T]:
```

**逻辑：** 将元数据字典（键为 schema 拼写）按 `names` 的实际拼写（大小写不敏感）重映射。例如，如果 `metadata` 中有 `{"TotalAssets": "数值"}`，而 `names` 包含 `"totalassets"`，则返回 `{"totalassets": "数值"}`。

---

## 4. Retry - `retry_missing_values()`

### 4.1 问题描述

某些列在大多数实体中都有值（高填充率），但少数实体在该列上为空。Retry 阶段检测这些"空洞"，对每个缺失实体从原始段落中定向重试提取，只回填仍为空的格。

### 4.2 函数签名

```python
def retry_missing_values(
    adapter: ModelAdapter,
    table: EntityTable,
    entity_groups: dict[str, list[str]] | None,
    schema_field_defs: dict[str, str] | None = None,
    forced_gaps: dict[str, list[str]] | None = None,
) -> None:
```

### 4.3 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `adapter` | `ModelAdapter` | LLM 模型适配器 |
| `table` | `EntityTable` | 内存表，**会被原地修改** |
| `entity_groups` | `dict[str, list[str]] \| None` | 实体分组结果，键为实体 ID，值为段落文本列表。为 `None` 时直接返回 |
| `schema_field_defs` | `dict[str, str] \| None` | 可选字段定义字典，用于提示中增加字段说明 |
| `forced_gaps` | `dict[str, list[str]] \| None` | 可选强制回填空洞，键为实体 ID，值为列名列表。这些空洞会合并到自动检测的 `gaps` 中 |

### 4.4 返回值

无返回值（`None`）。`table` 被原地修改。

### 4.5 核心逻辑步骤

#### 步骤 1：边界检查

```python
if not table.primary_key or not entity_groups or not table.columns:
    return
```

#### 步骤 2：检测空洞单元格

调用 `_detect_gap_cells(table)` 获取自动检测的空洞：

```python
gaps = _detect_gap_cells(table)
```

如果提供了 `forced_gaps`，合并到 `gaps` 中：

```python
if forced_gaps:
    for pk_val, cols in forced_gaps.items():
        existing = gaps.get(pk_val, [])
        gaps[pk_val] = sorted(set(existing + cols))
```

#### 步骤 3：限制重试实体数量

```python
retry_ids = sorted(gaps.keys())[:MAX_RETRY_ENTITIES]
```

最多处理 `MAX_RETRY_ENTITIES`（20）个实体，按主键排序取前 20 个。

#### 步骤 4：逐实体重试提取

对每个实体：

**a) 获取源文本：**

```python
source_paras = entity_groups.get(rid)
if not source_paras:
    continue
source_text = "\n\n".join(source_paras)
```

**b) 构建 LLM 提示：**

提示包含：
- 目标字段列表：`Fields: {col_list}`
- 字段定义提示（如果提供 `schema_field_defs`）
- 基金字段消歧提示（如果缺失字段包含基金相关字段）
- 语言要求：输出语言与源文本一致，绝不翻译
- 输出格式：管道符分隔的 `字段名: 值` 一行

**c) 调用 LLM 提取：**

```python
response = adapter.complete(messages)
```

**d) 解析响应：**

```python
reply = response.content.strip()
if reply.startswith("```"):
    reply = reply.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
```

处理代码块包装后，按 `|` 分割、按 `:` 解析键值对：

```python
extracted: dict[str, str] = {}
for part in reply.split("|"):
    if ":" not in part:
        continue
    key, _, val = part.partition(":")
    key = missing_col_by_lower.get(key.strip().lower())
    val = val.strip()
    if key and val:
        extracted[key] = val
```

**e) 回填到表中：**

```python
for record in by_pk.get(rid, []):
    for col, val in extracted.items():
        if record.fields.get(col, "").strip():
            continue  # 只回填仍为空的格
        _apply_llm_value(table, record, col, val, "retry")
```

### 4.6 关键代码片段

```python
# 提示构建
prompt = (
    f"Extract ONLY these fields from the text below:\n"
    f"Fields: {col_list}\n"
    f"{defs_hint}"
    f"{fund_hint}"
    f"LANGUAGE: output values in the SAME language as the source text. NEVER translate.\n"
    f"Output format — one line, pipe-separated:\n"
    f"  {' | '.join(f'{c}: <value>' for c in missing_cols)}\n"
    f"If a field's value is not in the text, leave it empty: {missing_cols[0]}: \n"
    f"Output ONLY the single key-value line, nothing else.\n\n"
    f"TEXT:\n{source_text}"
)
```

---

## 5. `_detect_gap_cells()`

检测高填充率列中的空值单元格。

### 5.1 函数签名

```python
def _detect_gap_cells(
    table: EntityTable,
) -> dict[str, list[str]]:
```

### 5.2 参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | `EntityTable` | 内存表 |

### 5.3 返回值

```python
dict[str, list[str]]  # {record_id: [missing_col, ...]}
```

- 键：缺少值的实体主键值
- 值：该实体缺少值的列名列表
- 空字典表示没有检测到空洞

### 5.4 核心逻辑

#### 步骤 1：确定候选列

```python
skip_lower = {a.lower() for a in table.anchor_keys}
candidate_cols = [c for c in table.columns if c.lower() not in skip_lower]
```

**跳过锚点列**（anchor keys），因为这些列通常是标识列，不期望有填充率。

#### 步骤 2：计算填充率

```python
n = len(table.records)
fill_counts: dict[str, int] = dict.fromkeys(candidate_cols, 0)
for record in table.records:
    for c in candidate_cols:
        if record.fields.get(c, "").strip():
            fill_counts[c] += 1
```

#### 步骤 3：筛选高填充率列

```python
gap_cols = [
    c
    for c in candidate_cols
    if fill_counts[c] / n >= FILL_RATE_THRESHOLD and fill_counts[c] < n
]
```

条件：
- 填充率 >= `FILL_RATE_THRESHOLD`（0.70，即 70%）
- 填充率 < 100%（如果已满则不视为空洞）

#### 步骤 4：收集空洞

```python
gaps: dict[str, list[str]] = {}
for record in table.records:
    if not record.pk:
        continue
    missing = [c for c in gap_cols if not record.fields.get(c, "").strip()]
    if missing:
        gaps[record.pk] = missing
```

只收集有 `pk` 的记录的空洞。

---

## 6. 关键常量

定义于 `_constants.py`：

| 常量名 | 值 | 说明 |
|--------|-----|------|
| `FILL_RATE_THRESHOLD` | `0.70` | 填充率阈值，用于判断哪些列需要空值重试 |
| `MAX_RETRY_ENTITIES` | `20` | 空值重试的最大实体数 |
| `DEDUP_CONTEXT_WINDOW` | `300` | 去重上下文窗口大小（字符数），从匹配位置向前后各取 |
| `MAX_LLM_VALUE_CHARS` | `500` | LLM 值最大字符数，超长内容截断 |
| `MAX_WORKERS` | `4` | 线程池最大工作线程数 |
| `LLM_CALL_TIMEOUT` | `180` | 每次 LLM 调用的超时时间（秒） |

---

## 7. 管线调用关系

### 7.1 Dedup 阶段

```
dedup_cross_field_copies()
  ├── _records_by_pk()          — 按主键分组记录
  ├── _value_search_needles()   — 生成数值搜索关键词
  ├── _extract_value_context()  — 提取源文本上下文
  │     └── _value_search_needles()
  ├── LLM 调用                   — 判定重复归属
  └── set_record_field()        — 清除错误字段
```

### 7.2 Retry 阶段

```
retry_missing_values()
  ├── _detect_gap_cells()       — 检测高填充列中的空洞
  ├── metadata_by_name()        — 获取字段定义
  ├── _records_by_pk()          — 按主键分组记录
  ├── LLM 调用                   — 逐实体提取缺失值
  └── _apply_llm_value()        — 落地回填
        ├── sanitize_llm_value()     — 推理泄漏截断 + 控制字符清理 + 长度上限
        │     └── strip_reasoning_leak()
        ├── clean_cell_for_type()    — 类型清洗
        │     ├── strip_reasoning_leak()
        │     └── _normalize_fund_type()
        └── set_record_field()       — 写入 Record
```

---

## 8. 复现指南

### 8.1 复现 Dedup 行为

```python
from agents.etl._verify import dedup_cross_field_copies

# 前提：table 已包含解析后的数据，source_text 为原始文档全文
cleared = dedup_cross_field_copies(adapter, table, source_text)

# cleared = {"pk_001": ["totalassets"]}
# 此时 table 中 pk_001 的 totalassets 字段已被清空为 ""
```

**关键行为：**
1. 只处理 `data_cols`（非锚点列）
2. 只处理能转为 float 的数值
3. 同一值在同一实体中出现在 2 个及以上列才触发去重
4. LLM 判定为 `BOTH` 的条目不修改任何字段
5. 判定为 `<column>` 的条目，保留该列，清除其他所有列
6. 清除前检查字段是否已为空（避免重复清除）
7. 清除值为空字符串 `""`（不是 None）

### 8.2 复现 Retry 行为

```python
from agents.etl._verify import retry_missing_values

# 前提：table 已解析，entity_groups 包含实体到段落的映射
retry_missing_values(adapter, table, entity_groups, schema_field_defs)

# 不必关注返回值，table 已被原地修改
```

**关键行为：**
1. 自动跳过锚点列
2. 只处理填充率 >= 70% 且 < 100% 的列
3. 最多处理 20 个实体（按主键排序后取前 20）
4. 每个实体单独调用 LLM，从 `entity_groups[rid]` 获取源段落
5. LLM 输出格式为 `|` 分隔的 `col_name: value` 行
6. 只回填当前仍为空的格（`if record.fields.get(col, "").strip(): continue`）
7. 使用 `_apply_llm_value()` 落地，确保类型安全和推理泄漏截断

### 8.3 验证值落地完整性

```python
# 验证 sanitize 效果
from agents.etl._record import sanitize_llm_value
from agents.etl._merge import strip_reasoning_leak

# 推理泄漏截断
assert strip_reasoning_leak("23890000 然而，根据上下文") == "23890000"

# 控制字符清理
assert sanitize_llm_value("hello\x00world") == "hello world"

# 超长截断
long_val = "x" * 1000
assert len(sanitize_llm_value(long_val)) == 500
```