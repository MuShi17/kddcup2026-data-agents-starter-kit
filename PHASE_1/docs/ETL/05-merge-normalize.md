# Phase 3 - Merge 和 Phase 4 - Normalize 阶段

## 1. 概述

Merge（合并）和 Normalize（归一化）是 Mamba Agent ETL 管线中紧接在解析（Parse）阶段之后的两个连续处理阶段，它们将 `EntityTable` 中的多条记录逐阶段清洗为结构化的行数据。

### 管线位置

```
Parse (Phase 1-2) → Merge (Phase 3) → Normalize (Phase 4) → Records to Rows
```

### 各自职责

| 阶段 | 目标 | 输入 | 输出 |
|------|------|------|------|
| **Merge** | 将同一实体的多条记录合并为一条 | `EntityTable`（含多条记录） | 同一 `EntityTable`，记录数减少，字段合并 |
| **Normalize** | 对每个字段进行类型级清洗 | `EntityTable`（合并后） | 同一 `EntityTable`，字段值标准化 |

---

## 2. Merge — `merge_records()`

### 2.1 函数签名

```python
def merge_records(
    table: EntityTable,
    source_text: str | None = None,
) -> None:
```

### 2.2 输入参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | `EntityTable` | 包含多条待合并记录的实体表，记录已通过 `parse_kv_text()` 解析 |
| `source_text` | `str \| None` | 原始源文本（markdown/prose），用于锚点验证；可选 |

### 2.3 返回值

无直接返回值。该函数**原地修改** `table.records`，将多条记录合并为唯一实体行。

### 2.4 核心逻辑步骤

`merge_records()` 的执行流程分为以下几个子步骤：

#### 步骤 1：前置修复 — 伪造顺序 PK 修复

调用 `_repair_fabricated_pks()`，检测并修复压缩阶段产生的伪造顺序主键：

- 扫描所有候选列（先声明的 anchor 列，再按首次出现顺序扫描其他列）
- 条件：候选列与 PK 一致的记录数 ≥ 3，且不一致的记录数 < 一致记录数
- 不一致的 PK 值必须是整数，且构成从 1 开始的稠密连续序列
- 满足条件时，用候选列的值**替换**主键值

```python
parsed = _repair_fabricated_pks(parsed, primary_key, anchor_keys)
```

#### 步骤 2：丢弃无效锚点 PK

调用 `_drop_unanchored_pks()`，丢弃那些数字 PK 在源文本中从未作为独立 token 出现的行：

- 仅对有 `source_text` 且主键为数字的情况生效
- 使用 `has_standalone_id()` 验证 PK 是否在源文本中以独立 token 形式出现
- 丢弃伪造的行（通常是 LLM 幻觉产生的行）

```python
parsed = _drop_unanchored_pks(parsed, primary_key, source_text)
```

#### 步骤 3：按主键分组并合并字段

- 使用 `_group_key_from_pairs()` 为每条记录确定分组键
- 优先使用主键值，若无主键则尝试使用辅助 anchor 列构造临时 anchor key
- 同一分组内的记录，逐字段合并：
  - 如果已有值为空/占位符，新值非空/非占位符 → 覆盖
  - 如果新旧值都非空且不相等 → 记录为冲突（`table.conflicts += 1`）
  - 不覆盖已有的非空非占位符值

```python
for key, raw_val in pairs:
    val, _approx = split_approx_tag(raw_val)
    val = val.strip()
    existing = record.fields.get(key)
    if existing is None or (is_placeholder(existing) and not is_placeholder(val)):
        set_record_field(record, key, raw_val.strip(), "pre_merge")
    elif (
        val
        and existing
        and not is_placeholder(existing)
        and not is_placeholder(val)
        and existing != val
    ):
        table.conflicts += 1
```

#### 步骤 4：辅助 anchor 合并（Round 2）

当有多个 anchor 列时，以**非主键的 anchor 列**为桥梁，将临时 anchor 记录合并到真实记录中：

- 按 anchor 列的值分组
- 如果一个 anchor 值同时关联了临时 anchor 记录和真实记录
- 且临时 anchor 记录的所有 anchor 字段值与真实记录不冲突
- 则将临时 anchor 记录的所有字段合并到真实记录中

```python
if anchor_keys and len(anchor_keys) > 1:
    for anchor in anchor_keys[1:]:
        anchor_to_pks: dict[str, list[str]] = {}
        # ... 建立 anchor 值 → PK 的映射
        # 找到临时 anchor 与真实记录的一对一关系，合并字段
```

#### 步骤 5：丢弃 identity-echo 片段

对于仍然无法合并的临时 anchor 记录（无真实主键的片段），判断其是否包含「有效信息」：

- 只携带 identity 字段（主键、anchor 列）值的片段 → 视为 echo，直接丢弃
- 携带非 identity 字段值的片段 → 保留为独立行，记录日志警告

```python
if not is_constant_echo:
    informative.append(field)
if informative:
    # 保留并记录警告
else:
    del groups[key]  # 丢弃 echo 片段
```

#### 步骤 6：重建记录列表

按照原始顺序重建 `table.records`，确保主键字段正确设置。

### 2.5 关键数据结构

**`_record_pairs_for_merge()`** — 将 Record 转为 `(key, value)` 对列表，近似标记保留在值中：

```python
def _record_pairs_for_merge(record: Record) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for key, val in record.fields.items():
        if val and key in record.approx:
            val = f"{APPROX_TAG}{val}"
        pairs.append((key, val))
    return pairs
```

**`_group_key_from_pairs()`** — 确定分组键：

```python
def _group_key_from_pairs(
    pk_val: str | None,
    pairs: list[tuple[str, str]],
    anchor_keys: list[str] | None,
) -> str | None:
```

- 优先使用非占位符的 PK 值
- 其次使用非主键的 anchor 列的第一个非空值，构造 `__anchor__:<anchor>:<val>` 临时键
- 两者都不可用时返回 `None`（丢弃该记录）

---

## 3. 归一化 — `normalize_records()`

### 3.1 函数签名

```python
def normalize_records(table: EntityTable) -> None:
```

### 3.2 输入参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | `EntityTable` | 合并后的实体表，包含 schema 列定义和字段类型信息 |

### 3.3 返回值

无直接返回值。该函数**原地修改** `table.records` 中每个 Record 的字段值。

### 3.4 核心逻辑

`normalize_records()` 遍历表中每个记录的每个 schema 列，对每个单元格调用 `clean_cell_for_type()` 进行类型级清洗：

```python
def normalize_records(table: EntityTable) -> None:
    field_types = metadata_by_name(table.columns, table.field_types)
    col_set = set(table.columns)
    pk = table.primary_key
    for record in table.records:
        for col in record.fields:
            if col not in col_set:
                continue  # 只清洗 schema 列，保留非 schema 列原样
            raw = record.fields[col]
            if col in record.approx:
                raw = f"{APPROX_TAG}{raw}"  # 重新注入 ~ 标记
            cleaned = clean_cell_for_type(
                _strip_cjk_ascii_spaces(raw.strip()),
                field_types.get(col),
                field_name=col,
            )
            set_record_field(record, col, cleaned, "normalize")
        if pk:
            record.pk = record.fields.get(pk) or None
```

### 3.5 处理细节

| 处理内容 | 说明 |
|----------|------|
| 仅清洗 schema 列 | 只遍历 `table.columns` 中定义的列，非 schema 列保留原样 |
| 近似标记回注 | 如果字段标记为 `approx`，在值前加 `~` 前缀，让 `clean_cell_for_type` 处理 |
| CJK-ASCII 空格清理 | 先调用 `_strip_cjk_ascii_spaces()` 去掉中英文之间的多余空格 |
| 主键更新 | 清洗后重新设置 `record.pk` 以反映最新值 |

---

## 4. 关键辅助函数

### 4.1 `clean_cell_for_type()` — 类型级清洗函数

这是整个 Normalize 阶段的核心，根据字段类型对单个单元格值进行标准化。

#### 函数签名

```python
def clean_cell_for_type(
    val: str,
    field_type: str | None = None,
    field_name: str | None = None,
) -> str:
```

#### 输入参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `val` | `str` | 待清洗的单元格值 |
| `field_type` | `str \| None` | 字段类型描述，如 `"number"`, `"date"`, `"boolean"` |
| `field_name` | `str \| None` | 字段名，用于特殊字段的逻辑（如 fund_type, name 字段） |

#### 返回值

| 类型 | 说明 |
|------|------|
| `str` | 清洗后的值，无效→空字符串 `""` |

#### 核心逻辑步骤

```
输入值 val
    │
    ├─ split_approx_tag() 剥离 ~ 前缀
    │
    ├─ clean_cell() 基础清洗：占位符检测 → 空字符串；单位后缀剥离 → 纯数字
    │
    ├─ strip_reasoning_leak() 截断 LLM 推理泄漏
    │
    ├─ _normalize_fund_type() 基金类型字段 → "XY型"（去掉"基金"后缀）
    │
    ├─ 名称/缩写字段 → 纯数字值拦截 → 清空
    │
    └─ 按字段类型 dispatch：
         ├─ numeric/integer（非 rank）→ 去千位分隔符、去%后缀、NUMERIC_SCALAR 校验
         ├─ date → DATE_SCALAR 校验
         ├─ boolean → normalize_boolean() 归一化
         └─ 其他 → 若值本身是数字则保留，否则返回 clean_cell() 结果
```

#### 代码详解

**步骤 1：基础清洗**

```python
bare, approx = split_approx_tag(val)    # 剥离 ~ 前缀
cleaned = clean_cell(bare)               # 占位符检测 + 单位后缀剥离
if not cleaned:
    return ""
cleaned = strip_reasoning_leak(cleaned)  # 截断推理泄漏
```

**步骤 2：基金类型归一化**

```python
if field_name:
    cleaned = _normalize_fund_type(cleaned, field_name)
```

**步骤 3：名称/缩写字段数字拦截**

```python
_NAME_ABBR_FIELDS = frozenset({
    "secuabbr", "chiname", "chinameabbr", "chinamabbr",
    "fund_name", "fund_name_short", "name", "abbr",
    "companyname", "abbrchiname",
})
if field_name and field_name.lower() in _NAME_ABBR_FIELDS:
    stripped_digits = cleaned.replace(",", "").replace(".", "").lstrip("-")
    if stripped_digits.isdigit():
        return ""  # 纯数字的名称/缩写 → 清空
```

**步骤 4：按字段类型清洗**

```python
type_text = (field_type or "").lower()
if ("number" in type_text or "integer" in type_text) and "rank" not in type_text:
    # 数字：去千位分隔符、去%后缀、格式校验
    normalized = cleaned.replace(",", "")
    if normalized.endswith("%"):
        normalized = normalized[:-1].strip()
    if not NUMERIC_SCALAR.match(normalized):
        return ""
    return f"{APPROX_TAG}{normalized}" if approx else normalized
elif "date" in type_text:
    return cleaned if DATE_SCALAR.match(cleaned) else ""
elif "boolean" in type_text:
    return normalize_boolean(cleaned) or ""
else:
    # 其他类型：保留近似标记或纯数字
    normalized = cleaned.replace(",", "")
    if approx and NUMERIC_SCALAR.match(normalized):
        return f"{APPROX_TAG}{normalized}"
    if normalized != cleaned and NUMERIC_SCALAR.match(normalized):
        return normalized
    return clean_cell(val)  # 最终回退到基础清洗
```

#### 例子

| 输入值 | `field_type` | `field_name` | 输出 | 说明 |
|--------|-------------|-------------|------|------|
| `"1,234,567"` | `"number"` | — | `"1234567"` | 去千位分隔符 |
| `"85%"` | `"number"` | — | `"85"` | 去 % 后缀 |
| `"~6970000"` | `"number"` | — | `"~6970000"` | 保留近似标记 |
| `"2024-01-15"` | `"date"` | — | `"2024-01-15"` | 日期格式校验 |
| `"2024/1/15"` | `"date"` | — | `"2024/1/15"` | DATE_SCALAR 接受 `/` 分隔 |
| `"Yes"` | `"boolean"` | — | `"true"` | 布尔归一化 |
| `"混合型基金"` | — | `"fund_type"` | `"混合型"` | 基金类型去除"基金"后缀 |
| `"12345"` | — | `"secuabbr"` | `""` | 名称字段纯数字拦截 |
| `"Source text: 123"` | — | — | `""` | 推理泄漏截断（整段被截） |
| `"value Source text more"` | — | — | `"value"` | 推理泄漏截断（保留前半） |

---

### 4.2 `strip_reasoning_leak()` — 去除 LLM 推理泄漏

#### 函数签名

```python
def strip_reasoning_leak(val: str) -> str:
```

#### 输入参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `val` | `str` | 可能包含推理泄漏的字段值 |

#### 返回值

`str` — 截断后的值，若泄漏标记在开头则返回空字符串。

#### 核心逻辑

使用正则 `_REASONING_LEAK` 匹配泄漏标记的起始位置：

```python
_REASONING_LEAK = re.compile(
    r"\s*\(?"
    r"(?:"
    r"Source text|However,|Wait,|Let me|Let's|Note:|The (?:text|context|extraction|source)"
    r"|strictly following|implies the|this (?:is|means|suggests)"
    r"|或根据|原文指代|原文(?:是|写|说|提到)|通常指|然而，|但仔细|但是，"
    r"|如果源文本|这看起来|根据文档|根据上下文|仔细阅读"
    r")"
    r".*",
    re.IGNORECASE | re.DOTALL,
)
```

- 如果泄漏标记出现在值中间（`m.start() > 0`），截断标记之前的部分
- 截断尾部多余的 ` ,;|` 符号
- 如果泄漏标记从开头匹配，`m.start() == 0`，返回原值（由调用方 `clean_cell_for_type` 进一步处理）

---

### 4.3 `normalize_local_record_ids()` — 本地记录 ID 归一化

#### 函数签名

```python
def normalize_local_record_ids(table: EntityTable) -> tuple[int, int]:
```

#### 输入参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | `EntityTable` | 解析后的实体表 |

#### 返回值

`tuple[int, int]` — `(dropped_records, normalized_values)`，丢弃的记录数 + 归一化的值数。

#### 核心逻辑

1. 遍历每个记录的每个字段，检查是否为本地记录 ID 字段
2. 本地记录 ID 判定：字段名在 `RECORD_ID_FIELDS` 中，或字段类型包含 `"local_record_id"`
3. 如果值匹配 `RECORD_ID_RANGE`（如 `"档案 197至217"`、`"Record 1-5"`）→ 整个记录丢弃
4. 如果值匹配 `RECORD_ID_LABEL`（如 `"档案 286"`、`"Record 286"`）→ 提取纯数字部分

```python
RECORD_ID_LABEL = re.compile(
    rf"^(?:{RECORD_ID_KEYWORDS}){RECORD_ID_SEPARATOR}(\d{{1,8}})$",
    re.IGNORECASE,
)
RECORD_ID_RANGE = re.compile(
    rf"(?:{RECORD_ID_KEYWORDS})?\s*\d{{1,8}}\s*(?:至|到|[-~–—])\s*\d{{1,8}}",
    re.IGNORECASE,
)
```

#### 例子

| 输入值 | 处理结果 |
|--------|---------|
| `"档案 286"` | → `"286"`，归一化计数 +1 |
| `"Record 29"` | → `"29"`，归一化计数 +1 |
| `"档案 197至217"` | → 整条记录丢弃 |
| `"Record 1-5"` | → 整条记录丢弃 |
| `"普通字符串"` | → 不做处理 |

---

### 4.4 `apply_synonym_merges()` — 同义列合并

#### 函数签名

```python
def apply_synonym_merges(
    header: list[str],
    rows: list[list[str]],
    merges: list[tuple[str, str]],
    protected: set[str] | None = None,
) -> tuple[list[str], list[list[str]], list[tuple[str, str]]]:
```

#### 输入参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `header` | `list[str]` | CSV 表头列名列表 |
| `rows` | `list[list[str]]` | CSV 数据行 |
| `merges` | `list[tuple[str, str]]` | 已裁决的 `(source, target)` 同义列对列表 |
| `protected` | `set[str] \| None` | 受保护的列名集合（不参与合并） |

#### 返回值

`tuple[list[str], list[list[str]], list[tuple[str, str]]]` — `(header, rows, applied)`，新表头、新数据行、实际应用的合并列表。

#### 核心逻辑

1. 按源列填充率降序（`fill.get(src, 0)` 降序）排列 merges，填充率相同时按列名排序
2. 对每个 `(src, dst)` 对：
   - **受保护列跳过**：两列名不区分大小写时视为同一列，允许合并；否则任一列在保护集中则跳过
   - **列存在性检查**：src 和 dst 必须都在表头中
   - **空源列跳过**：源列所有行都为空则跳过
   - **冲突检测**：任何一行两列都非空且值不相等（字符串或数值）→ 跳过该对
   - **执行合并**：源列非空、目标列为空的行，将源值复制到目标列；然后删除源列

```python
for src, dst in sorted(merges, key=lambda m: (-fill.get(m[0], 0), m[0])):
    # ... 检查保护、列存在性、空源列
    # ... 冲突检测
    for row in rows:
        if row[si].strip() and not row[di].strip():
            row[di] = row[si]
    header.pop(si)  # 删除源列
    for row in rows:
        row.pop(si)
```

---

## 5. 辅助函数（Merge）

### 5.1 `column_fill_counts()` — 列填充率统计

```python
def column_fill_counts(header: list[str], rows: list[list[str]]) -> dict[str, int]:
```

遍历所有行，统计每列非空单元格的数量。返回 `{列名: 非空计数}` 字典。

---

### 5.2 `canonical_key_map()` / `canonicalize_key()` — Key 归一化

#### `canonical_key_map()`

```python
def canonical_key_map(keys: list[str]) -> dict[str, str]:
```

为列名列表构建规范化映射字典：`normalized_key → canonical_key`。

- 使用 `strip_key()` 归一化 key
- 如果两个不同的列名归一化后相同，记录警告日志

#### `canonicalize_key()`

```python
def canonicalize_key(key: str, key_map: dict[str, str]) -> str:
```

在映射中查找 key 的规范拼写；未找到时返回 `key.strip()`。

---

### 5.3 `strip_key()` — Key 去除空格和分隔符

```python
def strip_key(s: str) -> str:
    return s.strip().lower().replace("_", "").replace("-", "")
```

将 key 转为小写，去除首尾空格，去掉所有 `_` 和 `-` 分隔符。

---

### 5.4 `is_placeholder()` — 占位符检测

```python
def is_placeholder(val: str) -> bool:
    return val.lower().strip() in PLACEHOLDER_VALS or "(placeholder)" in val.lower()
```

检测值是否为空占位符。`PLACEHOLDER_VALS` 包含：

```python
PLACEHOLDER_VALS = frozenset({
    "", "-", "null", "none", "nan", "n/a", "placeholder",
    "null (placeholder)", "none (placeholder)", "nan (placeholder)",
    "- (placeholder)", "0.0 (placeholder)", "redacted",
})
```

---

### 5.5 `normalize_boolean()` — 布尔归一化

```python
def normalize_boolean(val: str) -> str | None:
```

| 输入值 | 输出 |
|--------|------|
| `"true"`, `"yes"`, `"1"`, `"是"` | `"true"` |
| `"false"`, `"no"`, `"0"`, `"否"` | `"false"` |
| 其他 | `None` |

---

### 5.6 `split_approx_tag()` — 模糊标记分割

```python
def split_approx_tag(val: str) -> tuple[str, bool]:
```

剥离值开头的 `~` 或 `～` 前缀，返回 `(裸值, 是否有标记)`。

---

### 5.7 `clean_cell()` — 基础单元格归一化

```python
def clean_cell(val: str) -> str:
```

- 空值或占位符 → 返回 `""`
- 值匹配 `UNIT_SUFFIX` 正则（数字 + 空格 + 单位后缀）→ 返回纯数字部分
- 否则返回原值

`UNIT_SUFFIX` 正则：`^([+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?)\s+\S+\s*$`

---

### 5.8 `resolve_id_conflicts()` — 系统性 ID 偏移修复

```python
def resolve_id_conflicts(table: EntityTable) -> int:
```

检测并修复压缩阶段产生的系统性 ID 偏移错误：

- 以每个名称第一次出现的 ID 为基准
- 扫描所有行，如果某行同名的 ID 与基准不一致，修正为基准 ID
- 返回修复的记录数

---

### 5.9 `_repair_fabricated_pks()` — 伪造顺序 PK 修复

```python
def _repair_fabricated_pks(
    parsed: list[tuple[str | None, list[tuple[str, str]]]],
    primary_key: str | None,
    anchor_keys: list[str] | None,
) -> list[tuple[str | None, list[tuple[str, str]]]]:
```

当压缩阶段将段落编号（1, 2, 3, ...）而不是原始记录 ID 写入主键时，使用候选列（alias column）的值修复主键。

---

### 5.10 `_drop_unanchored_pks()` — 无锚点 PK 丢弃

```python
def _drop_unanchored_pks(
    parsed: list[tuple[str | None, list[tuple[str, str]]]],
    primary_key: str | None,
    source_text: str | None,
) -> list[tuple[str | None, list[tuple[str, str]]]]:
```

丢弃数字 PK 在源文本中从未作为独立 token 出现的行。非数字 PK 和无 PK 的行不受影响。

---

### 5.11 `cells_agree()` — 多值一致性判断

```python
def cells_agree(a: str, b: str) -> bool:
```

两个非空单元格在以下情况下视为一致：
- 字符串相等，或
- 解析为浮点数后相等

---

## 6. 常量定义

| 常量名 | 值 | 说明 |
|--------|-----|------|
| `APPROX_TAG` | `"~"` | 近似值标记前缀 |
| `APPROX_PREFIXES` | `("~", "～")` | 近似值标记前缀（全角/半角） |
| `TEMP_ANCHOR_PREFIX` | `"__anchor__:"` | 临时 anchor 键前缀 |
| `PLACEHOLDER_VALS` | `frozenset({...})` | 占位符值集合 |
| `NUMERIC_SCALAR` | `r"^[+-]?(?:\d+(?:\.\d+)?\|\.\d+)(?:[eE][+-]?\d+)?$"` | 数字格式校验 |
| `DATE_SCALAR` | `r"^\d{4}(?:[-/]\d{1,2}(?:[-/]\d{1,2})?)?$"` | 日期格式校验 |
| `BOOL_TRUE` | `frozenset({"true", "yes", "1", "是"})` | 真值集合 |
| `BOOL_FALSE` | `frozenset({"false", "no", "0", "否"})` | 假值集合 |
| `RECORD_ID_FIELDS` | `frozenset({...})` | 本地记录 ID 字段名集合 |
| `RECORD_ID_KEYWORDS` | `"档案\|记录\|条目\|战略单元\|Record\|File\|Case"` | 记录 ID 关键词 |
| `UNIT_SUFFIX` | `r"^([+-]?(?:\d+(?:\.\d+)?\|\.\d+)(?:[eE][+-]?\d+)?)\s+\S+\s*$"` | 单位后缀剥离正则 |

---

## 7. 完整管线流程

```
Parse (Phase 1-2)
  │
  ├─ parse_kv_text() → EntityTable
  ├─ resolve_id_conflicts()     (可选，修复 ID 偏移)
  └─ normalize_local_record_ids()  (可选，归一化本地记录 ID)
  │
  ▼
Merge (Phase 3)
  │
  ├─ _repair_fabricated_pks()   修复伪造顺序 PK
  ├─ _drop_unanchored_pks()     丢弃无锚点 PK
  ├─ 按主键分组合并字段
  ├─ 辅助 anchor 合并 (Round 2)
  ├─ 丢弃 identity-echo 片段
  └─ 重建 records 列表
  │
  ▼
Normalize (Phase 4)
  │
  └─ 对每个 schema 列调用 clean_cell_for_type()
       ├─ 剥离 ~ 标记
       ├─ 占位符检测 → 清空
       ├─ 推理泄漏截断
       ├─ 基金类型归一化
       ├─ 名称/缩写字段数字拦截
       └─ 按字段类型：数字/日期/布尔/其他
  │
  ▼
Records to Rows
  │
  └─ records_to_rows() → (header, rows)
       └─ to_csv_text() → CSV
```

---

## 8. 复现代码行为的关键要点

1. **Merge 的冲突处理**：只有"已有值为空/占位符，新值有效"时才覆盖；两个都有效但不相等时只计数不覆盖
2. **Normalize 只清洗 schema 列**：`table.columns` 之外的字段保留原始值
3. **近似标记回注**：Normalize 前先将 `approx` 标记重新注入值中，让 `clean_cell_for_type` 根据类型决定是否保留
4. **数字字段的 `~` 保留**：当字段类型为 `number`/`integer` 且值带有 `~` 标记时，标记会保留在输出中，供后续 identity-repair 使用
5. **日期字段严格校验**：只有匹配 `DATE_SCALAR` 的值才保留，否则清空
6. **同义列合并的确定性**：按填充率降序处理，确保跨次运行结果一致