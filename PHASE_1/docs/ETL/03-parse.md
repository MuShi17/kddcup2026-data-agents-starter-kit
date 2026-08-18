# Parse 阶段：压缩 KV 文本解析为内存表

> 来源：Mamba Agent (`Kosthi/kddcup2026-dataagents`)
> 核心模块：`src/agents/etl/_record.py`（383 行）
> 依赖模块：`_types.py`、`_merge.py`、`_columns.py`、`_constants.py`

---

## 1. 概述

### 1.1 Parse 阶段在 ETL 管线中的位置

```
ETL 管线（单文件提取链路）：

┌──────────────┐    ┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│  Phase 0:    │    │  Phase 1a:       │    │  Phase 1b:       │    │  Phase 2:        │
│  Schema      │───→│  Compress        │───→│  Parse           │───→│  Serialize +     │
│  Infer       │    │  (LLM 压缩)      │    │  (KV→内存表)    │    │  Verify          │
└──────────────┘    └──────────────────┘    └──────────────────┘    └──────────────────┘
                                                    │
                                                    ▼
                                         ┌──────────────────┐
                                         │  Merge (record   │
                                         │  dedup + merge)  │
                                         └──────────────────┘
```

Parse 阶段是 **Phase 1b**，紧接在 Compress（LLM 压缩散文为 KV 文本）之后，在 Merge（记录合并去重）、Verify（验证修复）和最终序列化之前。

### 1.2 为什么需要"一次解析为内存表"

在压缩阶段之后，数据以 `key: value | key: value` 的纯文本格式存在。如果每个后续阶段都直接操作文本，会导致：

- **重复解析开销**：每个阶段都要做 `split("|") + partition(":")`
- **文本回填困难**：verify/retry 阶段需要用正则替换往文本行里回填值，容易出错
- **类型信息丢失**：纯文本无法携带近似标记、来源追踪等元数据
- **一致性保证脆弱**：多阶段文本操作容易引入格式漂移

**Parse 阶段的解决方案**：解析一次（`parse_kv_text()`）将文本转为 `EntityTable` 内存表，后续阶段直接操作 `Record` 对象，最后序列化一次（`records_to_rows()` + `to_csv_text()`）。

### 1.3 已知边界

- KV wire 格式没有转义机制
- 值内的 `|` 仍会截断字段（竖线安全性只对入库后的值成立）
- 值内换行仍会劈断记录行（换行安全性只对入库后的值成立）

---

## 2. 核心数据结构

### 2.1 `Reject` 类

```python
@dataclass(slots=True)
class Reject:
    """A compressed-output line the parser could not turn into a record."""
    line: str    # 原始行文本
    reason: str  # 拒绝原因："no_pk" | "no_kv"
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `line` | `str` | 无法解析的原始行文本 |
| `reason` | `str` | 拒绝原因，仅两个取值：`"no_pk"`（缺少主键）、`"no_kv"`（非 KV 格式） |

**用途**：统计解析阶段的拒绝率，用于监控管线健康度。

### 2.2 `Record` 类

```python
@dataclass(slots=True)
class Record:
    """One entity: canonical-spelling fields (+ unmatched original keys) → bare values."""
    fields: dict[str, str]       # 字段名 → 裸值（不含 ~ 前缀）
    pk: str | None               # 主键值（从 fields 中提取）
    approx: set[str]             # 带 ~ 模糊标记的字段名集合
    provenance: dict[str, str]   # 字段名 → 最后写入者（"parse" | "normalize" | "pre_merge" | "id_conflict" | "record_id_normalize"）
    source_chunk: int | None     # 来源块编号（压缩阶段的分块索引）
```

**数据流图**：

```
┌─────────────────────────────────────────────────┐
│                    Record                         │
├─────────────────────────────────────────────────┤
│  fields:  │  "entity_name": "某某基金"           │
│           │  "fund_size": "5000000000"           │
│           │  "fund_type": "股票型"               │
│           │  "成立日期": "2020-01-01"            │  ← 未匹配 schema 的键也保留
├─────────────────────────────────────────────────┤
│  pk:      │  "293"                               │
├─────────────────────────────────────────────────┤
│  approx:  │  {"fund_size"}                       │  ← 该字段原值来自模糊数字
├─────────────────────────────────────────────────┤
│  provenance:  │  "entity_name": "parse"          │
│               │  "fund_size": "parse"            │
│               │  "fund_type": "normalize"        │
├─────────────────────────────────────────────────┤
│  source_chunk:  │  0                             │
└─────────────────────────────────────────────────┘
```

**关键设计点**：

1. **`fields` 存裸值**：所有值在存入 `fields` 前，`~` 前缀已被剥离到 `approx` 集合中。后续操作 `record.fields[col]` 得到的永远是不含 `~` 的裸值。
2. **`pk` 的冗余存储**：`pk` 字段是 `fields[pk_key]` 的快捷引用，两者始终同步。`pk = None` 表示该记录没有主键值（但字段数据仍然保留）。
3. **`provenance` 追踪**：记录每个字段的"最后写入者"，用于调试和审计。写入者名称包括 `"parse"`、`"normalize"`、`"pre_merge"`、`"id_conflict"`、`"record_id_normalize"`。
4. **`source_chunk`**：跟踪记录来自压缩阶段的哪个块，用于合并阶段识别分块边界。

### 2.3 `EntityTable` 类

```python
@dataclass(slots=True)
class EntityTable:
    """In-memory table shared by all post-compression pipeline stages."""
    columns: list[str]          # schema 列名列表（规范拼写）
    primary_key: str | None     # 主键列名（如 "ID"）
    anchor_keys: list[str]      # 锚点键列表（用于合并阶段的辅助分组键）
    field_types: dict[str, str] # 字段名 → 类型名（如 {"fund_size": "number"}）
    records: list[Record]       # 记录列表
    rejects: list[Reject]       # 拒绝行列表
    conflicts: int = 0          # 行内重复键（值不一致）计数
```

**类图**：

```
┌──────────────────────────────────────────────────────┐
│ EntityTable                                            │
├──────────────────────────────────────────────────────┤
│ columns:        ["ID", "entity_name", "fund_size", …] │
│ primary_key:    "ID"                                  │
│ anchor_keys:    ["ID", "entity_name"]                 │
│ field_types:    {"fund_size": "number", …}            │
│ records:        [Record, Record, …]                   │
│ rejects:        [Reject, Reject, …]                   │
│ conflicts:      3                                     │
├──────────────────────────────────────────────────────┤
│ stats() -> dict  # 记录数、非空单元格数、拒绝数、冲突数   │
└──────────────────────────────────────────────────────┘
```

**`stats()` 方法**：

```python
def stats(self) -> dict[str, int]:
    """Conservation metrics: phases compare before/after to detect silent loss."""
    nonempty = sum(
        1 for record in self.records for val in record.fields.values() if val.strip()
    )
    return {
        "records": len(self.records),
        "nonempty_cells": nonempty,
        "rejects": len(self.rejects),
        "conflicts": self.conflicts,
    }
```

返回四个守恒指标，各阶段比较前后差值来检测数据是否静默丢失。

### 2.4 数据结构的关系

```
EntityTable
  │
  ├── columns → schema 列名的规范拼写列表
  ├── field_types → 列名 → 类型（用于 normalize_records 的类型级清洗）
  │
  ├── records: list[Record]
  │     ├── fields → 列名到值的映射（键名已归一化到 schema 拼写）
  │     ├── pk → 主键值（冗余存储）
  │     ├── approx → 模糊标记字段集合
  │     ├── provenance → 字段写入者追踪
  │     └── source_chunk → 来源块编号
  │
  ├── rejects: list[Reject]
  │     ├── line → 原始文本
  │     └── reason → 拒绝原因
  │
  └── conflicts → int（行内重复键冲突总数）
```

### 2.5 数据结构如何被后续阶段使用

| 后续阶段 | 使用方式 |
|---------|---------|
| **Merge** (`_merge.py`) | 读取 `records` 进行分组去重（`merge_records`）；使用 `pk`、`anchor_keys` 作为分组键；使用 `approx` 在值中回注 `~` 前缀后参与合并比较；写入 `provenance["pre_merge"]` |
| **Verify** (`_verify.py`) | 读取 `records` 进行字段级验证修复；使用 `field_types` 判断字段类型；写入 `provenance` 标记验证来源 |
| **Normalize** (`_record.py`) | 读取 `field_types` 进行类型级清洗（`normalize_records`）；写入 `provenance["normalize"]` |
| **Row Projection** (`_record.py`) | `records_to_rows()` 将 `records` 投影到 `columns` 列表，生成 CSV 行 |

---

## 3. 核心函数：`parse_kv_text()`

### 3.1 函数签名

```python
def parse_kv_text(
    text: str,
    columns: list[str] | None = None,
    primary_key: str | None = None,
    anchor_keys: list[str] | None = None,
    field_types: dict[str, str] | None = None,
) -> EntityTable:
```

### 3.2 参数说明

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `text` | `str` | 必填 | 压缩后的 KV 文本，每行格式为 `key: value \| key: value \| ...` |
| `columns` | `list[str] \| None` | `None` | Schema 列名列表（规范拼写），用于键名归一化 |
| `primary_key` | `str \| None` | `None` | 主键列名，为 `None` 时沿用 `ENTITY_LINE` 匹配（`ID:` 行首约定） |
| `anchor_keys` | `list[str] \| None` | `None` | 锚点键列表，透传给 `EntityTable` |
| `field_types` | `dict[str, str] \| None` | `None` | 字段名到类型的映射，透传给 `EntityTable` |

### 3.3 返回值

返回 `EntityTable` 实例，包含：
- `records`：成功解析的 `Record` 列表
- `rejects`：无法解析的行（`Reject` 列表）
- `conflicts`：行内重复键冲突计数
- `columns`、`primary_key`、`anchor_keys`、`field_types`：透传自输入参数

### 3.4 核心逻辑步骤

#### 步骤 1：初始化

```python
columns = columns or []
anchor_keys = anchor_keys or []
key_map = canonical_key_map(columns)          # 构建 schema 键名归一化映射
pk_key = canonicalize_key(primary_key, key_map) if primary_key else NO_SCHEMA_PK  # "ID"
pk_norm = strip_key(primary_key) if primary_key else None  # 归一化后的主键键名
```

- `NO_SCHEMA_PK = "ID"`：无 schema 时约定主键名称为 "ID"
- `key_map`：由 `canonical_key_map(columns)` 构建，将各种拼写变体映射到 schema 列的标准拼写
- `pk_norm`：主键列的归一化形式（去空格、去下划线、去连字符、小写），用于行内比较

#### 步骤 2：逐行解析

```python
for raw_line in text.splitlines():
    stripped = raw_line.strip()
    if not stripped:          # 跳过空行
        continue
    if SECTION_HEADING.match(stripped):  # 跳过 Markdown 标题
        continue
    normalized = _normalize_kv_separators(stripped)  # 重写 = 为 :
    if ":" not in normalized:  # 非 KV 格式
        rejects.append(Reject(line=stripped, reason="no_kv"))
        continue
    if pk_norm is None and not ENTITY_LINE.match(normalized):  # 无 schema 时需 ID: 开头
        rejects.append(Reject(line=stripped, reason="no_pk"))
        continue
```

**行过滤逻辑**：

```
输入行
  │
  ├── 空行（strip 后为空） → 跳过
  │
  ├── Markdown 标题（如 ## 标题） → 跳过（SECTION_HEADING 匹配）
  │
  ├── 不含 ":"（_normalize_kv_separators 后） → Reject("no_kv")
  │
  ├── 无 schema 且不以 "ID:" 开头 → Reject("no_pk")
  │
  └── 其他 → 进入解析
```

- `SECTION_HEADING`：`re.compile(r"^(#{2,4})\s+(.+)$", re.MULTILINE)` — 匹配 Markdown 二级到四级标题
- `ENTITY_LINE`：`re.compile(r"^ID:\s*", re.MULTILINE)` — 匹配以 `ID:` 开头的行

#### 步骤 3：字段级解析

```python
record = Record()
line_conflicts = 0
has_pk_key = pk_norm is None  # 无 schema 路径：ENTITY_LINE 已匹配
for part in normalized.split("|"):
    raw_key, sep, val = part.partition(":")
    if not sep:       # 跳过没有 ":" 的片段
        continue
    if pk_norm is not None and strip_key(raw_key) == pk_norm:
        has_pk_key = True  # 标记该行包含主键
    key = canonicalize_key(raw_key, key_map)  # 键名归一化到 schema 拼写
    bare, approx = split_approx_tag(val.strip())  # 剥离 ~ 前缀
    
    if key in record.fields:  # 行内重复键处理
        existing = record.fields[key]
        if existing != bare:
            line_conflicts += 1
        if is_placeholder(existing) and not is_placeholder(bare):
            # 占位符被真实值覆盖
            record.fields[key] = bare
            record.provenance[key] = "parse"
            if approx and bare:
                record.approx.add(key)
            else:
                record.approx.discard(key)
        continue  # 保留首个有效值（非占位符优先）
    
    record.fields[key] = bare
    record.provenance[key] = "parse"
    if approx and bare:
        record.approx.add(key)
```

**键名归一化流程**：

```
原始键名（如 "Entity Name"、"entity-name"、"entity_name"）
  │
  ├── strip_key()：去空格、去下划线、去连字符、小写 → "entityname"
  │
  ├── canonical_key_map[key_map] 查找 → "entity_name"（schema 拼写）
  │
  └── 未匹配 → 保留原始键名（strip 后）
```

**行内重复键处理规则**：

| 已有值 | 新值 | 行为 |
|--------|------|------|
| A | A | 忽略（值相同，不计冲突） |
| A | B | 计 conflict +1，保留第一个值 A |
| 占位符 | 真实值 | 覆盖为真实值，不计冲突 |
| 真实值 | 占位符 | 忽略，保留真实值 |

#### 步骤 4：主键验证

```python
if not has_pk_key:
    rejects.append(Reject(line=stripped, reason="no_pk"))
    continue
record.pk = record.fields.get(pk_key) or None
records.append(record)
conflicts += line_conflicts
```

- 有 schema 时：`has_pk_key` 通过 `strip_key(raw_key) == pk_norm` 检测
- 无 schema 时：`has_pk_key` 初始为 `True`（因为 `ENTITY_LINE` 已匹配）
- `pk = None` 表示该行有主键列名但没有值

#### 步骤 5：返回结果

```python
return EntityTable(
    columns=list(columns),
    primary_key=primary_key,
    anchor_keys=list(anchor_keys),
    field_types=dict(field_types or {}),
    records=records,
    rejects=rejects,
    conflicts=conflicts,
)
```

### 3.5 完整解析流程示例

**输入文本**：
```
ID: 293 | entity_name: 某某基金 | fund_size: ~5000000000 | fund_type: 混合型基金
ID: 294 | Entity Name: 某某基金2 | fund_size: 3000000000 | 成立日期: 2020-01-01
ID: 295 | entity_name: 某某基金3 | fund_size: 2000000000 | fund_size: 2500000000
ID: 296 | Entity Name: 某某基金4 | fund_size: 无效值
一些非 KV 格式的文本
```

**解析过程**：

1. **第 1 行**：`ID: 293 | entity_name: 某某基金 | fund_size: ~5000000000 | fund_type: 混合型基金`
   - `entity_name` → schema 拼写 `entity_name` ✓
   - `fund_size` → `~5000000000` → 剥离 `~` → `5000000000`，`approx` 标记 `fund_size`
   - `fund_type` → schema 拼写 `fund_type` ✓
   - 结果：Record(pk="293", fields={entity_name: "某某基金", fund_size: "5000000000", fund_type: "混合型基金"}, approx={"fund_size"})

2. **第 2 行**：`ID: 294 | Entity Name: 某某基金2 | fund_size: 3000000000 | 成立日期: 2020-01-01`
   - `Entity Name` → `strip_key` → `entityname` → `key_map` → `entity_name` ✓
   - `成立日期` → 不在 schema 中，保留原始键名
   - 结果：Record(pk="294", fields={entity_name: "某某基金2", fund_size: "3000000000", "成立日期": "2020-01-01"})

3. **第 3 行**：`ID: 295 | entity_name: 某某基金3 | fund_size: 2000000000 | fund_size: 2500000000`
   - `fund_size` 出现两次，值不同（2000000000 vs 2500000000）
   - 首值 `2000000000` 保留，冲突计数 +1
   - 结果：Record(pk="295", fields={entity_name: "某某基金3", fund_size: "2000000000"}), conflicts=1

4. **第 4 行**：`ID: 296 | Entity Name: 某某基金4 | fund_size: 无效值`
   - 正常解析，所有字段非空
   - 结果：Record(pk="296", fields={entity_name: "某某基金4", fund_size: "无效值"})

5. **第 5 行**：`一些非 KV 格式的文本`
   - 不含 `:` → Reject("no_kv")

**返回结果**：

```
EntityTable(
    columns=["ID", "entity_name", "fund_size", "fund_type"],
    primary_key="ID",
    records=[Record, Record, Record, Record],  # 4 条记录
    rejects=[Reject(line="一些非 KV 格式的文本", reason="no_kv")],  # 1 条拒绝
    conflicts=1  # 1 次行内冲突
)
```

---

## 4. 辅助函数详解

### 4.1 `_normalize_kv_separators()`

```python
def _normalize_kv_separators(line: str) -> str:
```

**功能**：将行内 `key=value` 格式的片段重写为 `key: value` 格式，使后续解析只认 `:`。

**输入**：`line: str` — 可能包含 `=` 分隔符的 KV 行

**输出**：`str` — 统一使用 `: ` 分隔符的 KV 行

**逻辑步骤**：

1. 如果行内不包含 `=`，直接返回原行
2. 按 `|` 分割为片段
3. 对每个片段，如果 `=` 出现在 `:` 之前（或 `:` 不存在），将 `=` 替换为 `: `
4. 重新用 ` | ` 拼接

**示例**：

```
输入: "ID=293 | entity_name=某某基金"
输出: "ID: 293 | entity_name: 某某基金"

输入: "ID: 293 | entity_name=某某基金"  (混合使用)
输出: "ID: 293 | entity_name: 某某基金"  (已有 : 的片段不变)
```

**关键代码**：

```python
def _normalize_kv_separators(line: str) -> str:
    if "=" not in line:
        return line
    parts: list[str] = []
    for part in line.split("|"):
        stripped = part.strip()
        equals_at = stripped.find("=")
        colon_at = stripped.find(":")
        if equals_at >= 0 and (colon_at < 0 or equals_at < colon_at):
            key, _, val = stripped.partition("=")
            parts.append(f"{key.strip()}: {val.strip()}")
        else:
            parts.append(stripped)
    return " | ".join(parts)
```

---

### 4.2 `set_record_field()`

```python
def set_record_field(record: Record, col: str, val: str, writer: str) -> None:
```

**功能**：安全的字段值写入，保持 `approx` 集合和 `provenance` 追踪的一致性。

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `record` | `Record` | 目标记录 |
| `col` | `str` | 字段名 |
| `val` | `str` | 值（可能包含 `~` 前缀） |
| `writer` | `str` | 写入者标识（如 `"normalize"`、`"pre_merge"`） |

**逻辑步骤**：

1. 调用 `split_approx_tag(val)` 剥离 `~` 前缀，得到裸值和近似标记
2. 写入 `record.fields[col] = bare`
3. 如果有近似标记且裸值非空，将 `col` 加入 `record.approx`；否则从 `record.approx` 移除
4. 记录 `record.provenance[col] = writer`

**关键代码**：

```python
def set_record_field(record: Record, col: str, val: str, writer: str) -> None:
    bare, approx = split_approx_tag(val)
    record.fields[col] = bare
    if approx and bare:
        record.approx.add(col)
    else:
        record.approx.discard(col)
    record.provenance[col] = writer
```

---

### 4.3 `normalize_records()`

```python
def normalize_records(table: EntityTable) -> None:
```

**功能**：对每个 schema 列的值进行类型级清洗，就地修改 table 中的 records。

**输入**：`table: EntityTable` — 包含 records 和 field_types 的内存表

**输出**：`None`（就地修改 `table.records`）

**逻辑步骤**：

1. 通过 `metadata_by_name(table.columns, table.field_types)` 将 `field_types` 映射到 schema 列名的拼写
2. 对每个 record 的每个字段：
   - 跳过不在 schema 列集中的字段（未匹配 schema 的键不清洗也不丢）
   - 如果字段在 `approx` 中，回注 `~` 前缀
   - 调用 `_strip_cjk_ascii_spaces()` 去除中英文间多余空格
   - 调用 `clean_cell_for_type()` 进行类型级清洗
   - 通过 `set_record_field()` 写入清洗后的值，writer 标记为 `"normalize"`
3. 如果指定了主键，刷新 `record.pk`

**关键代码**：

```python
def normalize_records(table: EntityTable) -> None:
    field_types = metadata_by_name(table.columns, table.field_types)
    col_set = set(table.columns)
    pk = table.primary_key
    for record in table.records:
        for col in record.fields:
            if col not in col_set:
                continue
            raw = record.fields[col]
            if col in record.approx:
                raw = f"{APPROX_TAG}{raw}"
            cleaned = clean_cell_for_type(
                _strip_cjk_ascii_spaces(raw.strip()),
                field_types.get(col),
                field_name=col,
            )
            set_record_field(record, col, cleaned, "normalize")
        if pk:
            record.pk = record.fields.get(pk) or None
```

---

### 4.4 `normalize_local_record_ids()`

```python
def normalize_local_record_ids(table: EntityTable) -> tuple[int, int]:
```

**功能**：解析后归一化本地记录 ID 标签（如 `"档案 286"` → `"286"`），并丢弃范围记录（如 `"档案 197至217"`）。

**参数**：`table: EntityTable` — 包含 records 的内存表

**返回值**：`tuple[int, int]` — `(dropped_records, normalized_values)`

- `dropped_records`：被丢弃的范围记录数
- `normalized_values`：被归一化的记录 ID 数

**逻辑步骤**：

1. 对每个 record 的每个字段：
   - 判断是否为本地记录 ID 字段（字段名在 `RECORD_ID_FIELDS` 中，或 `field_type` 包含 `"local_record_id"`）
   - 如果是，检查值是否匹配 `RECORD_ID_RANGE`（如 `"197至217"`、`"197-217"`）→ 丢弃整个记录
   - 如果匹配 `RECORD_ID_LABEL`（如 `"档案 286"`）→ 提取数字部分，覆盖原值
2. 刷新 `record.pk`
3. 更新 `table.records` 为过滤后的列表

**关键代码**：

```python
def normalize_local_record_ids(table: EntityTable) -> tuple[int, int]:
    normalized = 0
    dropped = 0
    field_types = metadata_by_name(table.columns, table.field_types)
    kept: list[Record] = []
    for record in table.records:
        drop_record = False
        for key, val in list(record.fields.items()):
            field_type = field_types.get(key, "").lower()
            is_local_id = key.lower() in RECORD_ID_FIELDS or "local_record_id" in field_type
            if not is_local_id:
                continue
            if RECORD_ID_RANGE.search(val):
                drop_record = True
                break
            match = RECORD_ID_LABEL.match(val)
            if match:
                set_record_field(record, key, match.group(1), "record_id_normalize")
                normalized += 1
        if drop_record:
            dropped += 1
            continue
        if table.primary_key:
            record.pk = record.fields.get(table.primary_key) or None
        elif "ID" in record.fields:
            record.pk = record.fields.get("ID") or None
        kept.append(record)
    table.records = kept
    return dropped, normalized
```

**相关常量**：

```python
RECORD_ID_FIELDS = frozenset({
    "record_id", "archive_id", "file_id", "case_id", "entry_id", "local_id", "unit_id"
})
RECORD_ID_LABEL = re.compile(
    r"^(?:档案|记录|条目|战略单元|Record|File|Case)"
    r"\s*(?:\(?(?:ID|id|Id)[：:]\s*)?[：:#\-]?\s*(\d{1,8})$",
    re.IGNORECASE,
)
RECORD_ID_RANGE = re.compile(
    r"(?:档案|记录|条目|战略单元|Record|File|Case)?"
    r"\s*\d{1,8}\s*(?:至|到|[-~–—])\s*\d{1,8}",
    re.IGNORECASE,
)
```

---

### 4.5 `records_to_rows()`

```python
def records_to_rows(table: EntityTable) -> tuple[list[str], list[list[str]]]:
```

**功能**：将 `EntityTable` 的 records 投影到 `table.columns` 列，生成 CSV 行。

**参数**：`table: EntityTable`

**返回值**：`tuple[list[str], list[list[str]]]` — `(header, rows)`

- `header`：列名列表（`table.columns` 的副本）
- `rows`：数据行列表，每行按 header 顺序排列

**逻辑步骤**：

1. 使用 `table.columns` 作为 header
2. 对每个 record：
   - 按 header 顺序提取字段值
   - 如果字段值非空且在 `record.approx` 中，回注 `~` 前缀
   - 如果整行全空，丢弃该行（与旧 `deterministic_kv_to_csv` 语义一致）
3. 返回 `(header, rows)`

**关键代码**：

```python
def records_to_rows(table: EntityTable) -> tuple[list[str], list[list[str]]]:
    header = list(table.columns)
    rows: list[list[str]] = []
    for record in table.records:
        row: list[str] = []
        for col in header:
            val = record.fields.get(col, "")
            if val and col in record.approx:
                val = f"{APPROX_TAG}{val}"
            row.append(val)
        if any(row):
            rows.append(row)
    return header, rows
```

---

### 4.6 `table_to_kv_text()`

```python
def table_to_kv_text(table: EntityTable) -> str:
```

**功能**：将 EntityTable 渲染为 KV 文本行，仅用于人类可读的 trace 文件（`_cache/<stem>_clean.md`），**永不回读进管道**。

**参数**：`table: EntityTable`

**返回值**：`str` — 每行格式为 `col: val | col: val | ...`

**逻辑步骤**：

1. 对每个 record：
   - 按 `table.columns` 顺序输出字段（如果 columns 为空，使用 `record.fields` 的键顺序）
   - 近似值回注 `~` 前缀
   - 追加不在 columns 中的额外字段
2. 用换行符拼接

**关键代码**：

```python
def table_to_kv_text(table: EntityTable) -> str:
    lines: list[str] = []
    for record in table.records:
        cols: list[str] = table.columns or list(record.fields)
        parts: list[str] = []
        for col in cols:
            val = record.fields.get(col, "")
            if val and col in record.approx:
                val = f"{APPROX_TAG}{val}"
            parts.append(f"{col}: {val}")
        known: set[str] = set(cols)
        parts.extend(f"{key}: {val}" for key, val in record.fields.items() if key not in known)
        lines.append(" | ".join(parts))
    return "\n".join(lines)
```

---

### 4.7 `to_csv_text()`

```python
def to_csv_text(header: list[str], rows: list[list[str]]) -> str:
```

**功能**：将 header + rows 序列化为 CSV 文本（使用 `csv.writer` 处理引号转义）。

**参数**：
- `header: list[str]` — CSV 表头
- `rows: list[list[str]]` — CSV 数据行

**返回值**：`str` — 完整的 CSV 文本

**关键代码**：

```python
def to_csv_text(header: list[str], rows: list[list[str]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue()
```

---

### 4.8 `atomic_write_text()`

```python
def atomic_write_text(path: Path, text: str) -> None:
```

**功能**：原子写入文本文件（临时文件 + `os.replace`），确保读取方永远不会看到半写状态。

**参数**：
- `path: Path` — 目标文件路径
- `text: str` — 写入内容

**逻辑步骤**：

1. 在目标文件同目录下创建临时文件（前缀 `.文件名.`，后缀 `.tmp`）
2. 写入内容
3. `os.replace()` 原子替换目标文件
4. 异常时清理临时文件

**关键代码**：

```python
def atomic_write_text(path: Path, text: str) -> None:
    fd, tmp_name = tempfile.mkstemp(dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(tmp_name, path)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise
```

---

### 4.9 `sanitize_llm_value()`

```python
def sanitize_llm_value(val: str) -> str:
```

**功能**：LLM 文本进入 `Record.fields` 的唯一收口。截断推理泄漏、折叠控制字符和换行为空格、限制长度上限。

**参数**：`val: str` — LLM 输出的原始文本

**返回值**：`str` — 清洗后的值

**逻辑步骤**：

1. `strip_reasoning_leak(val)` — 截断推理泄漏文本（如 `"Source text..."`、`"然而，..."`）
2. 控制字符替换为空格（`\x00-\x1f\x7f`）
3. 折叠空白（`" ".join(cleaned.split())`）
4. 截断到 `MAX_LLM_VALUE_CHARS = 500` 字符

**关键代码**：

```python
MAX_LLM_VALUE_CHARS = 500
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")

def sanitize_llm_value(val: str) -> str:
    cleaned = strip_reasoning_leak(val)
    cleaned = _CONTROL_CHARS.sub(" ", cleaned)
    cleaned = " ".join(cleaned.split())
    return cleaned[:MAX_LLM_VALUE_CHARS].rstrip()
```

---

### 4.10 `_strip_cjk_ascii_spaces()`

```python
def _strip_cjk_ascii_spaces(s: str) -> str:
```

**功能**：去除中文字符与 ASCII 字符之间的多余空格。

**正则**：`([一-鿿㐀-䶿]) +([\x21-\x7e]) | ([\x21-\x7e]) +([一-鿿㐀-䶿])`

**示例**：

```
"某某 基金" → "某某基金"    (CJK + ASCII 间空格去掉)
"某某 Fund" → "某某Fund"    (CJK + ASCII 间空格去掉)
"hello 世界" → "hello世界"  (ASCII + CJK 间空格去掉)
"某某 基金 2020" → "某某基金2020"  (多次迭代)
```

**逻辑**：使用 while 循环反复替换，直到没有匹配（可能有多层 CJK-ASCII-CJK 交替）。

---

### 4.11 `strip_key()`（来自 `_merge.py`）

```python
def strip_key(s: str) -> str:
    return s.strip().lower().replace("_", "").replace("-", "")
```

**功能**：归一化键名以便比较。去空格、小写、去下划线、去连字符。

**示例**：

```
"Entity Name"    → "entityname"
"entity_name"    → "entityname"
"entity-name"    → "entityname"
"EntityName"     → "entityname"
```

---

### 4.12 `canonical_key_map()`（来自 `_merge.py`）

```python
def canonical_key_map(keys: list[str]) -> dict[str, str]:
```

**功能**：构建从归一化键名到 schema 标准拼写的映射。

**参数**：`keys: list[str]` — schema 列名列表

**返回值**：`dict[str, str]` — 归一化键名 → 标准拼写

**逻辑步骤**：

1. 对每个键调用 `strip_key()` 得到归一化形式
2. 映射到原始键名（schema 中的标准拼写）
3. 如果两个不同的 schema 列归一化后冲突，记录警告

**示例**：

```python
canonical_key_map(["entity_name", "fund_size", "fund_type"])
# 返回：
# {
#     "entityname": "entity_name",
#     "fundsize": "fund_size",
#     "fundtype": "fund_type",
# }
```

---

### 4.13 `canonicalize_key()`（来自 `_merge.py`）

```python
def canonicalize_key(key: str, key_map: dict[str, str]) -> str:
    return key_map.get(strip_key(key), key.strip())
```

**功能**：将任意拼写的键名归一化到 schema 标准拼写。未匹配的键保留原名（去空格后）。

**示例**：

```python
key_map = {"entityname": "entity_name", "fundsize": "fund_size"}
canonicalize_key("Entity Name", key_map)  → "entity_name"
canonicalize_key("entity-name", key_map)  → "entity_name"
canonicalize_key("成立日期", key_map)     → "成立日期"  # 未匹配，保留原名
```

---

### 4.14 `clean_cell_for_type()`（来自 `_merge.py`）

```python
def clean_cell_for_type(
    val: str,
    field_type: str | None = None,
    field_name: str | None = None,
) -> str:
```

**功能**：按字段类型清洗 KV 值，并执行轻量级 schema 类型检查。

**参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `val` | `str` | 待清洗的值（可能包含 `~` 前缀） |
| `field_type` | `str \| None` | 字段类型描述（如 `"number"`、`"date"`、`"boolean"`） |
| `field_name` | `str \| None` | 字段名（用于特殊字段的处理） |

**返回值**：`str` — 清洗后的值，如果值不合法返回空字符串

**逻辑步骤**：

1. `split_approx_tag(val)` 剥离 `~` 前缀，得到 `bare` 和 `approx` 标记
2. `clean_cell(bare)` 处理占位符和单位后缀
3. `strip_reasoning_leak(cleaned)` 截断推理泄漏
4. 基金类型字段（`fundtype`/`fund_type`/`fundtypename`）归一化：`"混合型基金"` → `"混合型"`
5. 名称/缩写字段（`_NAME_ABBR_FIELDS`）拦截纯数字值
6. 按字段类型清洗：

| 类型 | 行为 |
|------|------|
| `number` / `integer`（不含 `rank`） | 匹配 `NUMERIC_SCALAR`，否则返回空字符串；近似值保留 `~` 前缀 |
| `date` | 匹配 `DATE_SCALAR`（`YYYY`、`YYYY-MM-DD` 等），否则返回空字符串 |
| `boolean` | 归一化为 `"true"`/`"false"`，否则返回空字符串 |
| 其他 | 如果值本身是数字（去逗号后匹配 `NUMERIC_SCALAR`），去逗号返回；否则返回 `clean_cell(val)` |

**关键代码**：

```python
def clean_cell_for_type(
    val: str,
    field_type: str | None = None,
    field_name: str | None = None,
) -> str:
    bare, approx = split_approx_tag(val)
    cleaned = clean_cell(bare)
    if not cleaned:
        return ""
    cleaned = strip_reasoning_leak(cleaned)
    # 基金类型归一化
    if field_name:
        cleaned = _normalize_fund_type(cleaned, field_name)
    # 名称字段拦截纯数字
    if field_name and field_name.lower() in _NAME_ABBR_FIELDS:
        stripped_digits = cleaned.replace(",", "").replace(".", "").lstrip("-")
        if stripped_digits.isdigit():
            return ""
    # 按类型清洗
    type_text = (field_type or "").lower()
    if ("number" in type_text or "integer" in type_text) and "rank" not in type_text:
        # 数字类型：必须匹配 NUMERIC_SCALAR
        ...
    if "date" in type_text:
        return cleaned if DATE_SCALAR.match(cleaned) else ""
    if "boolean" in type_text:
        return normalize_boolean(cleaned) or ""
    # 其他类型：去逗号数字保留
    ...
```

**相关正则**：

```python
NUMERIC_SCALAR = re.compile(r"^[+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$")
DATE_SCALAR = re.compile(r"^\d{4}(?:[-/]\d{1,2}(?:[-/]\d{1,2})?)?$")
```

---

### 4.15 `split_approx_tag()`（来自 `_types.py`）

```python
def split_approx_tag(val: str) -> tuple[str, bool]:
```

**功能**：将值开头的 `~` 近似值标记剥离。

**参数**：`val: str` — 可能包含 `~` 前缀的值

**返回值**：`tuple[str, bool]` — `(剥离前缀后的值, 是否包含近似标记)`

**逻辑**：检查值是否以 `APPROX_PREFIXES` 中的任意前缀开头（`"~"`、`"～"`），如果是则剥离。

**关键代码**：

```python
def split_approx_tag(val: str) -> tuple[str, bool]:
    stripped = val.strip()
    for prefix in APPROX_PREFIXES:
        if stripped.startswith(prefix):
            return stripped[len(prefix):].strip(), True
    return stripped, False
```

---

### 4.16 `is_placeholder()`（来自 `_types.py`）

```python
def is_placeholder(val: str) -> bool:
    return val.lower().strip() in PLACEHOLDER_VALS or "(placeholder)" in val.lower()
```

**功能**：判断值是否为占位符。

**占位符集合**（`PLACEHOLDER_VALS`）：

```python
PLACEHOLDER_VALS = frozenset({
    "", "-", "null", "none", "nan", "n/a", "placeholder",
    "null (placeholder)", "none (placeholder)", "nan (placeholder)",
    "- (placeholder)", "0.0 (placeholder)", "redacted",
})
```

---

### 4.17 `metadata_by_name()`（来自 `_columns.py`）

```python
def metadata_by_name(
    names: Iterable[str],
    metadata: Mapping[str, T] | None,
) -> dict[str, T]:
```

**功能**：将元数据字典（如 `field_types`）的键映射到 schema 列名的拼写（不区分大小写）。

**参数**：

- `names: Iterable[str]` — schema 列名列表
- `metadata: Mapping[str, T] | None` — 按任意大小写拼写的元数据字典

**返回值**：`dict[str, T]` — 按 schema 列名拼写的元数据字典

**关键代码**：

```python
def metadata_by_name(names, metadata):
    if not metadata:
        return {}
    by_lower = {key.lower(): value for key, value in metadata.items()}
    return {name: by_lower[name.lower()] for name in names if name.lower() in by_lower}
```

---

## 5. 数据流

### 5.1 完整数据流图

```
压缩阶段输出（KV 文本）
  │
  │  ID: 293 | entity_name: 某某基金 | fund_size: ~5000000000
  │  ID: 294 | Entity Name: 某某基金2 | fund_size: 3000000000
  │  ID: 295 | entity_name: 某某基金3 | fund_size: 2000000000 | fund_size: 2500000000
  │  一些非 KV 格式的文本
  │  ## 标题行（Markdown 标题，跳过）
  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         parse_kv_text()                                   │
│                                                                           │
│  1. 行分割（splitlines） → 空行/Markdown 标题跳过                         │
│  2. _normalize_kv_separators() → 重写 = 为 :                              │
│  3. 非 KV 行 → Reject("no_kv")                                            │
│  4. 缺主键行 → Reject("no_pk")                                            │
│  5. 键名归一化（canonical_key_map + canonicalize_key）                     │
│  6. 值剥离 ~ 前缀（split_approx_tag）                                     │
│  7. 行内重复键处理                                                        │
│  8. 主键验证 → 构建 Record 或 Reject                                      │
└─────────────────────────────────────────────────────────────────────────┘
  │
  ▼
EntityTable
  ├── records: [Record, Record, …]    ← 成功解析的记录
  ├── rejects: [Reject("no_kv"), …]   ← 拒绝的行
  └── conflicts: N                     ← 行内重复键冲突数
  │
  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         normalize_records()                               │
│                                                                           │
│  对每个 schema 列的值进行类型级清洗：                                      │
│  - 中英文空格清理                                                         │
│  - 数字/日期/布尔类型校验                                                  │
│  - 基金类型归一化                                                          │
│  - 占位符处理                                                              │
│  - 推理泄漏截断                                                            │
└─────────────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     normalize_local_record_ids()                          │
│                                                                           │
│  - "档案 286" → "286"                                                     │
│  - "档案 197至217" → 丢弃整条记录                                         │
└─────────────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         merge_records()                                   │
│  （_merge.py）                                                             │
│                                                                           │
│  - 修复伪造的连续主键                                                      │
│  - 丢弃未锚定主键的行                                                      │
│  - 按主键分组合并                                                          │
│  - 通过辅助锚点挂接无主键片段                                              │
│  - 丢弃无数据字段的 identity-echo 片段                                     │
└─────────────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         records_to_rows()                                  │
│                                                                           │
│  - 投影到 table.columns 列                                                │
│  - 回注 ~ 前缀到 approx 字段                                              │
│  - 丢弃全空行                                                              │
│  → (header, rows)                                                         │
└─────────────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         to_csv_text()                                      │
│                                                                           │
│  - csv.writer 序列化                                                      │
│  → CSV 文本                                                               │
└─────────────────────────────────────────────────────────────────────────┘
  │
  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         atomic_write_text()                                │
│                                                                           │
│  - 临时文件 + os.replace                                                  │
│  → .csv 文件（原子写入）                                                   │
└─────────────────────────────────────────────────────────────────────────┘
```

### 5.2 关键设计决策

| 决策 | 理由 |
|------|------|
| 一次解析为内存表 | 避免每个阶段重复文本解析，减少 bug 源 |
| 值内剥离 `~` 到 `approx` 集合 | 后续阶段操作裸值，无需关心 `~` 前缀；序列化时再回注 |
| 未匹配 schema 的键保留 | 延迟投影到 `records_to_rows()`，避免信息丢失 |
| 行内重复键保留首值 | 占位符优先被真实值覆盖，非占位符的冲突计入 `conflicts` 但不影响数据 |
| 类型级清洗在 `normalize_records` 而非 parse 阶段 | 让 parse 阶段保持简单，只做格式解析；类型清洗单独处理，便于调试 |

### 5.3 管线调用示例

```python
# 1. Parse 阶段
table = parse_kv_text(
    text=compressed_kv_text,
    columns=["ID", "entity_name", "fund_size", "fund_type"],
    primary_key="ID",
    anchor_keys=["ID", "entity_name"],
    field_types={"fund_size": "number", "fund_type": "string"},
)

# 2. Normalize 阶段
normalize_records(table)

# 3. 本地记录 ID 归一化
normalize_local_record_ids(table)

# 4. Merge 阶段
merge_records(table, source_text=original_prose)

# 5. 序列化
header, rows = records_to_rows(table)
csv_text = to_csv_text(header, rows)
atomic_write_text(output_path, csv_text)
```

---

## 6. 复现检查清单

要在自己的项目中复现 Parse 阶段的行为，需要实现以下组件：

### 必要组件

| 组件 | 文件 | 说明 |
|------|------|------|
| `Record` | `_record.py` | 记录数据结构 |
| `EntityTable` | `_record.py` | 内存表数据结构 |
| `Reject` | `_record.py` | 拒绝行数据结构 |
| `parse_kv_text()` | `_record.py` | 核心解析函数 |
| `_normalize_kv_separators()` | `_record.py` | `=` → `:` 重写 |
| `set_record_field()` | `_record.py` | 安全字段写入 |
| `strip_key()` | `_merge.py` | 键名归一化 |
| `canonical_key_map()` | `_merge.py` | 键名映射构建 |
| `canonicalize_key()` | `_merge.py` | 键名归一化到 schema |
| `split_approx_tag()` | `_types.py` | `~` 前缀剥离 |
| `is_placeholder()` | `_types.py` | 占位符检测 |
| `ENTITY_LINE` | `_constants.py` | 实体行正则 |
| `SECTION_HEADING` | `_constants.py` | 标题行正则 |
| `NO_SCHEMA_PK` | `_record.py` | 无 schema 时主键名 |

### 可选组件

| 组件 | 文件 | 说明 |
|------|------|------|
| `normalize_records()` | `_record.py` | 类型级清洗 |
| `normalize_local_record_ids()` | `_record.py` | 本地 ID 归一化 |
| `records_to_rows()` | `_record.py` | CSV 行投影 |
| `table_to_kv_text()` | `_record.py` | trace 文件渲染 |
| `to_csv_text()` | `_record.py` | CSV 序列化 |
| `atomic_write_text()` | `_record.py` | 原子写入 |
| `sanitize_llm_value()` | `_record.py` | LLM 值收口 |
| `clean_cell_for_type()` | `_merge.py` | 类型级值清洗 |
| `metadata_by_name()` | `_columns.py` | 元数据拼写映射 |

---

## 附录：常量与正则速查

### 键名归一化相关

```python
NO_SCHEMA_PK = "ID"  # 无 schema 时的默认主键名

# strip_key 行为
# 输入: "Entity Name"  → 输出: "entityname"
# 等价于: s.strip().lower().replace("_", "").replace("-", "")
```

### 行模式匹配

```python
ENTITY_LINE     = re.compile(r"^ID:\s*", re.MULTILINE)
SECTION_HEADING = re.compile(r"^(#{2,4})\s+(.+)$", re.MULTILINE)
```

### 近似值和占位符

```python
APPROX_TAG      = "~"
APPROX_PREFIXES = ("~", "～")

PLACEHOLDER_VALS = frozenset({
    "", "-", "null", "none", "nan", "n/a", "placeholder",
    "null (placeholder)", "none (placeholder)", "nan (placeholder)",
    "- (placeholder)", "0.0 (placeholder)", "redacted",
})
```

### 标量校验

```python
NUMERIC_SCALAR = re.compile(r"^[+-]?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?$")
DATE_SCALAR    = re.compile(r"^\d{4}(?:[-/]\d{1,2}(?:[-/]\d{1,2})?)?$")
```

### 本地记录 ID

```python
RECORD_ID_FIELDS = frozenset({
    "record_id", "archive_id", "file_id", "case_id", "entry_id", "local_id", "unit_id"
})
RECORD_ID_LABEL = re.compile(
    r"^(?:档案|记录|条目|战略单元|Record|File|Case)"
    r"\s*(?:\(?(?:ID|id|Id)[：:]\s*)?[：:#\-]?\s*(\d{1,8})$",
    re.IGNORECASE,
)
RECORD_ID_RANGE = re.compile(
    r"(?:档案|记录|条目|战略单元|Record|File|Case)?"
    r"\s*\d{1,8}\s*(?:至|到|[-~–—])\s*\d{1,8}",
    re.IGNORECASE,
)
```

### 控制字符

```python
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")
MAX_LLM_VALUE_CHARS = 500
```

### CJK-ASCII 空格

```python
_CJK_ASCII_SPACE = re.compile(
    r"([一-鿿㐀-䶿]) +([\x21-\x7e])"
    r"|"
    r"([\x21-\x7e]) +([一-鿿㐀-䶿])"
)
```

### 名称/缩写字段列表（数字拦截）

```python
_NAME_ABBR_FIELDS = frozenset({
    "secuabbr", "chiname", "chinameabbr", "chinamabbr",
    "fund_name", "fund_name_short", "name", "abbr",
    "companyname", "abbrchiname",
})
```