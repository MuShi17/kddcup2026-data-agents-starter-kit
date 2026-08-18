# Phase 2: Repair & Reconcile（修复与调和）

## 概述

Phase 2 包含三个子任务，处理压缩后解析阶段遗留的问题：

1. **格式修复（Fix）** — 修复解析失败的文本行
2. **字段名调和（Reconcile）** — 将变体字段名映射到 schema 规范名
3. **ID 冲突解决（Resolve ID Conflicts）** — 修复系统性 ID 偏移

**位置：** `_reconcile.py`（637 行），`_merge.py`（resolve_id_conflicts 部分）

---

## 数据流

```
EntityTable (包含 records + rejects)
      │
      ▼
┌─────────────────────────────────────┐
│  fix_malformed_lines()             │
│  修复解析失败的文本行                │
│  成功 → 加入 records                │
│  失败 → 保留在 rejects              │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  reconcile_field_names()            │
│  变体字段名 → schema 规范名          │
│  "patient_id" → "uniquepid"         │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  resolve_id_conflicts()             │
│  修复系统性 ID 偏移                  │
│  同一实体不同章节的 ID 不一致         │
└──────────────┬──────────────────────┘
               │
               ▼
        EntityTable (已修复)
```

---

## 1. 格式修复 — `fix_malformed_lines()`

**位置：** `_reconcile.py` 第 143 行

**函数签名：**
```python
def fix_malformed_lines(
    adapter: ModelAdapter,
    table: EntityTable,
    schema_field_defs: dict[str, str] | None,
) -> int
```

**参数：**
| 参数 | 类型 | 说明 |
|------|------|------|
| `adapter` | `ModelAdapter` | LLM 适配器 |
| `table` | `EntityTable` | 包含 rejects 的内存表（会被修改） |
| `schema_field_defs` | `dict[str, str] \| None` | 字段定义映射 |

**返回值：** `int` — 成功恢复的记录数

**核心逻辑（最多 3 次重试）：**

```
Step 1: 检查 table.rejects 是否为空 → 直接返回 0
Step 2: 取前 FIX_FORMAT_MAX_LINES 条 reject 行
Step 3: 构建 LLM prompt：
  - 目标 schema 列
  - 主键字段
  - 字段定义
  - 编号的重写行 (1) ... / 2) ...)
Step 4: 调用 LLM 重写
Step 5: 解析 LLM 回复中的编号行
Step 6: 验证编号完整性（必须 1..N 全部出现）
Step 7: 对每条重写行调用 parse_kv_text() 重新解析
Step 8: 成功 → 加入 table.records
Step 9: 失败 → 保留在 table.rejects
Step 10: 重复直到无 rejects 或达到最大重试次数
```

**关键代码：**
```python
for attempt in range(1, FIX_FORMAT_MAX_RETRIES + 1):  # 最多 3 次
    if not table.rejects:
        return total_recovered
    
    batch = list(table.rejects[:FIX_FORMAT_MAX_LINES])
    prompt = f"Target schema columns: {schema_block}\n..."
    response = adapter.complete(messages)
    
    # 解析编号回复
    numbered: dict[int, str] = {}
    for line in content.splitlines():
        match = re.match(r"^\s*(\d+)\)\s*(.+?)\s*$", line)
        if match:
            numbered[int(match.group(1))] = match.group(2)
    
    # 验证编号完整性
    expected = set(range(1, len(batch) + 1))
    if set(numbered) != expected:
        break  # 编号不完整，丢弃本次修复
    
    # 重新解析每条重写行
    for idx, original in enumerate(batch, start=1):
        candidate = numbered[idx]
        parsed = parse_kv_text(candidate, columns=table.columns, ...)
        if parsed.records:
            table.records.extend(parsed.records)
            recovered += 1
        else:
            kept_rejects.append(original)
```

---

## 2. 字段名调和 — `reconcile_field_names()`

**位置：** `_reconcile.py` 第 264 行

**函数签名：**
```python
def reconcile_field_names(
    adapter: ModelAdapter,
    table: EntityTable,
    schema_field_defs: dict[str, str] | None,
) -> dict[str, str]
```

**参数：**
| 参数 | 类型 | 说明 |
|------|------|------|
| `adapter` | `ModelAdapter` | LLM 适配器 |
| `table` | `EntityTable` | 包含未匹配字段名的内存表（会被修改） |
| `schema_field_defs` | `dict[str, str] \| None` | 字段定义映射 |

**返回值：** `dict[str, str]` — `{变体名: 规范名}` 映射

**核心逻辑：**

```
Step 1: 收集所有未匹配 schema 的字段名
  - 遍历所有记录的 fields
  - 不在 table.columns 中的 key 视为未匹配
  - 去重（大小写不敏感）

Step 2: 构建 LLM prompt：
  - 目标 schema 列（含定义）
  - 未匹配的字段名列表
  - 要求判断每个未匹配字段是否是某个 schema 列的同义词

Step 3: 调用 LLM 裁决
  - 输出格式：<unmatched>=<schema_column> 或 <unmatched>=NONE

Step 4: 解析 LLM 回复
  - _parse_alias_response() 解析映射

Step 5: 应用映射
  - 对每条记录，将变体字段名的值迁移到规范字段名
  - 保护已填充的目标字段（不覆盖）
  - 更新 approx 和 provenance 集合
```

**LLM Prompt：**
```
Target schema columns:
- col_a: definition of col_a
- col_b: definition of col_b

The following field names appeared in the extraction output but do NOT
match any schema column (case-insensitive):
- patient_id
- height_cm

For each unmatched field, decide whether it is an EXACT synonym of one
schema column — same concept, same entity scope, same semantics.
- Match → output: <unmatched>=<schema_column>
- No match or unsure → output: <unmatched>=NONE
```

**关键代码：**
```python
# 收集未匹配字段
unmatched = []
for record in table.records:
    for key in record.fields:
        if key not in col_set and key.lower() not in seen_lower:
            unmatched.append(key)
            seen_lower.add(key.lower())

# LLM 裁决
mapping = _adjudicate_synonym_columns(adapter, ...)

# 应用映射
for record in table.records:
    for src, dst in mapping.items():
        actual_src = next((k for k in record.fields if k.lower() == src.lower()), None)
        if actual_src is None:
            continue
        dst_val = record.fields.get(dst)
        if dst_val is not None and not is_placeholder(dst_val):
            continue  # 目标已有值，不覆盖
        record.fields[dst] = record.fields.pop(actual_src)
        # 更新 approx 和 provenance
```

---

## 3. ID 冲突解决 — `resolve_id_conflicts()`

**位置：** `_merge.py` 第 63 行

**函数签名：**
```python
def resolve_id_conflicts(table: EntityTable) -> int
```

**参数：** `table` — EntityTable（会被修改）

**返回值：** `int` — 修复的冲突数

**问题：** 文档中同一实体在不同章节被分配了不同 ID。例如实体 "X" 在第一节是 ID 365，在第二节是 ID 369（系统性偏移）。

**核心逻辑：**

```
Step 1: 构建规范名称→ID 映射（取首次出现）
  - 遍历所有记录
  - 对每条记录，取其 name 字段和 ID 字段
  - 如果 name 首次出现，记录其 ID 为规范 ID

Step 2: 扫描所有记录
  - 如果记录的 (name, ID) 与规范映射不一致
  - 且规范映射中存在该 name
  - 则将 ID 修正为规范 ID

Step 3: 返回修复的冲突数
```

**关键代码：**
```python
def resolve_id_conflicts(table: EntityTable) -> int:
    if table.primary_key:
        return 0  # 有主键时不需要此步骤
    
    # 构建规范映射
    name_to_first_id: dict[str, int] = {}
    for record in table.records:
        name = record.fields.get("name", "")
        rid = record.fields.get(table.primary_key or "ID", "")
        if name and rid and name not in name_to_first_id:
            name_to_first_id[name] = rid
    
    # 修复偏移
    fixed = 0
    for record in table.records:
        name = record.fields.get("name", "")
        current_rid = record.fields.get(table.primary_key or "ID", "")
        canonical_rid = name_to_first_id.get(name)
        if canonical_rid and current_rid != canonical_rid:
            record.fields[table.primary_key or "ID"] = canonical_rid
            fixed += 1
    
    return fixed
```

---

## 辅助函数

### `_parse_alias_response()`

```python
def _parse_alias_response(
    text: str,
    discovered: list[str],
    canonicals: list[str],
) -> dict[str, str]
```

**说明：** 解析 LLM 返回的 `discovered=canonical` / `discovered=DISTINCT` 裁决行。

**逻辑：**
1. 按 `,` / `;` / 换行分割
2. 对每行，按 `=` 分割为左右
3. 左值必须在 `discovered` 中
4. 右值如果是 `DISTINCT` → 跳过
5. 右值必须在 `canonicals` 中
6. 返回 `{discovered: canonical}` 映射

### `strip_key()`

```python
def strip_key(s: str) -> str
```

**说明：** 归一化 key 用于比较：去空格、转小写、去 `_` 和 `-`。

### `canonical_key_map()`

```python
def canonical_key_map(keys: list[str]) -> dict[str, str]
```

**说明：** 构建归一化 key → 规范拼写的映射。检测拼写冲突。

### `canonicalize_key()`

```python
def canonicalize_key(key: str, key_map: dict[str, str]) -> str
```

**说明：** 将 key 归一化后通过 `key_map` 查找规范拼写。未找到时返回原 key。

### `line_has_kv_key()`

```python
def line_has_kv_key(line: str, key: str) -> bool
```

**说明：** 检查 KV 行是否包含指定 key 的字段。

---

## 常量定义

| 常量 | 值 | 说明 |
|------|:---:|------|
| `FIX_FORMAT_MAX_LINES` | 配置值 | 单次修复的最大行数 |
| `FIX_FORMAT_MAX_RETRIES` | `3` | 最大重试次数 |

---

## 完整流程

```
EntityTable (records + rejects)
      │
      ▼
┌─────────────────────────────────────┐
│  fix_malformed_lines()              │
│  ┌───────────────────────────────┐  │
│  │ 取 rejects 前 N 条             │  │
│  │ → LLM 重写                     │  │
│  │ → parse_kv_text 重新解析       │  │
│  │ → 成功 → records              │  │
│  │ → 失败 → 保留 rejects          │  │
│  │ → 重复最多 3 次                │  │
│  └───────────────────────────────┘  │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  reconcile_field_names()            │
│  ┌───────────────────────────────┐  │
│  │ 收集未匹配字段名                │  │
│  │ → LLM 裁决映射到 schema 列     │  │
│  │ → 迁移值到规范字段名            │  │
│  └───────────────────────────────┘  │
└──────────────┬──────────────────────┘
               │
               ▼
┌─────────────────────────────────────┐
│  resolve_id_conflicts()             │
│  ┌───────────────────────────────┐  │
│  │ 构建规范 name→ID 映射          │  │
│  │ → 扫描修复偏移 ID              │  │
│  └───────────────────────────────┘  │
└──────────────┬──────────────────────┘
               │
               ▼
        EntityTable (已修复)
```

---

## 复现关键点

1. **Fix 的编号协议：** LLM 回复必须包含 `1)` / `2)` 等编号，且编号必须完整（1..N 全部出现）。不完整的回复被丢弃。
2. **Reconcile 的保护逻辑：** 目标字段已有非占位符值时，不覆盖。这防止 LLM 将多个变体映射到同一个已填充的字段。
3. **ID 冲突的假设：** 假设首次出现的 name→ID 映射是正确的。只修复系统性偏移，不处理随机错误。
4. **所有修改都通过 `set_record_field()` 写入：** 确保 `approx` 和 `provenance` 集合始终一致。