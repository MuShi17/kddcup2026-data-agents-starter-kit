# Phase 7 — Verify（值级验证）

## 1. 概述

### 1.1 在 ETL 管线中的位置

Verify 阶段是整个 ETL 管线的**倒数第二阶段**，位于 Merge（值级合并）之后、Output（序列化输出）之前。它属于**值级修正**（value-level retry）系列操作之一，与 `retry_missing_values`（回填空缺值）、`dedup_cross_field_copies`（去重跨字段副本）并列。

管线数据流（相关阶段）：

```
Parsing → Merge → Normalize → Retry → Dedup → Verify → Output
```

Verify 阶段直接操作 `EntityTable`（in-place 修改），不产生新的数据结构。

### 1.2 目标

用 LLM 比对**当前提取值**与**原始源文本**，检测并修正以下三类错误：

| 错误类型 | LLM 输出标记 | 行为 |
|----------|--------------|------|
| 值错误（提取值与原文不符） | `field_name: correct_value` | 用新值覆盖 |
| 值不存在于原文（幻觉） | `field_name: NONE` | 仅当现值为空时标记（实际不写入） |
| 全部正确 | `ALL_CORRECT` | 跳过该实体 |

### 1.3 为什么只验证采样实体

LLM 调用成本高，且大部分提取值在压缩（Compress）阶段已经正确。Verify 通过 `_sample_verify_entities()` 选择**源段落最多的实体**进行抽样验证——这些实体跨段落拼接，价值混淆概率最高，LLM 校验的边际收益最大。

---

## 2. 常量与配置

```python
# 文件：_constants.py
MAX_VERIFY_ENTITIES = 50    # 最多验证的实体数
MAX_WORKERS = 4             # 并发线程数
LLM_CALL_TIMEOUT = 180      # 单次 LLM 调用超时（秒）
```

---

## 3. 辅助函数

### 3.1 `_records_by_pk()`

```python
def _records_by_pk(table: EntityTable) -> dict[str, list[Record]]:
```

**输入：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | `EntityTable` | 内存表，包含 `records: list[Record]` |

**返回值：** `dict[str, list[Record]]` — 主键值 → 记录列表的映射。

**核心逻辑：**
1. 遍历 `table.records` 中所有 `Record` 对象
2. 对每个 `record.pk` 非空的记录，按 `pk` 分组
3. 返回 `{pk_value: [record1, record2, ...]}` 字典

**关键代码：**
```python
by_pk: dict[str, list[Record]] = {}
for record in table.records:
    if record.pk:
        by_pk.setdefault(record.pk, []).append(record)
return by_pk
```

> **注意：** 一个主键可能对应多个 `Record`（合并前的碎片），但 Verify 阶段只读取第一个 `Record` 的 `fields` 进行验证，修正会应用到该主键下的**所有** `Record`。

---

### 3.2 `_apply_llm_value()`

```python
def _apply_llm_value(
    table: EntityTable,
    record: Record,
    col: str,
    val: str,
    writer: str,
) -> None:
```

**LLM 修正值落地的唯一路径。** 所有来自 LLM 的修正值必须经过此函数写入，以保证类型安全和格式一致。

**输入：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | `EntityTable` | 内存表（用于获取字段类型定义） |
| `record` | `Record` | 要修改的记录 |
| `col` | `str` | 字段名 |
| `val` | `str` | LLM 输出的原始值文本 |
| `writer` | `str` | 写入者标识（如 `"verify"`、`"retry"`） |

**核心逻辑步骤：**
1. 调用 `sanitize_llm_value(val)` 清理 LLM 文本（去推理泄漏、控制字符、长度截断）
2. 调用 `metadata_by_name(table.columns, table.field_types)` 获取字段类型映射
3. 调用 `clean_cell_for_type(cleaned, field_types.get(col), field_name=col)` 按类型清洗
4. 调用 `set_record_field(record, col, cleaned, writer)` 写入 Record

**关键代码：**
```python
def _apply_llm_value(table: EntityTable, record: Record, col: str, val: str, writer: str) -> None:
    field_types = metadata_by_name(table.columns, table.field_types)
    cleaned = clean_cell_for_type(
        sanitize_llm_value(val),
        field_types.get(col),
        field_name=col,
    )
    set_record_field(record, col, cleaned, writer)
```

**数据流：**
```
LLM 原始文本
    → sanitize_llm_value()     # 截断推理泄漏、清理控制字符、限制长度
    → clean_cell_for_type()    # 数值校验、日期格式校验、fund-type 归一化
    → set_record_field()       # 写入 Record.fields + 更新 approx/provenance
```

---

### 3.3 `_sample_verify_entities()`

```python
def _sample_verify_entities(
    table: EntityTable,
    entity_groups: dict[str, list[str]],
) -> list[str]:
```

**选择验证实体。** 优先选择源段落数最多的实体（跨段落混淆概率更高）。

**输入：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `table` | `EntityTable` | 内存表 |
| `entity_groups` | `dict[str, list[str]]` | 主键 → 源段落列表的映射 |

**返回值：** `list[str]` — 选中的主键 ID 列表，最长 `MAX_VERIFY_ENTITIES`（50）个。

**核心逻辑：**
1. 遍历所有 `Record`，筛选出 `pk` 非空且存在于 `entity_groups` 中的实体
2. 构建 `(pk, len(entity_groups[pk]))` 元组列表
3. 按段落数**降序**排序
4. 取前 `MAX_VERIFY_ENTITIES` 个主键

**关键代码：**
```python
candidates: list[tuple[str, int]] = []
for record in table.records:
    if record.pk and record.pk in entity_groups:
        candidates.append((record.pk, len(entity_groups[record.pk])))
candidates.sort(key=lambda x: -x[1])
return [pk for pk, _ in candidates[:MAX_VERIFY_ENTITIES]]
```

---

## 4. 核心函数：`verify_field_values()`

```python
def verify_field_values(
    adapter: ModelAdapter,
    table: EntityTable,
    entity_groups: dict[str, list[str]] | None,
    schema_field_defs: dict[str, str] | None = None,
    compress_guide: str | None = None,
) -> None:
```

**入口函数。** 对采样实体并行发起 LLM 验证，应用修正到 `EntityTable`（in-place）。

### 4.1 输入参数

| 参数 | 类型 | 说明 |
|------|------|------|
| `adapter` | `ModelAdapter` | LLM 调用适配器 |
| `table` | `EntityTable` | 内存表（被 in-place 修改） |
| `entity_groups` | `dict[str, list[str]] \| None` | 主键 → 源段落列表 |
| `schema_field_defs` | `dict[str, str] \| None` | 字段名 → 字段定义描述 |
| `compress_guide` | `str \| None` | 压缩阶段生成的字段映射指南 |

### 4.2 返回值

无（`None`）。`table` 被 in-place 修改。

### 4.3 核心逻辑步骤

#### 步骤 1：前置检查与采样

```python
primary_key = table.primary_key
if not primary_key or not entity_groups or not table.columns or not table.records:
    return

verify_ids = _sample_verify_entities(table, entity_groups)
if not verify_ids:
    return
```

#### 步骤 2：确定待验证字段

排除两类字段：
- **锚点/身份字段**（`anchor_keys`）：这些字段的值来自身份识别阶段，不参与值级验证
- **日期字段**：日期格式校验由类型清洗阶段处理，不在此验证

```python
skip_lower = {a.lower() for a in table.anchor_keys}
types = metadata_by_name(table.columns, table.field_types)
verify_cols = [
    c for c in table.columns
    if c.lower() not in skip_lower and "date" not in types.get(c, "").lower()
]
```

#### 步骤 3：构建 Prompt 模板

LLM Prompt 包含以下区块：

```
1. 字段列表：Fields to verify: {col_list}
2. 字段定义（可选）：{defs_block}
3. 压缩指南（可选）：{guide_block}
4. 当前提取值：CURRENT EXTRACTION: {current_kv}
5. 源文本：SOURCE TEXT: {source_text}
6. 验证规则（重要）：
   - 匹配精确标签（"Reserve Assets" ≠ "Total Assets"）
   - 缩写字段三级规则（SecuAbbr 必须是最短名称形式）
   - 纯数字值在 SecuAbbr 中一定是错误的
   - 已有合理值时不要输出 NONE
7. 输出格式要求：
   - 错误字段：field_name: correct_value
   - 不存在字段：field_name: NONE
   - 全部正确：ALL_CORRECT
   - 只输出错误字段，不重复正确字段
```

#### 步骤 4：并行执行验证

```python
all_corrections: dict[str, dict[str, str]] = {}
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futs = {submit_in_context(pool, _verify_one, rid): rid for rid in verify_ids}
    try:
        for fut in as_completed(futs, timeout=LLM_CALL_TIMEOUT * len(verify_ids)):
            try:
                rid, corr = fut.result(timeout=LLM_CALL_TIMEOUT)
                if corr:
                    all_corrections[rid] = corr
            except Exception:
                pass
    except TimeoutError:
        logger.warning("ETL field verify: pool timed out")
```

#### 步骤 5：应用修正

```python
for rid, corrections in all_corrections.items():
    for record in by_pk.get(rid, []):
        for col, val in corrections.items():
            if val == "":
                continue  # NONE 修正在解析层已被拦截
            _apply_llm_value(table, record, col, val, "verify")
```

---

## 5. 单实体验证：`_verify_one()` 闭包

这是 `verify_field_values()` 内部定义的闭包函数，对单个实体执行 LLM 验证。

```python
def _verify_one(rid: str) -> tuple[str, dict[str, str]]:
```

### 5.1 输入与输出

| 项目 | 说明 |
|------|------|
| 输入 | `rid: str` — 实体主键 ID |
| 返回值 | `tuple[str, dict[str, str]]` — `(rid, {field_name: corrected_value, ...})` |
| 副作用 | 无（修正结果通过返回值传递，由外层统一写入） |

### 5.2 构建验证 Prompt

```python
current_kv = " | ".join(f"{c}: {row.get(c, '')}" for c in verify_cols)
source_text = "\n\n".join(source_paras)

prompt = (
    f"Verify each field value against the source text.\n"
    f"Fields to verify: {col_list}\n"
    f"{defs_block}"
    f"{guide_block}\n"
    f"CURRENT EXTRACTION:\n{current_kv}\n\n"
    f"SOURCE TEXT:\n{source_text}\n\n"
    # ... 详细规则 ...
    f"For each field whose extracted value is WRONG, output:\n"
    f"  field_name: correct_value\n"
    f"If the source text does not mention a field at all, output:\n"
    f"  field_name: NONE\n"
    f"Output ONLY wrong fields, one per line. "
    f"NEVER repeat correct values. NEVER output all fields. "
    f"No commentary or analysis.\n"
    f"If all values are correct, output: ALL_CORRECT\n"
)
```

### 5.3 LLM 输出格式

LLM 应返回以下三种格式之一：

| 格式 | 含义 |
|------|------|
| `field_name: correct_value` | 该字段值错误，correct_value 为正确值 |
| `field_name: NONE` | 源文本中不存在该字段的值 |
| `ALL_CORRECT` | 所有字段值均正确 |

**允许多行输出**（每行一个字段），也允许 pipe 分隔的单行：
```
totalprofit: 23890000
secuabbr: 大摩资源
```

或 pipe 格式：
```
totalprofit: 23890000 | secuabbr: 大摩资源
```

### 5.4 结果解析流程

#### 5.4.1 预处理

```python
reply = response.content.strip()
if reply.startswith("```"):
    reply = reply.split("\n", 1)[-1].rsplit("```", 1)[0].strip()
if "ALL_CORRECT" in reply.upper():
    return rid, {}
```

#### 5.4.2 清除 LLM 推理泄漏

```python
# Strip LLM commentary: stop at blank lines or markdown headers
clean_lines: list[str] = []
for line in reply.splitlines():
    stripped = line.strip()
    if not stripped:
        break
    if stripped.startswith(("**", "#")):
        break
    clean_lines.append(line)
```

在遇到空行、粗体标记 `**` 或 Markdown 标题 `#` 时截断——这些标记后的内容被认为是 LLM 的推理泄漏，不是结构化输出。

#### 5.4.3 展开 Pipe 分隔行

```python
segments: list[str] = []
for line in clean_lines:
    if "|" in line and ":" in line.split("|", 1)[1]:
        segments.extend(seg.strip() for seg in line.split("|") if seg.strip())
    else:
        segments.append(line)
```

#### 5.4.4 解析单字段修正

```python
for seg in segments:
    if ":" not in seg:
        continue
    key, _, val = seg.partition(":")
    key = col_by_lower.get(key.strip().lower())
    if not key:
        continue
    parsed = _parse_correction(key, val)
    if parsed is None:
        continue
    # ... 后续比较和冲突检测 ...
```

---

## 6. 单字段解析：`_parse_correction()`

```python
def _parse_correction(field: str, raw_val: str) -> str | None:
```

### 6.1 输入与输出

| 项目 | 说明 |
|------|------|
| 输入 `field` | `str` — 字段名 |
| 输入 `raw_val` | `str` — LLM 输出的原始值 |
| 返回值 | `str \| None` — 解析后的值，`None` 表示解析失败应跳过 |

### 6.2 核心逻辑

```python
def _parse_correction(field: str, raw_val: str) -> str | None:
    val = strip_reasoning_leak(raw_val.strip())
    if not val:
        return None
    if val.upper().startswith("NONE"):
        return ""          # NONE 标记 → 空字符串
    type_text = types.get(field, "").lower()
    if ("number" in type_text or "integer" in type_text) and "rank" not in type_text:
        normalized = val.replace(",", "")
        try:
            float(normalized)
        except ValueError:
            return None    # 数值字段值不可解析为数字 → 拒绝
        return normalized  # 去掉逗号后的纯数字形式
    return val             # 非数值字段直接返回
```

**解析规则：**

| 条件 | 行为 |
|------|------|
| `strip_reasoning_leak` 后为空 | 返回 `None`（跳过） |
| 值以 `NONE`（不区分大小写）开头 | 返回 `""`（空字符串，表示该字段不存在于源文本） |
| 数值/整数类型字段（非 rank） | 去掉逗号后尝试 `float()` 解析，成功则返回去掉逗号的字符串，失败返回 `None` |
| 其他类型字段 | 直接返回原始值 |

---

## 7. 交叉字段碰撞保护

### 7.1 为什么需要

LLM 在验证时可能将**一个字段的源文本值错误地分配到另一个字段**。例如，源文本中 "Reserve Assets: 100万" 和 "Total Assets: 500万" 同时出现，LLM 可能将 "100万" 修正到 Total Assets 字段。

### 7.2 保护机制

在解析 LLM 输出之前，预先构建两类保护集：

```python
_anchor_vals: set[str] = set()
_typed_vals: dict[str, set[str]] = {}
for c in table.columns:
    v = row.get(c, "").strip()
    if not v:
        continue
    if c.lower() in anchor_lower:
        _anchor_vals.add(v)           # 保护集 1：锚点/身份字段的值
    else:
        t = types.get(c, "").lower()
        if t:
            _typed_vals.setdefault(t, set()).add(v)  # 保护集 2：按类型分组的字段值
```

### 7.3 `_is_cross_field_collision()`

```python
def _is_cross_field_collision(field: str, value: str) -> bool:
    if value in _anchor_vals:
        return True
    ft = types.get(field, "").lower()
    if not ft:
        return False
    return any(value in vs for ot, vs in _typed_vals.items() if ot != ft)
```

**逻辑：**

| 条件 | 判定为碰撞 |
|------|-----------|
| 修正值等于某个**锚点/身份字段**的现有值 | 是（锚点字段值不能被覆盖） |
| 修正值的字段类型与值来源字段类型**不同** | 是（不同类型的字段值不能互相赋值） |
| 其他情况 | 否 |

**示例：**

| 场景 | 修正值 | 目标字段 | 目标字段类型 | 现有值 | 结果 |
|------|--------|----------|-------------|--------|------|
| 锚点碰撞 | `"300"` | `totalassets` | number | `"300"` 在 `secucode`（锚点）中 | 拒绝 |
| 类型碰撞 | `"5000000"` | `totalassets` | number | `"5000000"` 在 `reserveassets`（也是 number）中 | 拒绝（如果字段名不同，但类型相同且值相同，说明可能是跨字段赋值） |
| 安全 | `"23890000"` | `totalprofit` | number | 不存在于任何其他字段 | 接受 |

### 7.4 碰撞保护的触发时机

```python
if _is_cross_field_collision(key, parsed):
    logger.warning(
        "ETL field verify: rejecting correction %s=%r for entity %s "
        "(collides with anchor or cross-type field value)",
        key, parsed, rid,
    )
    continue
```

---

## 8. 应用修正时的额外保护

### 8.1 只修正发生变化的值

```python
current = row.get(key, "").strip()
key_type = types.get(key, "").lower()
if ("number" in key_type or "integer" in key_type) and "rank" not in key_type:
    current = current.replace(",", "")
if parsed != current:
    # ... 应用修正
```

### 8.2 NONE 修正保护

```python
if parsed == "" and current:
    logger.warning(
        "ETL field verify: rejecting NONE correction for %s "
        "(entity %s already has value %r)",
        key, rid, current,
    )
    continue
```

**规则：** 如果 LLM 说字段值不存在（NONE），但当前字段**已有非空值**，则拒绝该修正。这防止 LLM 错误地删除从源文本中正确提取的值。

### 8.3 最终写入时的过滤

```python
if val == "":
    continue  # NONE 修正在解析层已被拦截（非空现值）或为 no-op
_apply_llm_value(table, record, col, val, "verify")
```

---

## 9. 完整执行流程

```
verify_field_values()
│
├─ 1. 前置检查（primary_key, entity_groups, columns, records）
│
├─ 2. _sample_verify_entities() → verify_ids
│
├─ 3. 确定 verify_cols（排除锚点字段和日期字段）
│
├─ 4. 构建 verify_cols 的字段定义和压缩指南区块
│
├─ 5. 并行执行 _verify_one()（ThreadPoolExecutor, max_workers=4）
│   │
│   └─ _verify_one(rid)
│      │
│      ├─ 5.1 获取源段落和当前提取值
│      ├─ 5.2 构建 LLM Prompt
│      ├─ 5.3 调用 adapter.complete()
│      ├─ 5.4 解析 LLM 响应
│      │   ├─ 去掉代码块标记
│      │   ├─ 检查 ALL_CORRECT
│      │   ├─ 构建碰撞保护集（_anchor_vals, _typed_vals）
│      │   ├─ 截断推理泄漏（空行/##/** 截断）
│      │   ├─ 展开 pipe 分隔行
│      │   └─ _parse_correction() 解析每个字段
│      │       ├─ strip_reasoning_leak
│      │       ├─ NONE 标记 → ""（空字符串）
│      │       ├─ 数值字段类型校验
│      │       └─ 返回解析值或 None（跳过）
│      ├─ 5.5 值变化检查（parsed != current）
│      ├─ 5.6 NONE 保护（已有值时不覆盖）
│      ├─ 5.7 _is_cross_field_collision() 碰撞检测
│      └─ 5.8 返回 (rid, corrections)
│
├─ 6. 收集所有修正 all_corrections
│
└─ 7. 应用修正
    └─ 对每个 (rid, corrections)：
       └─ 对 by_pk[rid] 中所有 Record：
          └─ _apply_llm_value(table, record, col, val, "verify")
             ├─ sanitize_llm_value()
             ├─ clean_cell_for_type()
             └─ set_record_field()
```

---

## 10. 依赖的外部函数

### 10.1 `sanitize_llm_value()`（来自 `_record.py`）

```python
def sanitize_llm_value(val: str) -> str:
```

**功能：** LLM 文本进入 `Record.fields` 的唯一收口。

**处理步骤：**
1. `strip_reasoning_leak(val)` — 截断推理泄漏（如 "Source text says...", "然而，" 等模式）
2. 控制字符（`\x00-\x1f`, `\x7f`）折叠为空格
3. 连续空白折叠为单个空格
4. 截断到 `MAX_LLM_VALUE_CHARS`（500）字符

### 10.2 `clean_cell_for_type()`（来自 `_merge.py`）

```python
def clean_cell_for_type(
    val: str,
    field_type: str | None = None,
    field_name: str | None = None,
) -> str:
```

**功能：** 按字段类型清洗值。

**处理步骤：**
1. 剥离近似标记 `~`
2. `clean_cell()` — 占位符归一、单位后缀剥离
3. `strip_reasoning_leak()` — 二次截断
4. `_normalize_fund_type()` — 基金类型字段归一化（"混合型基金" → "混合型"）
5. 名称/缩写字段数字拦截（纯数字值 → 清空）
6. 按类型执行校验：
   - 数值/整数：`NUMERIC_SCALAR` 正则匹配
   - 日期：`DATE_SCALAR` 正则匹配
   - 布尔：`normalize_boolean()` 归一

### 10.3 `set_record_field()`（来自 `_record.py`）

```python
def set_record_field(record: Record, col: str, val: str, writer: str) -> None:
```

**功能：** 将清洗后的值写入 Record，同步更新 `approx` 和 `provenance`。

### 10.4 `metadata_by_name()`（来自 `_columns.py`）

```python
def metadata_by_name(
    names: Iterable[str],
    metadata: Mapping[str, T] | None,
) -> dict[str, T]:
```

**功能：** 将元数据字典按键名重新映射到目标拼写（大小写不敏感匹配）。

### 10.5 `strip_reasoning_leak()`（来自 `_merge.py`）

```python
def strip_reasoning_leak(val: str) -> str:
```

**功能：** 截断 LLM 推理泄漏文本。使用正则 `_REASONING_LEAK` 匹配中英文推理模式关键词，从匹配位置截断。

**英文模式关键词：** `Source text`, `However,`, `Wait,`, `Let me`, `Note:`, `The text/context/extraction/source`, `strictly following`, `implies the`, `this is/means/suggests`

**中文模式关键词：** `或根据`, `原文指代`, `原文是/写/说/提到`, `通常指`, `然而，`, `但仔细`, `但是，`, `如果源文本`, `这看起来`, `根据文档/上下文`, `仔细阅读`

---

## 11. 日志输出

Verify 阶段通过结构化日志输出关键状态：

| 日志级别 | 消息 | 触发条件 |
|---------|------|---------|
| INFO | `ETL field verify: checking N entities (M numeric fields each)` | 开始验证 |
| DEBUG | `ETL field verify failed for {rid}` | 单实体 LLM 调用失败 |
| WARNING | `ETL field verify: pool timed out` | 并发池整体超时 |
| WARNING | `ETL field verify: rejecting NONE correction for {col} (entity {rid} already has value {val!r})` | NONE 修正被拒绝 |
| WARNING | `ETL field verify: rejecting correction {col}={val!r} for entity {rid} (collides with anchor or cross-type field value)` | 交叉字段碰撞被拒绝 |
| INFO | `ETL field verify: all values confirmed correct` | 无修正 |
| INFO | `ETL field verify: corrected entity {rid}: {corrections}` | 单实体修正应用 |
| INFO | `ETL field verify: corrected N values across M entities` | 汇总 |

---

## 12. 复现指南

### 12.1 最小复现

```python
from agents.etl._verify import verify_field_values
from agents.etl._record import EntityTable, Record

# 构造 EntityTable
table = EntityTable(
    columns=["secucode", "secuabbr", "totalassets", "totalprofit"],
    primary_key="secucode",
    anchor_keys=["secucode"],
    field_types={"secucode": "string", "secuabbr": "string", "totalassets": "number", "totalprofit": "number"},
    records=[
        Record(
            pk="300",
            fields={"secucode": "300", "secuabbr": "XX基金", "totalassets": "5000000", "totalprofit": "23890000"},
        ),
    ],
    rejects=[],
)

# entity_groups: 主键 → 源段落列表
entity_groups = {
    "300": [
        "基金代码: 300，简称: XX基金，总资产: 500万，总利润: 2389万。",
    ],
}

# 调用验证
verify_field_values(
    adapter=llm_adapter,
    table=table,
    entity_groups=entity_groups,
    schema_field_defs={"totalprofit": "本期总利润（元）"},
    compress_guide="totalprofit → 总利润",
)
```

### 12.2 预期行为

1. 采样实体：`["300"]`（仅一个实体）
2. 验证字段：`["secuabbr", "totalassets", "totalprofit"]`（排除锚点 `secucode`）
3. LLM Prompt 包含当前提取值和源文本
4. LLM 返回 `ALL_CORRECT` → 无修正
5. 日志输出：`ETL field verify: all values confirmed correct`

### 12.3 错误修正场景

如果 LLM 返回 `totalprofit: 23890000`（值相同），则 `parsed == current`，不产生修正。

如果 LLM 返回 `totalprofit: 23890001`（值不同），则：
1. `_parse_correction("totalprofit", "23890001")` → `"23890001"`（数值类型，可解析）
2. `parsed != current` → True
3. `_is_cross_field_collision("totalprofit", "23890001")` → False（值不存在于其他字段）
4. 修正被接受，`_apply_llm_value()` 写入

### 12.4 碰撞保护场景

如果 LLM 错误地将 `totalassets` 的值修正在 `totalprofit` 上：

1. 当前 `totalassets = "5000000"` 存在于 `_typed_vals["number"]` 中
2. LLM 返回 `totalprofit: 5000000`
3. `_is_cross_field_collision("totalprofit", "5000000")` → True
4. 修正被拒绝，日志警告

---

## 13. 与其他阶段的关系

| 阶段 | 与 Verify 的关系 |
|------|-----------------|
| **Retry**（`retry_missing_values`） | 在 Verify 之前运行，回填空缺值。两者共享 `_apply_llm_value()` 写入路径 |
| **Dedup**（`dedup_cross_field_copies`） | 在 Verify 之前运行，清除跨字段重复值。减少 Verify 的误报可能性 |
| **Normalize**（`normalize_records`） | 在 Verify 之前运行，确保字段类型一致。Verify 依赖 `field_types` 做碰撞检测 |
| **Output**（`to_csv_text`） | 在 Verify 之后运行，将修正后的 `EntityTable` 序列化为 CSV |