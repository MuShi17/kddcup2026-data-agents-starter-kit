# ETL 预处理管线详解

> 来源：Mamba Agent (`Kosthi/kddcup2026-dataagents`)
> 核心模块：`src/agents/etl/` — 22 个 Python 文件，约 6000 行代码

---

## 一、为什么需要 ETL？

DABench 的很多任务包含**非结构化数据**（大段散文、Markdown 文档、PDF 文件），例如：

- 基金招募说明书（数百页 PDF，包含费率、投资范围、持仓明细等结构化信息）
- 病历记录（包含患者基本信息、诊断、用药、检查结果等）
- 公司简介/财务报告（包含财务数据、高管信息、业务描述等）
- 合同文档（包含条款、金额、日期、签署方等）

原始 ReAct agent 面对这些大段文本时，有几个根本性困难：

1. **上下文窗口限制** — 大文档无法全部放入 LLM 上下文
2. **信息提取困难** — 非结构化文本中定位关键字段值需要大量轮次
3. **一致性差** — 同一个文档的多次提取结果可能不一致
4. **轮次浪费** — agent 需要多次 `read_doc` / `preview_file` 来定位信息

**ETL 管线的目标：** 在 agent 循环开始之前，提前将非结构化文档转化为结构化 CSV，让 agent 可以直接用 SQL 查询。

---

## 二、整体管线流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          run_etl_for_task()                              │
│                          Orchestrator                                    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                ┌───────────────────┴───────────────────┐
                ▼                                       ▼
┌─────────────────────────────┐    ┌─────────────────────────────┐
│   detect_prose_files()      │    │   parse_knowledge_schema()   │
│   发现需要 ETL 的散文件       │    │   从 knowledge.md 解析 schema  │
└─────────────┬───────────────┘    └──────────────┬──────────────┘
              │                                   │
              └───────────┬───────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         extract_prose_file()                            │
│                         单文件提取管线                                    │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────┐    ┌───────────────────────┐    ┌───────────────────────┐
│ Phase 0:      │    │ Phase 1:              │    │ Phase 2:              │
│ Schema Infer  │───→│ Compress + Parse      │───→│ Serialize + Verify    │
│ 推断字段 schema │    │ LLM压缩 + 结构化解析    │    │ 确定性序列化 + 校验    │
└───────────────┘    └───────────────────────┘    └───────────────────────┘
```

---

## 三、管线各阶段详解

### 阶段 0：Schema 推断

**入口：** `extract_prose_file()` 第 230-243 行

**触发条件：** 当 `knowledge.md` 中没有提供该文件的 schema 信息时

**流程：**
1. 从文档中采样段落（`sample_paragraphs_by_section` — 按章节均匀采样）
2. 调用 LLM 推断可能的字段列表、字段类型、主键
3. 返回 `(schema_columns, anchor_keys, field_types, field_units, field_defs)`

**关键函数：** `infer_schema_from_sample()` 在 `_schema.py` 中

**代码位置：** `_schema.py` — 1258 行，是整个 ETL 管线最复杂的子模块之一

---

### 阶段 1：Compress（LLM 压缩）

**入口：** `compress_prose()` 在 `_compress.py` 第 787 行

这是整个 ETL 管线中最核心、最复杂的阶段。它将原始散文文本压缩为结构化的 key-value 行。

#### 1.1 实体感知分块（Entity-Aware Chunking）

**位置：** `_grouping.py`

```
原始散文 → 检测段落 → LLM 分组 → 按 token 预算分块
```

- 将段落按实体分组（同一实体的所有段落聚在一起）
- 分组后按 token 预算分块，确保每个实体的所有上下文在同一块中
- 好处：避免实体信息被切分到不同块导致提取不完整

#### 1.2 生成压缩指南（Compress Guide）

**位置：** `_generate_compress_guide()` 在 `_compress.py` 第 219 行

在处理实际数据之前，先用文档采样生成一份**提取指南**：

```
1. DOCUMENT LAYOUT — 文档结构描述
2. FIELD-TO-PROSE MAPPING — 每个字段对应的中文/英文模板
3. CRITICAL DISAMBIGUATION — 易混淆字段的区分规则
4. MULTIPLE NAME FORMS — 多名称形式的处理规则
5. FUZZY NUMBER MARKING — 模糊数字标记规则
```

这份指南作为后续压缩步骤的 system prompt，确保不同 chunk 的提取一致性。

#### 1.3 逐块压缩（Compress Chunk）

**位置：** `compress_chunk()` 在 `_compress.py` 第 474 行

对每个 chunk 调用 LLM，将散文段落转换为 `key: value | key: value` 格式：

```
输入段落：
  "患者张三，2023年5月10日入院，诊断为2型糖尿病，空腹血糖8.2mmol/L，
   身高175cm，体重82kg，BMI 26.8。"

输出：
  patient_name: 张三 | admission_date: 2023-05-10 | diagnosis: 2型糖尿病 |
  fasting_glucose: 8.2 | height_cm: 175 | weight_kg: 82 | bmi: 26.8
```

**压缩规则（prompt 中详细说明）：**
- 字段名必须使用 schema 中定义的名称，不能重命名/缩写
- 数字去掉单位后缀（`8.2mmol/L` → `8.2`）
- 百分比去掉 `%` 后缀（`2.0%` → `2.0`）
- 中文数字转阿拉伯数字（`两亿元` → `200000000`）
- 模糊数字加 `~` 前缀（`约六百九十七万` → `~6970000`）
- 日期统一为 `YYYY-MM-DD` 格式
- 保留 URL、名称、分类等文本值原样
- 源语言保持（中文源 → 中文值，不翻译）

#### 1.4 实体完整性校验

**位置：** `compress_prose()` 第 886-970 行

关键创新：每个 chunk 的输出必须包含该 chunk 中所有实体的记录。如果某个实体缺失，触发**定向恢复**：

```python
# 检查每个 chunk 的输出是否包含了所有期望的实体
expected = set(expected_rids)  # 分块前的实体 ID 列表
got = _extracted_pks(chunk_result)  # 实际提取到的实体 ID
missing = expected - got
if missing:
    # 对缺失实体执行定向恢复
    retry_text = entity_groups[missing_rid]
    result = compress_chunk(adapter, retry_text, ...)
```

#### 1.5 并行处理

所有 chunk 通过 `ThreadPoolExecutor` 并行处理，提高吞吐量：

```python
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
    futs = {submit_in_context(pool, compress_chunk, ...): idx for ...}
    for fut in as_completed(futs, timeout=LLM_CALL_TIMEOUT * len(work)):
        ...
```

---

### 阶段 1.5：Parse（KV 解析为内存表）

**入口：** `parse_kv_text()` 在 `_record.py` 第 143 行

压缩输出是纯文本的 `key: value | key: value` 格式。这一阶段**一次性解析**为内存中的 `EntityTable`，后续所有阶段直接操作内存表，不再需要文本解析。

**关键数据结构：**

```python
@dataclass(slots=True)
class Record:
    fields: dict[str, str]     # 字段名 → 值
    pk: str | None             # 主键值
    approx: set[str]           # 带 ~ 模糊标记的字段
    provenance: dict[str, str] # 字段 → 最后写入者（用于追踪）

@dataclass(slots=True)
class EntityTable:
    columns: list[str]
    primary_key: str | None
    anchor_keys: list[str]
    field_types: dict[str, str]
    records: list[Record]
    rejects: list[Reject]      # 解析失败的文本行
    conflicts: int             # 行内重复键冲突计数
```

**解析过程：**
1. 每行按 `|` 分割，每段按 `:` 分割为 key-value
2. key 通过 `canonical_key_map` 归一化到 schema 拼写
3. 未匹配 schema 的 key 保留原名（不丢弃）
4. 值中的 `~` 前缀剥离到 `Record.approx` 集合中
5. 行内重复键取首值，不一致时计入 `conflicts`
6. 缺主键的行 → `Reject("no_pk")`
7. 非 KV 格式的行 → `Reject("no_kv")`

---

### 阶段 2：修复与调和（Repair & Reconcile）

#### 2.1 格式修复（Fix Malformed Lines）

**位置：** `fix_malformed_lines()` 在 `_reconcile.py` 第 143 行

将解析失败的文本行（`rejects`）重新发送给 LLM 修复，最多 3 次重试：

```python
for attempt in range(1, FIX_FORMAT_MAX_RETRIES + 1):
    batch = table.rejects[:FIX_FORMAT_MAX_LINES]
    # LLM 重写格式错误的行
    response = adapter.complete(messages)
    # 解析修复后的行，成功则加入 records
    parsed = parse_kv_text(candidate, ...)
    if parsed.records:
        table.records.append(parsed.records)
```

#### 2.2 字段名调和（Reconcile Field Names）

**位置：** `reconcile_field_names()` 在 `_reconcile.py` 第 264 行

压缩过程中，不同 chunk 可能对同一字段使用不同名称（如 `patient_id` vs `uniquepid`）。LLM 将这些变体映射到 schema 定义的规范名称：

```python
unmatched = [...]  # 收集所有未匹配 schema 的字段名
prompt = "Match each unmatched field to a schema column or NONE"
response = adapter.complete(messages)
# 将匹配结果应用到所有记录
for src, dst in field_map.items():
    record.fields[dst] = record.fields.pop(src)
```

#### 2.3 ID 冲突解决（Resolve ID Conflicts）

**位置：** `resolve_id_conflicts()` 在 `_merge.py` 第 63 行

解决系统性 ID 偏移问题：当文档中同一实体在不同章节被分配了不同 ID 时，使用"首次出现"的 ID 作为基准，修正后续的偏移：

```python
# 构建规范名称→ID 映射（取首次出现）
name_to_first_id = {name: id for name, id in first_occurrence}
# 扫描所有行，修正偏移
if line's (ID, name) disagrees with canonical mapping:
    rewrite ID to canonical ID
```

---

### 阶段 3：合并（Merge）

**位置：** `merge_records()` 在 `_merge.py`

将同一实体的多条记录合并为一条。由于同一实体的信息可能分散在不同段落中，这一步至关重要：

```python
def merge_records(table: EntityTable, source_text: str) -> None:
    # 按主键分组
    by_pk = group_by_pk(table.records)
    for pk, records in by_pk.items():
        if len(records) == 1:
            continue
        # 合并字段：取非空值，冲突时取置信度高的
        merged = merge_record_fields(records, source_text)
        # 替换为合并后的记录
        ...
```

---

### 阶段 4：归一化（Normalize）

**位置：** `normalize_records()` 在 `_record.py` 第 241 行

对每个字段进行类型级清洗：

```python
for record in table.records:
    for col in record.fields:
        raw = record.fields[col]
        # 类型清洗（根据字段类型）
        cleaned = clean_cell_for_type(raw, field_type, field_name=col)
        # 写入清洗后的值
        set_record_field(record, col, cleaned, "normalize")
```

**类型清洗规则（`clean_cell_for_type` 在 `_merge.py`）：**
- 占位符检测（None/NaN/placeholder/redacted → 清空）
- 数字：去掉千位分隔符，去掉单位后缀
- 日期：归一化到 `YYYY-MM-DD`
- 布尔：归一化到 `true`/`false`
- 基金类型：去掉"基金"后缀（`混合型基金` → `混合型`）
- 名称/缩写字段：拦截纯数字值（可能是代码而非名称）

---

### 阶段 5：去重（Dedup）

**位置：** `dedup_cross_field_copies()` 在 `_verify.py` 第 132 行

当同一数值出现在两个不同字段中时（如 `reserveassets` 的值被错误复制到 `totalassets`），LLM 判断哪个字段正确，清除错误字段的值：

```python
# 找到所有跨字段重复的数值
dupes = [(pk, val, [col1, col2], context)]
# LLM 判断：BOTH（两字段都正确）或 <column_to_keep>
response = adapter.complete(prompt)
# 清除错误字段的值
for pk_val, cols in to_clear.items():
    for col in cols:
        set_record_field(record, col, "", "dedup")
```

---

### 阶段 6：空值重试（Retry Missing Values）

**位置：** `retry_missing_values()` 在 `_verify.py` 第 264 行

对于高填充率列（填充率 > 阈值）中的空值，用原始源文本定向重试提取：

```python
gaps = _detect_gap_cells(table)  # 找出高填充列中的空值
for rid in retry_ids:
    source_paras = entity_groups[rid]  # 该实体的原始段落
    prompt = f"Extract ONLY these fields from the text: {missing_cols}"
    response = adapter.complete(prompt)
    # 只回填仍为空的格
    for col, val in extracted.items():
        if record.fields[col].strip():
            continue  # 已经有值，跳过
        set_record_field(record, col, val, "retry")
```

---

### 阶段 7：值级验证（Verify Field Values）

**位置：** `verify_field_values()` 在 `_verify.py` 第 412 行

对提取结果进行最终验证，用 LLM 比对原始文本和提取值：

```python
for rid in verify_ids:
    prompt = f"""
    CURRENT EXTRACTION:
    field1: val1 | field2: val2 | ...
    
    SOURCE TEXT:
    {source_paras}
    
    For each field whose extracted value is WRONG, output:
      field_name: correct_value
    If all values are correct, output: ALL_CORRECT
    """
    response = adapter.complete(prompt)
    # 应用修正（有交叉字段碰撞保护）
    for col, val in corrections.items():
        if not _is_cross_field_collision(col, val):
            set_record_field(record, col, val, "verify")
```

**验证检查项：**
1. 值与源文本是否一致
2. 字段映射是否正确（如 `Reserve Assets` 的值没有放入 `Total Assets`）
3. 缩写字段是否取了最短形式
4. 数值字段是否包含纯数字（不是代码/ID）
5. 交叉字段碰撞保护（防止一个字段的值被错误复制到另一个字段）

---

### 阶段 8：身份修复（Identity Repair）

**位置：** `repair_numeric_identities()` 在 `_identity.py`

修复数值型 ID 的格式问题：

```python
rows, repairs = repair_numeric_identities(header, rows)
# 例如：001 → 1, 000123 → 123
```

---

### 阶段 9：同义词统一（Synonym Unify）

**位置：** `unify_table_synonyms()` 在 `_reconcile.py` 第 432 行

当提取过程中同一概念被分配到不同列名时，LLM 判断哪些列是精确同义词，然后确定性合并：

```python
# 1. 检测值分裂：规范列有缺失值，而发现列有值
# 2. LLM 判断：发现列是否是规范列的同义词
mapping = _adjudicate_synonym_columns(adapter, ...)
# 3. 确定性合并（保护身份列，行级冲突时否决）
apply_synonym_merges(header, rows, merges, protected=anchor_keys)
```

**合并守卫：**
- 碰撞守卫：两个发现列不能合并到同一个规范列
- 单位守卫：不同单位的列不能合并
- 行级冲突守卫：任何一行的值冲突都会否决整对合并

---

### 阶段 10：CSV 落盘

**位置：** `extract_prose_file()` 第 390-404 行

全管线唯一一次 CSV 写入，使用原子写入确保读者看不到半写状态：

```python
# 原子写入：tmp 文件 + os.replace
atomic_write_text(csv_path, to_csv_text(header, rows))

# 写入后立即验证
result = validate_csv(csv_path)
if not result:
    logger.warning("ETL deterministic extraction produced invalid CSV")
    return None
```

---

## 四、类图

```
┌─────────────────────────────────────────────────────────────────┐
│                        ETL 管线总览                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  run_etl_for_task()  ─── 编排器                                  │
│       │                                                         │
│       ├── detect_prose_files()  ─── 发现需要 ETL 的文件          │
│       │                                                         │
│       ├── parse_knowledge_schema()  ─── 从 knowledge.md 解析     │
│       │                                                         │
│       ├── merge_prose_schema_into_governance()  ─── schema 合并  │
│       │                                                         │
│       ├── infer_field_units_from_prose()  ─── 单位推断           │
│       │                                                         │
│       ├── vote_target_units()  ─── 目标单位投票                  │
│       │                                                         │
│       └── extract_prose_file()  ─── 单文件提取管线               │
│              │                                                  │
│              ├── Phase 0: Schema Infer （如无 schema）           │
│              │     └── infer_schema_from_sample()               │
│              │                                                  │
│              ├── Phase 1: Compress                              │
│              │     ├── group_paragraphs_by_entity()  ─── 分组    │
│              │     ├── entity_chunks_by_token_budget()  ── 分块  │
│              │     ├── _generate_compress_guide()  ── 生成指南   │
│              │     └── compress_chunk() × N  ── 并行压缩         │
│              │                                                  │
│              ├── Phase 1.5: Parse                               │
│              │     └── parse_kv_text()  ── 解析为内存表          │
│              │                                                  │
│              ├── Phase 2: Repair & Reconcile                    │
│              │     ├── fix_malformed_lines()  ── 格式修复        │
│              │     ├── reconcile_field_names()  ── 字段名调和    │
│              │     └── resolve_id_conflicts()  ── ID 冲突解决    │
│              │                                                  │
│              ├── Phase 3: Merge                                 │
│              │     └── merge_records()  ── 同类记录合并          │
│              │                                                  │
│              ├── Phase 4: Normalize                             │
│              │     └── normalize_records()  ── 类型归一化        │
│              │                                                  │
│              ├── Phase 5: Dedup                                 │
│              │     └── dedup_cross_field_copies()  ── 跨字段去重 │
│              │                                                  │
│              ├── Phase 6: Retry                                 │
│              │     └── retry_missing_values()  ── 空值重试       │
│              │                                                  │
│              ├── Phase 7: Verify                                │
│              │     └── verify_field_values()  ── 值级验证        │
│              │                                                  │
│              ├── Phase 8: Identity Repair                       │
│              │     └── repair_numeric_identities()  ── ID 修复  │
│              │                                                  │
│              ├── Phase 9: Synonym Unify                         │
│              │     └── unify_table_synonyms()  ── 同义词统一     │
│              │                                                  │
│              └── Phase 10: CSV 落盘                              │
│                    └── atomic_write_text()  ── 原子写入          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 五、关键设计哲学

### 5.1 一次解析，内存操作

**原则：** 压缩输出只解析一次（`parse_kv_text`），后续所有阶段直接操作内存表。避免每个阶段都要做文本解析/正则替换。

### 5.2 LLM 做判断，Python 做执行

**原则：** LLM 只负责需要语义理解的任务（判断字段名是否同义、判断值是否属于哪个字段），所有确定性操作（合并、去重、类型清洗、序列化）由 Python 代码执行。

### 5.3 原子写入

**原则：** CSV 文件通过 `tmp 文件 + os.replace` 原子写入，确保读者永远不会看到半写状态。

### 5.4 实体完整性守卫

**原则：** 压缩后验证每个 chunk 是否包含了所有预期实体，缺失实体触发定向恢复，避免实体被静默丢弃。

### 5.5 守恒监控

**原则：** 每个阶段前后记录 `(records, nonempty_cells)` 统计，记录数净减少时发出警告（`conservation violation`）。

### 5.6 缓存利用

**原则：** ETL 结果按文件缓存到 `_etl/<stem>.csv`。下次运行直接使用缓存，跳过 LLM 调用。

### 5.7 交叉字段碰撞保护

**原则：** LLM 的验证/修正建议必须经过碰撞检测：不能将值写入已存在值的 anchor 字段，不能将值写入不同类型的字段。

---

## 六、子模块索引

| 文件 | 职责 | 行数 |
|------|------|:----:|
| `extractor.py` | 编排器 + 单文件提取管线 | 606 |
| `_compress.py` | LLM 压缩 + 分块 + 压缩指南生成 | 980 |
| `_record.py` | 内存表数据结构 + KV 解析 + 序列化 | 383 |
| `_merge.py` | ID 冲突解决 + 记录合并 + 类型清洗 | 632 |
| `_reconcile.py` | 格式修复 + 字段名调和 + 同义词统一 | 637 |
| `_verify.py` | 去重 + 空值重试 + 值级验证 | 684 |
| `_schema.py` | Schema 推断 + knowledge.md 解析 + 合并 | 1258 |
| `_schema_kb.py` | Schema 知识库管理 | — |
| `_detect.py` | 散文文件检测 + 章节解析 | 77 |
| `_identity.py` | 数值型 ID 修复 | — |
| `_pdf.py` | PDF → Markdown 转换 | — |
| `_units.py` | 单位检测 + 转换 + 比例约定 | — |
| `_grouping.py` | 实体分组 + 按 token 预算分块 | — |
| `_columns.py` | 元数据按名称查找 | — |
| `_constants.py` | 常量定义 | — |
| `_types.py` | 类型工具函数 | — |
| `_threading.py` | 线程上下文提交 | — |
| `_trace.py` | ETL 追踪 | — |
| `_router.py` | 文件路由 | — |
| `_anchors.py` | 锚点检测 | — |
| `knowledge.py` | 知识库字段提取 | — |
| `__init__.py` | 模块导出 | 9 |

---

## 七、总结

**ETL 预处理管线是 Mamba Agent 最大的差异化优势。** 它将非结构化数据提取这一复杂任务分解为 10 个可管理阶段，LLM 只负责需要语义理解的判断（压缩、调和、验证），所有确定性操作由 Python 执行。这种"LLM 做判断，代码做执行"的架构保证了结果的可靠性和可复现性。

对于需要在 DABench 中处理大量非结构化文档的团队，这套 ETL 管线是最值得复用的核心组件。