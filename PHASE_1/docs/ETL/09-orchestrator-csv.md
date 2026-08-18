# Orchestrator & Phase 10: CSV 落盘

## 概述

Orchestrator（编排器）是整个 ETL 管线的入口和调度中心。它负责：
1. 检测需要 ETL 的散文文件
2. 编排每个文件的完整提取管线
3. 管理缓存和并发
4. 返回 ETLResult 供下游使用

Phase 10（CSV 落盘）是单文件提取管线的最后一步，将最终结果原子写入 CSV 文件。

**位置：** `extractor.py`（606 行），`_detect.py`，`_units.py`，`_pdf.py`，`_router.py`，`_threading.py`，`_trace.py`

---

## 完整管线流程

```
run_etl_for_task()  ─── 编排器
      │
      ├── detect_prose_files()  ─── 发现需要 ETL 的文件
      │
      ├── 读取 knowledge.md  ─── 用于 schema 解析和目标单位投票
      │
      ├── infer_proportion_conventions()  ─── 观察同目录比例约定
      │
      └── 并行处理每个文件 (ThreadPoolExecutor)
             │
             ├── 缓存检查 (validate_csv)  ─── 跳过已处理的文件
             │
             └── extract_prose_file()  ─── 单文件提取管线
                    │
                    ├── Phase 0: Schema Infer（如无 schema）
                    ├── Phase 1: Compress（LLM 压缩）
                    ├── Phase 1.5: Parse（KV 解析为内存表）
                    ├── Phase 2: Fix + Reconcile + Resolve ID
                    ├── Phase 3: Merge
                    ├── Phase 4: Normalize
                    ├── Phase 5: Dedup
                    ├── Phase 6: Retry
                    ├── Phase 7: Verify
                    ├── Phase 8: Identity Repair
                    ├── Phase 9: Synonym Unify
                    └── Phase 10: CSV 落盘
```

---

## 1. 编排器 — `run_etl_for_task()`

**位置：** `extractor.py` 第 413 行

**函数签名：**
```python
def run_etl_for_task(
    task: PublicTask,
    model_adapter: Any,
    *,
    selected_prose_files: list[Path] | None = None,
) -> list[ETLResult] | None
```

**参数：**
| 参数 | 类型 | 说明 |
|------|------|------|
| `task` | `PublicTask` | 当前任务，包含 context_dir 等信息 |
| `model_adapter` | `Any` | 主 agent 的模型适配器（用于构建 ETL 专用 adapter） |
| `selected_prose_files` | `list[Path] \| None` | 可选，指定只处理特定文件 |

**返回值：** `list[ETLResult] | None` — 每个成功转换的文件返回一个 ETLResult

**核心逻辑：**

```
Step 1: 检测散文文件
  - 如果 selected_prose_files 未指定，调用 detect_prose_files(task)
  - 否则使用指定的文件列表

Step 2: 读取 knowledge.md
  - 用于 schema 解析和目标单位投票

Step 3: 观察比例约定
  - infer_proportion_conventions(task.context_dir)
  - 查看同目录结构化数据的比例存储方式

Step 4: 构建 ETL 专用 ModelAdapter
  - _make_text_adapter(model_adapter)
  - 共享主 agent 的限速状态
  - temperature=0.1, disable thinking

Step 5: 并行处理每个文件
  - 单文件：直接调用
  - 多文件：ThreadPoolExecutor 并行
  - 超时保护：ETL_ORCHESTRATOR_LLM_CALL_TIMEOUT * max(4, len(prose_files))

Step 6: 返回 ETLResult 列表
```

**多文件并行处理：**
```python
def _run_one(pf: Path) -> ETLResult | None:
    try:
        result = _process(pf)  # 单个文件的完整管线
    except Exception as exc:
        logger.warning("ETL failed for %s; skipping file: %s", pf.name, exc)
        return None
    if result is None:
        return None
    return result

if len(prose_files) == 1:
    result = _run_one(prose_files[0])
    return [result] if result is not None else None

# 多文件并行
pool = ThreadPoolExecutor(max_workers=min(len(prose_files), ETL_ORCHESTRATOR_MAX_WORKERS))
futs = {submit_in_context(pool, _run_one, pf): pf for pf in prose_files}
for fut in as_completed(futs, timeout=orch_timeout):
    result = fut.result()
    if result is not None:
        results.append(result)
```

---

## 2. 单文件提取 — `extract_prose_file()`

**位置：** `extractor.py` 第 183 行

**函数签名：**
```python
def extract_prose_file(
    adapter: ModelAdapter,
    path: Path,
    task: PublicTask,
    prose_text: str,
    schema_columns: list[str] | None = None,
    anchor_keys: list[str] | None = None,
    schema_field_types: dict[str, str] | None = None,
    field_units: dict[str, str] | None = None,
    schema_field_defs: dict[str, str] | None = None,
    governance_columns: list[str] | None = None,
    proportion_conventions: dict[str, str] | None = None,
) -> ETLResult | None
```

**参数：**
| 参数 | 类型 | 说明 |
|------|------|------|
| `adapter` | `ModelAdapter` | ETL 专用 LLM 适配器 |
| `path` | `Path` | 散文文件路径 |
| `task` | `PublicTask` | 当前任务 |
| `prose_text` | `str` | 散文文本内容（PDF 已转 Markdown） |
| `schema_columns` | `list[str] \| None` | schema 字段列表 |
| `anchor_keys` | `list[str] \| None` | 锚点身份字段 |
| `schema_field_types` | `dict[str, str] \| None` | 字段类型映射 |
| `field_units` | `dict[str, str] \| None` | 字段单位映射 |
| `schema_field_defs` | `dict[str, str] \| None` | 字段定义映射 |
| `governance_columns` | `list[str] \| None` | governance 字段列表 |
| `proportion_conventions` | `dict[str, str] \| None` | 比例约定 |

**返回值：** `ETLResult | None` — 转换成功返回 ETLResult，失败返回 None

**完整管线调用：**

```python
def extract_prose_file(adapter, path, task, prose_text, ...):
    # ---- 缓存检查：如果 CSV 已存在且有效，直接返回 ----
    cached = validate_csv(csv_path)
    if cached:
        return ETLResult(source_file=path.name, csv_path=str(csv_path), ...)
    
    # ---- Phase 0: Schema Infer（如无 schema） ----
    if not schema_columns:
        inferred = infer_schema_from_sample(adapter, prose_text, ...)
        if inferred:
            schema_columns, anchor_keys, ... = inferred
    
    # ---- Phase 1: Compress ----
    compressed, entity_groups, compress_guide = compress_prose(
        adapter, prose_text, schema_columns=schema_columns, ...
    )
    
    # ---- Phase 1.5: Parse ----
    table = parse_kv_text(compressed, columns=schema_columns, ...)
    normalize_local_record_ids(table)
    
    # ---- Phase 2: Fix ----
    fix_malformed_lines(adapter, table, ...)
    # ---- Phase 2: Reconcile ----
    reconcile_field_names(adapter, table, ...)
    # ---- Phase 2: Resolve ID Conflicts ----
    if not primary_key:
        resolve_id_conflicts(table)
    
    # ---- Phase 3: Merge ----
    merge_records(table, source_text=prose_text)
    # ---- Phase 4: Normalize ----
    normalize_records(table)
    # ---- Phase 5: Dedup ----
    dedup_cleared = dedup_cross_field_copies(adapter, table, prose_text)
    # ---- Phase 6: Retry ----
    retry_missing_values(adapter, table, entity_groups, ..., forced_gaps=dedup_cleared)
    # ---- Phase 7: Verify ----
    verify_field_values(adapter, table, entity_groups, ..., compress_guide=compress_guide)
    
    # ---- 单位处理 ----
    if field_units:
        apply_proportion_convention(field_units, proportion_conventions)
        write_units_sidecar(task, path.stem, field_units)
    
    # ---- Phase 8: Identity Repair ----
    header, rows = records_to_rows(table)
    rows, repairs = repair_numeric_identities(header, rows)
    
    # ---- Phase 9: Synonym Unify ----
    if governance_columns:
        header, rows, applied = unify_table_synonyms(adapter, header, rows, ...)
    
    # ---- Phase 10: CSV 落盘 ----
    atomic_write_text(csv_path, to_csv_text(header, rows))
    
    # ---- 验证并返回 ----
    result = validate_csv(csv_path)
    return ETLResult(source_file=path.name, csv_path=str(csv_path), ...)
```

---

## 3. ETL 专用 ModelAdapter — `_make_text_adapter()`

**位置：** `extractor.py` 第 92 行

**函数签名：**
```python
def _make_text_adapter(model_adapter: Any) -> ModelAdapter
```

**说明：** 从主 agent 的 ModelAdapter 中提取凭证，构建一个纯文本（无 tools）的 ETL 专用适配器。

**关键特性：**
- `temperature=0.1` — 低温度保证提取一致性
- `max_tokens=ETL_MAX_OUTPUT_TOKENS` — 足够的输出 token
- `enable_thinking=None`（DeepSeek）或 `False`（其他）— 禁用思考模式
- 共享主 agent 的限速状态（`RateLimitedAdapter`）
- 共享主 agent 的追踪（`TracedModelAdapter`）
- 更小的 `reserve_output_tokens`（ETL 用更小的预扣）

---

## 4. ETLResult 数据结构

```python
@dataclass(frozen=True, slots=True)
class ETLResult:
    source_file: str    # 源文件名，如 "fund_notes.pdf"
    csv_path: str       # 生成的 CSV 文件绝对路径
    columns: list[str]  # CSV 列名
    row_count: int      # CSV 行数
```

**用途：** ETLResult 被下游的 `explore` 工具消费，作为 `etl_sources` 字段返回给 agent，告知 agent 哪些文档已被转换为 CSV 以及 CSV 的路径。

---

## 5. 散文文件检测 — `detect_prose_files()`

**位置：** `_detect.py` 第 44 行

**函数签名：**
```python
def detect_prose_files(task: PublicTask) -> list[Path]
```

**说明：** 搜索 `context/doc/` 和 `context/` 目录下的散文文件。

**搜索范围：**
- `task.context_dir / "doc"` 目录
- `task.context_dir` 根目录

**支持的文件类型：**
- `.md` — Markdown 文件（非空、UTF-8 编码）
- `.txt` — 文本文件（非空、UTF-8 编码）
- `.pdf` — PDF 文件（非空）

**排除规则：**
- `knowledge.md` 被排除（它是 schema 指导文件，不是 ETL 目标）
- 空文件被排除
- 非 UTF-8 编码的文件被排除
- 重复文件被排除（硬链接/符号链接去重）

---

## 6. 辅助模块

### PDF 转换 — `pdf_to_markdown()`

**位置：** `_pdf.py`

```python
def pdf_to_markdown(path: Path) -> str
```

**说明：** 将 PDF 文件转换为 Markdown 文本，使其可以被 compress 阶段处理。

### 线程安全 — `submit_in_context()`

**位置：** `_threading.py`

```python
def submit_in_context(pool: ThreadPoolExecutor, fn: Callable, *args, **kwargs) -> Future
```

**说明：** 安全的多线程上下文提交，确保每个线程都能正确访问 LLM 适配器。

### 单位处理

**位置：** `_units.py`

```python
def infer_proportion_conventions(context_dir: Path) -> dict[str, str]
# 观察同目录结构化数据的比例存储方式（如百分比用 0.0~1.0 还是 0~100）

def apply_proportion_convention(field_units: dict[str, str], conventions: dict[str, str] | None) -> None
# 应用比例约定到字段单位

def write_units_sidecar(task: PublicTask, stem: str, field_units: dict[str, str]) -> None
# 将单位信息写入 sidecar JSON 文件，供下游使用
```

### ETL 追踪

**位置：** `_trace.py`

```python
class ETLFileTrace:
    # 记录每个文件的 ETL 过程

def etl_phase(name: str, input_size: int = 0) -> Generator
# 上下文管理器，记录阶段的输入大小和耗时

def serialize_trace(trace: ETLFileTrace) -> dict
# 序列化为 JSON 供调试
```

### 文件路由

**位置：** `_router.py`

```python
def route_prose_files(task: PublicTask, prose_files: list[Path]) -> list[Path]
```

**说明：** 路由哪些文件需要 ETL 处理，基于文件大小和内容特征。

---

## 7. 缓存机制

**缓存文件路径：** `ETL_SCRATCH_ROOT / <task_id> / _etl / <stem>.csv`

**缓存检查：**
```python
# extract_prose_file 入口处
cached = validate_csv(csv_path)
if cached:
    header, row_count = cached
    return ETLResult(source_file=path.name, csv_path=str(csv_path), ...)

# run_etl_for_task 的 _process 内层
cached = validate_csv(csv_path)
if cached:
    return ETLResult(...)  # 跳过文件读取和所有 LLM 调用
```

**`validate_csv()` 函数：**
```python
def validate_csv(csv_path: Path) -> tuple[list[str], int] | None
# 验证 CSV 文件是否有效
# 返回 (header, row_count) 或 None
```

**缓存策略：**
- 两级缓存检查：编排器层和单文件层
- 缓存命中跳过文件读取和所有 LLM 调用
- 原子写入保证缓存文件一致性

---

## 8. 常量定义

| 常量 | 值 | 说明 |
|------|:---:|------|
| `ETL_SCRATCH_ROOT` | `/tmp/dabench` | ETL 临时文件根目录 |
| `ETL_HTTP_TIMEOUT` | 配置值 | HTTP 超时时间 |
| `ETL_MAX_OUTPUT_TOKENS` | 配置值 | ETL LLM 最大输出 token |
| `ETL_ORCHESTRATOR_LLM_CALL_TIMEOUT` | 配置值 | 编排器单次 LLM 调用超时 |
| `ETL_ORCHESTRATOR_MAX_WORKERS` | 配置值 | 编排器最大并行 worker 数 |
| `ETL_RESERVE_OUTPUT_TOKENS` | 配置值 | ETL 限速预留 token |
| `PROSE_EXTS` | `{".md", ".txt", ".pdf"}` | 散文文件扩展名 |

---

## 9. 完整时序图

```
run_etl_for_task(task, adapter)
  │
  ├── detect_prose_files(task) → [file1.md, file2.pdf, ...]
  │
  ├── 读取 knowledge.md → km_text
  │
  ├── infer_proportion_conventions(context_dir) → conventions
  │
  └── _make_text_adapter(adapter) → etl_adapter
       │
       └── ThreadPoolExecutor
             │
             ├── file1.md ──→ _process(file1.md)
             │                   │
             │                   ├── validate_csv(cache) → 命中？→ 返回
             │                   │
             │                   ├── PDF? → pdf_to_markdown()
             │                   │
             │                   ├── parse_knowledge_schema()
             │                   │
             │                   ├── merge_prose_schema_into_governance()
             │                   │
             │                   ├── infer_field_units_from_prose()
             │                   │
             │                   ├── vote_target_units()
             │                   │
             │                   └── extract_prose_file(...)
             │                         │
             │                         ├── [Phase 0] Schema Infer
             │                         ├── [Phase 1] Compress
             │                         ├── [Phase 1.5] Parse
             │                         ├── [Phase 2] Fix + Reconcile + ID
             │                         ├── [Phase 3] Merge
             │                         ├── [Phase 4] Normalize
             │                         ├── [Phase 5] Dedup
             │                         ├── [Phase 6] Retry
             │                         ├── [Phase 7] Verify
             │                         ├── [Phase 8] Identity Repair
             │                         ├── [Phase 9] Synonym Unify
             │                         └── [Phase 10] CSV 落盘
             │
             └── file2.pdf ──→ _process(file2.pdf)
                   (同上)
```

---

## 10. 复现关键点

1. **并发安全：** `submit_in_context()` 确保每个线程正确初始化 LLM 适配器
2. **缓存优先级：** 两级缓存检查优先于文件读取和 LLM 调用
3. **单文件容错：** 一个文件的 ETL 失败不影响其他文件
4. **超时保护：** 编排器设总超时，超时后取消未完成的 future
5. **原子写入：** `atomic_write_text()` 通过 tmp 文件 + `os.replace` 保证一致性
6. **单位 sidecar：** 单位信息写入 `_cache/<stem>_units.json`，供下游的 answer verifier 使用
7. **ETL 追踪：** 每个文件的 ETL 过程写入 `_cache/<stem>_trace.json`，供调试