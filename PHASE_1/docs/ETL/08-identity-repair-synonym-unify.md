# Phase 8 - Identity Repair（身份修复）与 Phase 9 - Synonym Unify（同义词统一）

## 1. 概述

Identity Repair（身份修复）和 Synonym Unify（同义词统一）是 Mamba Agent ETL 管线中的两个确定性修正阶段，位于压缩（Compression）和字段名调和（Field Name Reconciliation）之后，处于 CSV 表数据最终输出之前的清理阶段。

### 1.1 在 ETL 管线中的位置

```
压缩 → 字段名调和 → [Phase 8: Identity Repair] → [Phase 9: Synonym Unify] → CSV 输出
```

- **Phase 8 - Identity Repair**：修复压缩阶段留下的近似数值标记（`~6970000`）和数值型 ID 格式不一致问题，利用挖掘出的加法恒等式（additive identity）重新计算精确值。
- **Phase 9 - Synonym Unify**：检测并修复"值分裂"（value split）——同一概念的值被分配到两个不同的列名下（例如 canonical 列和 discovered 列），通过 LLM 裁决后确定性合并。

### 1.2 确定性特征

两阶段都是**确定性驱动**的阶段：
- **Identity Repair**：完全基于数学恒等式，零 LLM 调用。
- **Synonym Unify**：仅在一次 LLM 调用（`_adjudicate_synonym_columns`）中进行语义裁决，其余所有逻辑（可行性检查、碰撞守卫、单位守卫、冲突守卫、合并执行）都是确定性规则。

---

## 2. Identity Repair - `repair_numeric_identities()`

### 2.1 问题描述

压缩阶段（Compression）从源文本中提取数值时，遇到近似值（如"约六百九十七万"）会添加 `~` 前缀标记为 `~6970000`。此外，数值型 ID 可能存在格式不一致（如 `001` vs `1`，`000123` vs `123`）。Identity Repair 阶段的任务是：

1. 挖掘数据中隐含的加法恒等式关系（如 `总股本 = 流通股 + 非流通股`）
2. 利用这些恒等式将近似标记的数值替换为精确计算值
3. 修复格式不一致的数值型 ID

### 2.2 函数签名

```python
def repair_numeric_identities(
    header: list[str],
    rows: list[list[str]],
) -> tuple[list[list[str]], list[tuple[int, str, str, str]]]:
```

### 2.3 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `header` | `list[str]` | CSV 表头，每列的名称 |
| `rows` | `list[list[str]]` | CSV 数据行，每行是一个字符串列表，长度与 header 一致 |

### 2.4 返回值说明

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `rows` | `list[list[str]]` | 修复后的数据行（新副本，原输入不变） |
| `repairs` | `list[tuple[int, str, str, str]]` | 修复记录列表，每个元素为 `(行索引, 列名, 旧值, 新值)` |

### 2.5 核心逻辑步骤

#### 步骤 1：挖掘加法恒等式

调用 `mine_additive_identities(header, rows)` 找出所有满足 `col_a == col_b + col_c` 的三元组 `(a, b, c)`。

#### 步骤 2：建立按列索引的恒等式索引

```python
by_col: dict[int, list[tuple[int, int, int]]] = {}
for a, b, c in identities:
    by_col.setdefault(a, []).append((a, b, c))
    by_col.setdefault(b, []).append((a, b, c))
    by_col.setdefault(c, []).append((a, b, c))
```

#### 步骤 3：三种修复触发条件

对每一行，依次检查三种修复场景：

**场景 A — 近似标记值（Approx-tagged）**：单元格值带有 `~` 前缀标记。对于每个这样的单元格，遍历所有涉及该列的恒等式，用其他两个非近似值解出目标值。如果解出的值通过粒度一致性检查（`_coarse_consistent`）或数值接近检查（`_numbers_close`），则加入提议列表。

**场景 B — 缺失值填充**：如果某行中恒等式的三个列恰好有一个为空，且另外两个列的值都是非近似的精确值，则从恒等式解出缺失值。

**场景 C — 违规行修复**：如果某行三个列都有非近似值但违反了恒等式（`va != vb + vc`），检查是否有且仅有一个"可归责"（blamable）的项（通过 `_blamable_term` 判断），如果是，则用恒等式解出的值替换该项。

#### 步骤 4：共识检查

对所有提议值进行去重合并（`_numbers_close` 容差内视为相同值），只有当所有相关的恒等式都解出**同一个**值（distinct 列表长度 == 1）时，才执行替换。如果解出的值与原始值相同（数值接近），则跳过。

#### 步骤 5：剥离近似标记

无论是否修复，所有 `~` 前缀标记都被剥离。

### 2.6 关键代码片段

```python
repairs: list[tuple[int, str, str, str]] = []
for i, row in enumerate(rows):
    proposals: dict[int, list[float]] = {}

    # 场景 A：近似标记值
    for j in range(len(header)):
        raw = row[j].strip()
        num, approx = _parse_cell_number(raw)
        if num is None or not approx:
            continue
        for identity in by_col.get(j, []):
            solved = _solve(identity, j, row)
            if solved is None:
                continue
            if _coarse_consistent(num, raw, solved) or _numbers_close(solved, num):
                proposals.setdefault(j, []).append(solved)

    # 场景 B：缺失值
    for identity in identities:
        a, b, c = identity
        empty_cols = []
        cells = []
        unusable = False
        for j in (a, b, c):
            raw = row[j].strip()
            if not raw:
                empty_cols.append(j)
                continue
            num, approx = _parse_cell_number(raw)
            if num is None or approx:
                unusable = True
                break
            cells.append((j, num, approx))
        if unusable:
            continue
        if len(empty_cols) == 1:
            solved_missing = _solve(identity, empty_cols[0], row)
            if solved_missing is not None:
                proposals.setdefault(empty_cols[0], []).append(solved_missing)
            continue
        if empty_cols:
            continue

        # 场景 C：违规行
        va, vb, vc = cells[0][1], cells[1][1], cells[2][1]
        if _numbers_close(va, vb + vc):
            continue
        grains = identity_grains[identity]
        blamable = [
            (j, solved)
            for j, stated, solved in ((a, va, vb + vc), (b, vb, va - vc), (c, vc, va - vb))
            if _blamable_term(stated, row[j].strip(), solved, grains[j])
        ]
        if len(blamable) == 1:
            j, solved = blamable[0]
            proposals.setdefault(j, []).append(solved)

    # 共识检查
    for j, solved_values in proposals.items():
        distinct = []
        for value in solved_values:
            if not any(_numbers_close(value, seen) for seen in distinct):
                distinct.append(value)
        if len(distinct) != 1:
            continue
        old = row[j].strip()
        stated, _approx = _parse_cell_number(old)
        if stated is not None and _numbers_close(distinct[0], stated):
            continue
        new = _format_repaired_number(distinct[0])
        row[j] = new
        repairs.append((i, header[j], old, new))

# 剥离所有近似标记
for row in rows:
    for j in range(len(header)):
        raw = row[j].strip()
        num, approx = _parse_cell_number(raw)
        if num is not None and approx:
            bare, _ = split_approx_tag(raw)
            row[j] = bare
```

---

## 3. 辅助函数（Identity Repair）

### 3.1 `mine_additive_identities()`

**函数签名：**
```python
def mine_additive_identities(
    header: list[str],
    rows: list[list[str]],
) -> list[tuple[int, int, int]]:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `header` | `list[str]` | CSV 表头 |
| `rows` | `list[list[str]]` | CSV 数据行 |

**返回值：** `list[tuple[int, int, int]]` — 满足 `col_a == col_b + col_c` 的三元组列索引列表，其中 `b < c`。

**核心逻辑：**

1. 调用 `_parse_numeric_table()` 解析所有列为数值列或非数值列
2. 枚举所有数值列的三元组 `(a, b, c)`，其中 `b < c` 且 `a != b != c`
3. 对每个三元组调用 `_scan_identity()` 检查是否满足恒等式
4. 接受条件：
   - 无硬性违规（`hard_violation == False`）
   - 支持行数 >= `MIN_IDENTITY_SUPPORT`（5 行）
   - 支持行数 > 可疑行数

### 3.2 `_parse_numeric_table()`

**函数签名：**
```python
def _parse_numeric_table(
    header: list[str],
    rows: list[list[str]],
) -> list[list[tuple[float | None, bool]] | None]:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `header` | `list[str]` | CSV 表头 |
| `rows` | `list[list[str]]` | CSV 数据行 |

**返回值：** `list[list[tuple[float | None, bool]] | None]` — 每列一个元素。如果列为数值列，则值为 `[(value, is_approx), ...]` 列表；否则为 `None`。

**核心逻辑：**

- 遍历每一列，对每一行调用 `_parse_cell_number()` 解析数值
- 一个列被判定为数值列的条件：**所有非空单元格都能解析为数值**（允许近似标记），且**至少有一个非空单元格**
- 如果某列中有任何非空单元格无法解析为浮点数，则整列标记为非数值列

### 3.3 `_parse_cell_number()`

**函数签名：**
```python
def _parse_cell_number(raw: str) -> tuple[float | None, bool]:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `raw` | `str` | 原始单元格字符串 |

**返回值：** `(float | None, bool)` — 解析后的数值（或 `None`）和是否为近似标记值。

**核心逻辑：**

1. 调用 `split_approx_tag(raw)` 分离近似标记（`~` 或 `～`）
2. 如果剥离后为空，返回 `(None, False)`
3. 尝试将剥离后的字符串（去除逗号）转换为 `float`
4. 转换失败则返回 `(None, False)`

### 3.4 `_scan_identity()`

**函数签名：**
```python
def _scan_identity(
    parsed: list[list[tuple[float | None, bool]] | None],
    rows: list[list[str]],
    triple: tuple[int, int, int],
) -> tuple[int, int, bool, dict[int, list[float]]]:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `parsed` | `list[list[tuple[float \| None, bool]] \| None]` | 解析后的数值表 |
| `rows` | `list[list[str]]` | 原始 CSV 数据行 |
| `triple` | `tuple[int, int, int]` | `(a, b, c)` 列索引三元组 |

**返回值：** `(support, suspects, hard_violation, support_grains)`

| 字段 | 类型 | 说明 |
|------|------|------|
| `support` | `int` | 支持行数（三个值都精确且满足 `va == vb + vc`） |
| `suspects` | `int` | 可疑行数（违规行中至少有一个可归责项） |
| `hard_violation` | `bool` | 是否存在硬性违规（违规行中没有可归责项） |
| `support_grains` | `dict[int, list[float]]` | 每列在支持行中的陈述粒度列表 |

**核心逻辑：**

**Pass 1 — 分类：** 遍历所有行，只考虑三个值都非空且非近似的行。如果 `_numbers_close(va, vb + vc)` 则加入支持者列表，否则加入违规者列表。

**Pass 2 — 违规行分析：** 对每个违规行，检查三个项中是否有可归责的（通过 `_blamable_term` 判断）：
- 如果没有可归责项 → `hard_violation = True`，立即返回
- 如果有可归责项 → `suspects += 1`

### 3.5 `_numbers_close()`

**函数签名：**
```python
def _numbers_close(a: float, b: float) -> bool:
```

使用 `math.isclose(a, b, rel_tol=NUMERIC_EQ_REL, abs_tol=NUMERIC_EQ_ABS)` 判断两个浮点数是否接近。常量值：`NUMERIC_EQ_REL = 1e-9`，`NUMERIC_EQ_ABS = 1e-9`。

### 3.6 `_stated_granularity()`

**函数签名：**
```python
def _stated_granularity(raw: str) -> float:
```

**核心逻辑：** 计算一个数值的陈述粒度。例如 `6970000`（六百九十七万）的粒度为 `1e4`（万）。如果数值没有尾随零、包含小数点或科学计数法，则返回 `0.0`（表示完全精度，禁用粗粒度匹配）。

### 3.7 `_coarse_consistent()`

**函数签名：**
```python
def _coarse_consistent(stated: float, raw: str, solved: float) -> bool:
```

**核心逻辑：** 判断恒等式解出的值 `solved` 在原始值的粒度下是否一致。条件：
1. `_stated_granularity(raw) > 1.0`（存在粗粒度）
2. `solved` 和 `stated` 不接近（`_numbers_close` 返回 False）
3. `round(solved / granularity) == round(stated / granularity)`（在粒度上四舍五入后相等）

### 3.8 `_blamable_term()`

**函数签名：**
```python
def _blamable_term(stated: float, raw: str, solved: float, support_grains: list[float]) -> bool:
```

**核心逻辑：** 判断一个未标记的违规项是否可以吸收该行的差异。两个独立条件，都需要满足：
1. **粗粒度一致性**（`_coarse_consistent`）：恒等式解出的值在陈述粒度上一致
2. **粒度异常**：该单元格的粒度严格大于该列中所有支持行的粒度。如果一个列中取整数值是常态，则取整不携带近似信号；如果该列通常是全精度，则一个唯一取整的数值正是模糊数值的签名

### 3.9 `_format_repaired_number()`

**函数签名：**
```python
def _format_repaired_number(value: float) -> str:
```

**核心逻辑：** 如果 `value` 接近整数（`math.isclose(value, round(value), rel_tol=0, abs_tol=NUMERIC_EQ_ABS)`），返回整数形式（`str(round(value))`）；否则返回 `format(value, ".12g")` 保留最多 12 位有效数字。

---

## 4. Synonym Unify - `unify_table_synonyms()`

### 4.1 问题描述

"值分裂"（Value Split）是指：治理规约（governance）定义了一个 canonical 列，但压缩提取时将同一概念的值分配到了另一个不同的 discovered 列名下，导致 canonical 列有大量空值而 discovered 列有值。例如：

```
| 总股本(万元) | total_equity_wan |
|-------------|-----------------|
|             | 6970000         |
| 5000000     |                 |
|             | 3200000         |
```

这里 `total_equity_wan` 是 discovered 列，`总股本(万元)` 是 canonical 列，它们实际上是同一个概念。

### 4.2 函数签名

```python
def unify_table_synonyms(
    adapter: ModelAdapter,
    header: list[str],
    rows: list[list[str]],
    prose_stem: str,
    governance_columns: list[str],
    anchor_keys: list[str],
    field_defs: dict[str, str],
    question: str,
    units_path: Path | None,
) -> tuple[list[str], list[list[str]], list[tuple[str, str]]]:
```

### 4.3 参数说明

| 参数 | 类型 | 说明 |
|------|------|------|
| `adapter` | `ModelAdapter` | LLM 模型适配器，用于同义词裁决 |
| `header` | `list[str]` | CSV 表头 |
| `rows` | `list[list[str]]` | CSV 数据行 |
| `prose_stem` | `str` | 源文件名（不含扩展名），用于日志和 LLM 提示 |
| `governance_columns` | `list[str]` | 治理规约定义的 canonical 列名列表 |
| `anchor_keys` | `list[str]` | 锚定键（标识列），受保护不被合并 |
| `field_defs` | `dict[str, str]` | 字段名 → 定义描述 的映射 |
| `question` | `str` | 任务问题（上下文信息） |
| `units_path` | `Path \| None` | 单位元数据 JSON 文件路径 |

### 4.4 返回值说明

| 返回值 | 类型 | 说明 |
|--------|------|------|
| `header` | `list[str]` | 合并后的表头 |
| `rows` | `list[list[str]]` | 合并后的数据行 |
| `applied` | `list[tuple[str, str]]` | 实际执行的合并列表 `(source, target)`，没有合并时为 `[]` |

### 4.5 完整流程

#### 阶段 1：大小写合并（确定性）

调用 `_case_only_header_merges()` 检测表头中仅因大小写不同的列名，执行确定性合并。

```python
case_merges = _case_only_header_merges(current_header, governance_columns)
if case_merges:
    current_header, current_rows, case_applied = apply_synonym_merges(
        current_header, current_rows, case_merges, protected=set(anchor_keys),
    )
    pending_applied.extend(case_applied)
```

#### 阶段 2：检测值分裂

计算每列的填充率，找出两类列：

- **Gapped Canonicals**：属于 governance 列、不是 anchor 列、填充数 < 总行数
- **Discovered Cols**：不属于 governance 列、不是 anchor 列、填充数 > 0

```python
gapped_canonicals = [
    col for col in current_header
    if col.lower() in gov_lower and col.lower() not in anchor_lower and fill[col] < n_rows
]
discovered_cols = [
    col for col in current_header
    if col.lower() not in gov_lower and col.lower() not in anchor_lower and fill[col] > 0
]
```

如果两者都非空，则进入下一步。

#### 阶段 3：行级可行性检查

对每个 (canonical, discovered) 对，调用 `_pair_viable()` 检查：

```python
def _pair_viable(canon: str, disc: str) -> bool:
    """零冲突 AND（贡献新值 OR 完全冗余重叠）。"""
    ci, di = col_idx[canon], col_idx[disc]
    contributes = 0
    overlap = 0
    disc_nonempty = 0
    for row in current_rows:
        src, dst = row[di].strip(), row[ci].strip()
        if not src:
            continue
        disc_nonempty += 1
        if not dst:
            contributes += 1
        elif cells_agree(src, dst):
            overlap += 1
        else:
            return False  # 冲突，不可合并
    return disc_nonempty > 0 and (contributes > 0 or overlap == disc_nonempty)
```

#### 阶段 4：LLM 同义词裁决

对至少有一个可行 discovered 列的 canonical 列，调用 `_adjudicate_synonym_columns()` 让 LLM 判断哪些 discovered 列是 canonical 列的同义词。

```python
viable_canonicals = [
    canon for canon in gapped_canonicals
    if any(_pair_viable(canon, disc) for disc in discovered_cols)
]
mapping = _adjudicate_synonym_columns(
    adapter, prose_stem,
    [(col, field_defs.get(col, ""), f"{fill[col]}/{n_rows} rows filled") for col in viable_canonicals],
    [(col, field_defs.get(col, ""), f"{fill[col]}/{n_rows} rows filled", samples[col]) for col in discovered_cols],
    question,
)
```

#### 阶段 5：碰撞守卫

两个 discovered 列不能合并到同一个 canonical 列。如果 LLM 将多个 discovered 列映射到同一个 canonical，则全部跳过（不信任分歧裁决）。

```python
by_canon: dict[str, list[str]] = {}
for disc, canon in mapping.items():
    by_canon.setdefault(canon, []).append(disc)
merges: list[tuple[str, str]] = []
for canon, discs in sorted(by_canon.items()):
    if len(discs) > 1:
        logger.warning(...)
        continue  # 跳过碰撞
    merges.append((discs[0], canon))
```

#### 阶段 6：单位守卫

如果 discovered 列和 canonical 列的单位元数据都存在且不同，则不能合并。

```python
for src, dst in merges:
    src_unit, dst_unit = _unit_meta_get(units_meta, src), _unit_meta_get(units_meta, dst)
    if src_unit and dst_unit and str(src_unit) != str(dst_unit):
        continue  # 跳过单位不匹配
    guarded.append((src, dst))
```

#### 阶段 7：行级冲突守卫

调用 `apply_synonym_merges()` 执行合并时，对每一对（src, dst）逐行检查冲突：如果任何一行中两个单元格都非空且不相等，则跳过该对合并。

#### 阶段 8：单位元数据迁移

合并成功后，调用 `_move_unit_meta()` 将单位元数据从源列迁移到目标列。

### 4.6 关键代码片段

```python
def unify_table_synonyms(
    adapter: ModelAdapter,
    header: list[str],
    rows: list[list[str]],
    prose_stem: str,
    governance_columns: list[str],
    anchor_keys: list[str],
    field_defs: dict[str, str],
    question: str,
    units_path: Path | None,
) -> tuple[list[str], list[list[str]], list[tuple[str, str]]]:
    if not header or not rows:
        return header, rows, []
    current_header = list(header)
    width = len(current_header)
    current_rows = [
        row + [""] * (width - len(row)) if len(row) < width else row[:width]
        for row in rows
    ]
    pending_applied: list[tuple[str, str]] = []

    # 阶段 1: 大小写合并
    case_merges = _case_only_header_merges(current_header, governance_columns)
    if case_merges:
        current_header, current_rows, case_applied = apply_synonym_merges(
            current_header, current_rows, case_merges, protected=set(anchor_keys),
        )
        pending_applied.extend(case_applied)

    # 阶段 2: 检测值分裂
    fill = column_fill_counts(current_header, current_rows)
    n_rows = len(current_rows)
    gov_lower = {g.lower() for g in governance_columns}
    anchor_lower = {a.lower() for a in anchor_keys}
    gapped_canonicals = [
        col for col in current_header
        if col.lower() in gov_lower and col.lower() not in anchor_lower and fill[col] < n_rows
    ]
    discovered_cols = [
        col for col in current_header
        if col.lower() not in gov_lower and col.lower() not in anchor_lower and fill[col] > 0
    ]
    if not gapped_canonicals or not discovered_cols:
        return _commit(pending_applied)

    col_idx = {col: current_header.index(col) for col in current_header}

    # 阶段 3: 行级可行性检查
    viable_canonicals = [
        canon for canon in gapped_canonicals
        if any(_pair_viable(canon, disc) for disc in discovered_cols)
    ]
    if not viable_canonicals:
        return _commit(pending_applied)

    # 阶段 4: LLM 同义词裁决
    mapping = _adjudicate_synonym_columns(...)
    if not mapping:
        return _commit(pending_applied)
    mapping = {disc: canon for disc, canon in mapping.items() if _pair_viable(canon, disc)}

    # 阶段 5: 碰撞守卫
    # 阶段 6: 单位守卫
    # 阶段 7: 行级冲突守卫（在 apply_synonym_merges 内部）
    new_header, new_rows, applied = apply_synonym_merges(
        current_header, current_rows, guarded, protected=set(anchor_keys),
    )
    if not applied:
        return _commit(pending_applied)

    current_header, current_rows = new_header, new_rows
    pending_applied.extend(applied)
    return _commit(pending_applied)
```

---

## 5. 辅助函数（Synonym Unify）

### 5.1 `_adjudicate_synonym_columns()`

**函数签名：**
```python
def _adjudicate_synonym_columns(
    adapter: ModelAdapter,
    prose_stem: str,
    canonicals: list[tuple[str, str, str]],
    discovered: list[tuple[str, str, str, list[str]]],
    question: str,
) -> dict[str, str]:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `adapter` | `ModelAdapter` | LLM 模型适配器 |
| `prose_stem` | `str` | 源文件名（不含扩展名） |
| `canonicals` | `list[tuple[str, str, str]]` | Canonical 列列表，每个元素为 `(列名, 定义描述, 填充说明)` |
| `discovered` | `list[tuple[str, str, str, list[str]]]` | Discovered 列列表，每个元素为 `(列名, 定义描述, 填充说明, 示例值列表)` |
| `question` | `str` | 任务问题 |

**返回值：** `dict[str, str]` — `{discovered列名: canonical列名}` 映射。

**核心逻辑：**

1. 构造包含 canonical 列和 discovered 列信息（名称、定义、填充率、示例值）的 LLM 提示
2. 要求 LLM 判断每个 discovered 列是否与某个 canonical 列是**完全相同的概念**（same concept AND every binding qualifier: entity scope, before/after state, time window, numerator/denominator, unit family, aggregation level）
3. 完全匹配 → `discovered=canonical`；不匹配或不确定 → `discovered=DISTINCT`
4. 每个 canonical 列最多吸收一个 discovered 列
5. 调用 `_parse_alias_response()` 解析 LLM 输出

### 5.2 `_parse_alias_response()`

**函数签名：**
```python
def _parse_alias_response(
    text: str,
    discovered: list[str],
    canonicals: list[str],
) -> dict[str, str]:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `text` | `str` | LLM 输出的原始文本 |
| `discovered` | `list[str]` | 所有 discovered 列名 |
| `canonicals` | `list[str]` | 所有 canonical 列名 |

**返回值：** `dict[str, str]` — `{discovered列名: canonical列名}` 映射。

**核心逻辑：**

1. 构建 `canon_by_lower` 和 `disc_by_lower`（不区分大小写的查找字典）
2. 按逗号、分号、换行符拆分文本
3. 对每个 `=` 格式的条目：
   - 提取 `=` 左侧（容许 `ALIASES:` 前缀）
   - 通过不区分大小写匹配找到实际的 discovered 列名
   - 如果 `=` 右侧是 `DISTINCT`，跳过
   - 通过不区分大小写匹配找到实际的 canonical 列名
   - 如果左右都匹配，加入映射

### 5.3 `_case_only_header_merges()`

**函数签名：**
```python
def _case_only_header_merges(
    header: list[str],
    preferred_columns: list[str],
) -> list[tuple[str, str]]:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `header` | `list[str]` | CSV 表头 |
| `preferred_columns` | `list[str]` | 优先使用的列名列表（通常为 governance 列名） |

**返回值：** `list[tuple[str, str]]` — 需要合并的列对列表 `(源列, 目标列)`。

**核心逻辑：**

1. 按不区分大小写分组表头列
2. 对每组只有一个列名的，跳过
3. 对每组有多个列名的，确定目标列：
   - 如果 preferred_columns 中有该不区分大小写形式且实际存在，使用它
   - 否则使用该组中第一个出现的列名
4. 其他列名都合并到目标列

### 5.4 `_pair_viable()`

**函数签名：**
```python
def _pair_viable(canon: str, disc: str) -> bool:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `canon` | `str` | Canonical 列名 |
| `disc` | `str` | Discovered 列名 |

**返回值：** `bool` — 是否可以合并。

**核心逻辑：**

对每一行，检查 discovered 列非空单元格与 canonical 列对应单元格的关系：
- 如果 canonical 列为空 → `contributes += 1`（贡献新值）
- 如果 canonical 列非空且 `cells_agree(src, dst)` → `overlap += 1`（重叠一致）
- 如果 canonical 列非空且不相等 → 返回 `False`（冲突）

通过条件：`disc_nonempty > 0 AND (contributes > 0 OR overlap == disc_nonempty)`

### 5.5 `_unit_meta_get()`

**函数签名：**
```python
def _unit_meta_get(units_meta: dict[str, Any], field: str) -> Any:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `units_meta` | `dict[str, Any]` | 单位元数据字典 |
| `field` | `str` | 字段名 |

**返回值：** `Any` — 不区分大小写匹配的单位元数据值，未找到返回 `None`。

### 5.6 `_move_unit_meta()`

**函数签名：**
```python
def _move_unit_meta(units_meta: dict[str, Any], src: str, dst: str) -> bool:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `units_meta` | `dict[str, Any]` | 单位元数据字典（可变） |
| `src` | `str` | 源列名 |
| `dst` | `str` | 目标列名 |

**返回值：** `bool` — 是否发生了任何修改。

**核心逻辑：**

对三个前缀（`""`、`_target_`、`_factor_`）分别处理：
1. 不区分大小写查找源列的单位元数据键
2. 如果源键和目标键不区分大小写相同但实际拼写不同，且目标键不存在，则重命名键
3. 否则将源键的值复制到目标键（不覆盖已有值），然后删除源键

### 5.7 `apply_synonym_merges()`

**函数签名：**
```python
def apply_synonym_merges(
    header: list[str],
    rows: list[list[str]],
    merges: list[tuple[str, str]],
    protected: set[str] | None = None,
) -> tuple[list[str], list[list[str]], list[tuple[str, str]]]:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `header` | `list[str]` | CSV 表头 |
| `rows` | `list[list[str]]` | CSV 数据行 |
| `merges` | `list[tuple[str, str]]` | 待执行合并的 `(source, target)` 对列表 |
| `protected` | `set[str] \| None` | 受保护的列名（标识列），不会被合并（大小写合并除外） |

**返回值：** `(header, rows, applied)` — 新副本；`applied` 是实际执行的合并列表。

**核心逻辑：**

1. 按源列填充率降序、名称升序排序合并对（确定性顺序）
2. 对每对 `(src, dst)`：
   - **大小写排他**：如果 `src.lower() == dst.lower()`，允许合并（大小写合并）
   - **保护守卫**：非大小写合并时，如果 src 或 dst 在 protected 中，跳过
   - **存在性检查**：src 和 dst 必须在 header 中
   - **空源检查**：如果 src 列全为空，跳过
   - **行级冲突检查**：逐行检查，如果两个单元格都非空且 `cells_agree()` 返回 False，该对有冲突，跳过
   - **合并执行**：对每行，如果 src 非空且 dst 为空，将 src 值复制到 dst；然后删除 src 列

```python
def apply_synonym_merges(header, rows, merges, protected=None):
    protected_lower = {p.lower() for p in (protected or set())}
    header = list(header)
    rows = [list(row) for row in rows]
    fill = column_fill_counts(header, rows)

    applied = []
    for src, dst in sorted(merges, key=lambda m: (-fill.get(m[0], 0), m[0])):
        case_only = src.lower() == dst.lower()
        if src == dst or (not case_only and (src.lower() in protected_lower or dst.lower() in protected_lower)):
            continue
        if src not in header or dst not in header:
            continue
        si, di = header.index(src), header.index(dst)
        if not any(row[si].strip() for row in rows):
            continue
        conflicts = sum(
            1 for row in rows
            if row[si].strip() and row[di].strip() and not cells_agree(row[si].strip(), row[di].strip())
        )
        if conflicts:
            continue
        for row in rows:
            if row[si].strip() and not row[di].strip():
                row[di] = row[si]
        header.pop(si)
        for row in rows:
            row.pop(si)
        applied.append((src, dst))
    return header, rows, applied
```

### 5.8 `column_fill_counts()`

**函数签名：**
```python
def column_fill_counts(header: list[str], rows: list[list[str]]) -> dict[str, int]:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `header` | `list[str]` | CSV 表头 |
| `rows` | `list[list[str]]` | CSV 数据行 |

**返回值：** `dict[str, int]` — `{列名: 非空单元格数}`。

**核心逻辑：** 遍历所有行，对每列统计 `val.strip()` 非空的单元格数量。

### 5.9 `cells_agree()`

**函数签名：**
```python
def cells_agree(a: str, b: str) -> bool:
```

**参数说明：**

| 参数 | 类型 | 说明 |
|------|------|------|
| `a` | `str` | 第一个单元格值 |
| `b` | `str` | 第二个单元格值 |

**返回值：** `bool` — 两个单元格是否一致。

**核心逻辑：**

1. 如果 `a == b`（字符串相等），返回 `True`
2. 尝试将两者解析为 `float`，如果 `float(a) == float(b)`，返回 `True`
3. 否则返回 `False`

---

## 6. 常量定义

### 6.1 `_constants.py` 中与 Identity Repair / Synonym Unify 相关的常量

| 常量名 | 值 | 说明 |
|--------|-----|------|
| `MIN_IDENTITY_SUPPORT` | `5` | 加法恒等式最少需要的支持行数 |
| `NUMERIC_EQ_REL` | `1e-9` | 浮点数比较的相对容差 |
| `NUMERIC_EQ_ABS` | `1e-9` | 浮点数比较的绝对容差 |
| `NUMERIC_SCALAR` | `r"^[+-]?(?:\d+(?:\.\d+)?\|\.\d+)(?:[eE][+-]?\d+)?$"` | 数值标量正则 |
| `APPROX_TAG` | `"~"` | 近似值标记前缀 |
| `APPROX_PREFIXES` | `("~", "～")` | 所有近似值标记前缀 |
| `PLACEHOLDER_VALS` | `frozenset({"-", "null", "none", "nan", "n/a", "placeholder", ...})` | 占位符值集合 |

---

## 7. 复现指南

### 7.1 复现 Identity Repair

```python
from agents.etl._identity import repair_numeric_identities

header = ["总股本", "流通股", "非流通股", "名称"]
rows = [
    ["6970000", "3000000", "3970000", "公司A"],
    ["~5000000", "2000000", "3000000", "公司B"],  # ~ 标记的近似值
    ["", "1500000", "2500000", "公司C"],           # 缺失值
    ["8000000", "3000000", "5000000", "公司D"],    # 正常值
]

repaired_rows, repairs = repair_numeric_identities(header, rows)
# repairs 将包含被修复的单元格记录
# 所有 ~ 前缀被剥离
```

### 7.2 复现 Synonym Unify

```python
from agents.etl._reconcile import unify_table_synonyms

header = ["总股本(万元)", "total_equity_wan", "公司名称"]
rows = [
    ["", "6970000", "公司A"],
    ["5000000", "", "公司B"],
    ["", "3200000", "公司C"],
]

new_header, new_rows, applied = unify_table_synonyms(
    adapter=llm_adapter,  # ModelAdapter 实例
    header=header,
    rows=rows,
    prose_stem="sample_report",
    governance_columns=["总股本(万元)"],
    anchor_keys=["公司名称"],
    field_defs={"总股本(万元)": "总股本，单位为万元"},
    question="各公司的总股本是多少？",
    units_path=None,  # 单位元数据文件路径
)

# 如果 LLM 裁决 total_equity_wan 是 总股本(万元) 的同义词，
# 则 applied = [("total_equity_wan", "总股本(万元)")]
# new_header = ["总股本(万元)", "公司名称"]
# new_rows = [["6970000", "公司A"], ["5000000", "公司B"], ["3200000", "公司C"]]
```

---

## 8. 安全守卫总结

### 8.1 Identity Repair 守卫

| 守卫 | 说明 |
|------|------|
| 支持行数阈值 | 至少 `MIN_IDENTITY_SUPPORT`（5）行支持 |
| 支持 > 嫌疑 | 支持行数必须严格大于可疑行数 |
| 硬性违规拒绝 | 任何不可归责的违规行导致整个恒等式被拒绝 |
| 共识检查 | 所有相关恒等式必须解出同一值 |
| 粒度一致性 | 解出的值必须在原始陈述粒度上量化一致 |

### 8.2 Synonym Unify 守卫

| 守卫 | 说明 | 阶段 |
|------|------|------|
| 行级可行性 | 零冲突 AND（贡献新值 OR 完全冗余重叠） | 阶段 3 |
| 碰撞守卫 | 两个 discovered 列不能合并到同一个 canonical 列 | 阶段 5 |
| 单位守卫 | 不同单位的列不能合并 | 阶段 6 |
| 行级冲突守卫 | 任何冲突否决整对 | 阶段 7（`apply_synonym_merges` 内部） |
| 保护守卫 | 标识列（anchor_keys）不能合并（大小写合并除外） | `apply_synonym_merges` |