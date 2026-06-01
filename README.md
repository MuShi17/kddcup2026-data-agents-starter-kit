<div align="center">

# KDD Cup 2026 DataAgent-Bench Starter Kit

English | [中文](README.zh.md)

[![Official Website](https://img.shields.io/badge/Official%20Website-Visit%20dataagent.top-0ea5e9?style=for-the-badge&logo=googlechrome&logoColor=white&labelColor=0f172a)](https://dataagent.top)
[![Discord](https://img.shields.io/badge/Discord-Join%20Community-5865F2?style=for-the-badge&logo=discord&logoColor=white&labelColor=0f172a)](https://discord.com/invite/7eFwJQN3Fx)

</div>

This repository provides baseline starter kits for KDD Cup 2026 DataAgent-Bench. It is organized by competition phase so participants can start from the package that matches the phase they are working on.

## Repository Layout

| Directory | Purpose |
| --- | --- |
| `PHASE_1/` | Starter kit for Phase 1 tasks. |
| `PHASE_2/` | Starter kit for Phase 2 tasks and demo data format. |

Each phase directory is self-contained and includes its own README, configuration files, source code, dependency lock file, and baseline command-line entry points.

## Which Directory Should I Use?

Use `PHASE_1/` if you are working with the Phase 1 task format.

Use `PHASE_2/` if you are working with the Phase 2 demo release or Phase 2 task format. Phase 2 keeps the same general agent workflow while allowing richer task context files.

## Quick Start

Choose the phase directory first, then follow that directory's README.

```bash
cd PHASE_1
# or
cd PHASE_2
```

Then install dependencies and run the baseline from inside the selected directory:

```bash
uv sync
uv run dabench status --config configs/react_baseline.example.yaml
uv run dabench run-benchmark --config configs/react_baseline.example.yaml
```

The exact dataset layout, configuration options, tools, and output paths are documented in each phase-specific README.

## Baseline Overview

The starter kit contains a minimal ReAct-style data agent. The baseline is intentionally simple: it reads task metadata, inspects files under each task's `context/` directory through a small set of tools, calls an OpenAI-compatible model endpoint, and writes `prediction.csv` outputs.

Participants are expected to adapt or replace the baseline logic for their own methods. The code reads model endpoint settings from configuration or environment-compatible values rather than hardcoding service credentials.

## Common Project Structure

Inside each phase directory, the main files are organized as follows:

```text
configs/                         # Example baseline configuration
src/data_agent_baseline/          # Baseline source code
artifacts/                        # Local run outputs, ignored except .gitkeep
README.md                         # Phase-specific usage guide
README.zh.md                      # Chinese usage guide
pyproject.toml                    # Python project metadata
uv.lock                           # Locked dependency versions
```

## Notes

- Run commands from inside `PHASE_1/` or `PHASE_2/`, not from the repository root.
- Keep local run outputs under `artifacts/`; they are not intended to be committed.
- Review the phase-specific README before packaging or submitting a solution.

## Contact

- Official website: https://dataagent.top
- Discord: https://discord.com/invite/7eFwJQN3Fx
- WeChat official account: `数据智能与分析实验室 DIAL`
