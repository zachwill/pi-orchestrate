# @zachwill/pi-workers

Async worker orchestration for pi. It preserves the `crew_*` tool names from `@melihmucuk/pi-crew` while making worker definitions user-owned instead of package-bundled.

## Install / dev

```bash
cd /Users/zachwill/code/pi-workers
bun install
bun run typecheck
pi install /Users/zachwill/code/pi-workers
```

On first load, the extension copies the default `investigator`, `scout`, and `worker` examples into `~/.pi/agent/pi-workers/agents/` if they do not already exist. It writes `~/.pi/agent/pi-workers/default-workers.json` as a one-time marker, so deleting those worker Markdown files later does not cause them to reappear. The package-owned files in `examples/agents/*.md` remain templates only; active workers are always loaded from user/project namespaces.

## Tools

- `crew_list` — list discovered workers and active workers for the current session.
- `crew_spawn` — spawn a non-blocking worker. Use `worker`; legacy `subagent` is accepted.
- `crew_abort` — abort one, many, or all active workers.
- `crew_respond` — send input to an interactive waiting worker.
- `crew_done` — dispose an interactive waiting worker.

Results are delivered as session-owned steering messages with custom types `pi-workers-result` and `pi-workers-note`. Do not poll `crew_list` for completion.

## Default workers

The default workers are ordinary Markdown files after first load:

```txt
~/.pi/agent/pi-workers/agents/investigator.md
~/.pi/agent/pi-workers/agents/scout.md
~/.pi/agent/pi-workers/agents/worker.md
```

Edit or delete them like any other user-owned worker. They are not overwritten during package updates. To reinstall defaults manually, copy files from `examples/agents/` or remove `~/.pi/agent/pi-workers/default-workers.json` and restart pi.

## Worker locations

Discovery precedence, highest first:

1. Project canonical: `<cwd>/.pi/pi-workers/agents/*.md`, `<cwd>/.pi/pi-workers/config.json`
2. Project compatibility: `<cwd>/.pi/agents/*.md`, `<cwd>/.pi/pi-workers.json`, `<cwd>/.pi/pi-crew.json`
3. User canonical: `~/.pi/agent/pi-workers/agents/*.md`, `~/.pi/agent/pi-workers/config.json`
4. User compatibility: `~/.pi/agent/agents/*.md`, `~/.pi/agent/pi-workers.json`, `~/.pi/agent/pi-crew.json`

Compatibility paths work with warnings. Legacy `pi-crew.json` is only read when the canonical `pi-workers/config.json` is absent in the same scope.

## Worker Markdown

```md
---
name: scout
description: Fast bounded reconnaissance.
model: anthropic/claude-sonnet-4-5
thinking: low
tools: read, grep, find, ls
skills:
  - vitepress-writing
compaction: true
interactive: false
---
You are a scout worker. Report concise findings with file paths.
```

Required frontmatter: `name`, `description`.

Optional frontmatter: `model`, `thinking`, `tools`, `skills`, `compaction`, `interactive`.

`tools` and `skills` accept YAML arrays or comma-separated strings. Omitted `tools` means child-session defaults after filtering this extension out. `tools: []` means no tools. Omitted `skills` means normal pi skill discovery. `skills: []` means no skills.

## Config schema

Canonical filename: `config.json` in the `pi-workers` namespace.

```json
{
  "defaults": {
    "scope": "project",
    "confirmProjectWorkers": false,
    "maxActiveWorkers": 8,
    "maxConcurrentSpawns": 4
  },
  "workers": {
    "scout": {
      "description": "Fast bounded reconnaissance.",
      "promptFile": "./prompts/scout.md",
      "model": "anthropic/claude-sonnet-4-5",
      "thinking": "low",
      "tools": ["read", "grep", "find", "ls"],
      "skills": [],
      "compaction": true,
      "interactive": false
    }
  }
}
```

Within the same source/scope, Markdown defines the base worker and config overrides Markdown fields. Config-only workers are valid when they have `description` and exactly one prompt source (`prompt` or `promptFile`). `promptFile` resolves relative to the config file and supports `~`.

## Skills are not the registry

`skills/` contains main-agent instructions. Worker definitions are child-agent system prompts plus runtime config. This package includes only `skills/pi-workers/SKILL.md` for orchestration protocol: when to spawn, how to brief workers, async result handling, interactive lifecycle, and anti-polling rules.

## Migration from pi-crew

- Move active agents from `~/.pi/agent/agents/*.md` to `~/.pi/agent/pi-workers/agents/*.md`.
- Move project agents from `<cwd>/.pi/agents/*.md` to `<cwd>/.pi/pi-workers/agents/*.md`.
- Rename `pi-crew.json` to `pi-workers/config.json` and use root `workers` instead of `agents`.
- Update new `crew_spawn` calls to pass `worker`; existing `subagent` calls still work for now.

Compatibility is intentionally a bridge, not the design center. The preferred shape is the canonical `pi-workers/` namespace plus `worker` terminology.

## Runtime model

The extension uses a process-level singleton `WorkerRuntime` on `globalThis`. Each worker gets an isolated child `AgentSession`; the child resource loader filters this extension out by resolved path to avoid recursive orchestration tools by default. Interactive workers remain alive in waiting state until `crew_respond` or `crew_done`; non-interactive workers dispose after result delivery. Pending result messages are queued in memory for inactive owner sessions and dropped after 24 hours.
