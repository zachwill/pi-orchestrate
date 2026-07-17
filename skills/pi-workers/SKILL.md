---
name: pi-workers
description: Async worker orchestration protocol for pi-workers. Use when spawning, coordinating, responding to, or aborting crew_* workers.
---

# Pi Workers

Use `crew_list` to discover available workers, then `crew_spawn` to delegate one self-contained task.

## Delegation brief

Include:

- the exact task
- relevant files, paths, commands, or constraints
- acceptance criteria
- expected output format
- what not to change

## Async protocol

- Do not poll `crew_list` for completion.
- Results arrive as `pi-workers-result` steering messages owned by the parent session.
- After spawning, do not duplicate the worker's task in the parent session unless the user changes scope.
- Use `crew_abort` when delegated work is obsolete or cancelled.
- Use `crew_respond` only for interactive workers in waiting state.
- Use `crew_done` to dispose a waiting interactive worker when no more input is needed.

## Scout boundaries

Broad synthesis, architecture review, cross-file review, transcript/content review, planning, and recommendations go to `investigator`, not `scout`.

## Terminology

Use worker terminology in new prompts. The tools keep `crew_*` names for compatibility. `crew_spawn` accepts `worker`; `subagent` remains a legacy alias.
