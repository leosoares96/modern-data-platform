# Architecture Decision Records

Each file records one decision: the context, what was decided and what it costs. Decisions are
append-only. When one is replaced, a new ADR supersedes it instead of the old one being edited.

| ADR | Decision | Status |
| --- | --- | --- |
| [0001](0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](0002-compose-profiles.md) | Split the stack into Compose profiles | Accepted |

## Template

```markdown
# NNNN. Title

- Status: Proposed | Accepted | Superseded by NNNN
- Date: YYYY-MM-DD

## Context
What forces are at play?

## Decision
What we are doing.

## Consequences
What gets easier, what gets harder.
```
