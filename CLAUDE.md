# CLAUDE.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Don't assume. Don't hide confusion. Surface tradeoffs.**

Before implementing:
- State your assumptions explicitly. If uncertain, ask.
- If multiple interpretations exist, present them - don't pick silently.
- If a simpler approach exists, say so. Push back when warranted.
- If something is unclear, stop. Name what's confusing. Ask.

## 2. Simplicity First

**Minimum code that solves the problem. Nothing speculative.**

- No features beyond what was asked.
- No abstractions for single-use code.
- No "flexibility" or "configurability" that wasn't requested.
- No error handling for impossible scenarios.
- If you write 200 lines and it could be 50, rewrite it.

Ask yourself: "Would a senior engineer say this is overcomplicated?" If yes, simplify.

## 3. Surgical Changes

**Touch only what you must. Clean up only your own mess.**

When editing existing code:
- Don't "improve" adjacent code, comments, or formatting.
- Don't refactor things that aren't broken.
- Match existing style, even if you'd do it differently.
- If you notice unrelated dead code, mention it - don't delete it.

When your changes create orphans:
- Remove imports/variables/functions that YOUR changes made unused.
- Don't remove pre-existing dead code unless asked.

The test: Every changed line should trace directly to the user's request.

## 4. Goal-Driven Execution

**Define success criteria. Loop until verified.**

Transform tasks into verifiable goals:
- "Add validation" → "Write tests for invalid inputs, then make them pass"
- "Fix the bug" → "Write a test that reproduces it, then make it pass"
- "Refactor X" → "Ensure tests pass before and after"

For multi-step tasks, state a brief plan:
```
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria let you loop independently. Weak criteria ("make it work") require constant clarification.

## 5. Comments

- Comments should be written according to the Go standards. No AI-driven comments are allowed.
- Every function, struct, field must cleanly and concisely documented with godoc with sufficient and relevant information to the user.
- Comments must written for IDEs to help developers understand the usage
- proto messages, fields, and services must be documented with comments that will be included in the generated code and documentation.

## 6. Naming Convention

- Variable names must clear and meaningful according idiomatic go standards

## 7. Code style: blank lines around multi-line blocks

Put a blank line before and after any multi-line statement block (a `{ }`-delimited body spanning more than one line — `for`, `if`, `switch`, `select`, function literals, composite literals used as statements).

Exceptions:

- No blank line needed if the block is the first or last statement in its enclosing block.
- A simple statement immediately before a block may be grouped with it (no blank line between) when it directly sets up the block's condition or subject (e.g. `schema := cfg.schema` before `if schema == nil`).

Single-line statements forming a related unit may stay grouped without blank lines. The blank line is only required at the boundary between a multi-line block and surrounding code.

Example:

```go
cfg := funcConfig{}

for _, o := range opts {
    o(&cfg)
}

schema := cfg.schema
if schema == nil && !hasExplicitSchema(opts) {
    var zero T
    reflector := &jsonschema.Reflector{
        Anonymous:      true,
        DoNotReference: true,
    }
    schema = reflector.Reflect(&zero)
}
```

## 8. No Magic Values

A literal is "magic" when its value carries meaning beyond the literal itself — a branching selector, an identifier, a sentinel, a limit. Replace these with named constants.

- Extract any repeated literal, and any single-use literal whose meaning isn't obvious from context, into a named `const` (grouped in a `const (...)` block near the top of the file, per the file-layout rule).
- Name the constant for its meaning, not its value: `actorSystemName`, not `matlasString`.
- Config / enumeration vocabularies (e.g. the accepted `AUTH_PROVIDER` or `EMAIL_SENDER` values) are constants, not bare strings in a `switch`.
- Follow existing conventions: if siblings already have constants (e.g. `systemRegistrationActor = "system:registration"`), a new peer literal (`"system:cleanup"`) gets the same treatment.
- Exempt: self-evident single-use values where a name adds no clarity — log messages, `slog` field keys, format strings, and zero/one/identity values.

---

**These guidelines are working if:** fewer unnecessary changes in diffs, fewer rewrites due to overcomplication, and clarifying questions come before implementation rather than after mistakes.