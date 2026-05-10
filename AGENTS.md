# Project rules for AI tools

## Commits

**Never add `Co-Authored-By:` trailers crediting any AI tool to a git commit.**

This includes any AI assistant (e.g. GitHub Copilot, Cursor, Codeium, Gemini, and similar tools). Commits must list a human author only — AI tools are not co-authors.

**You may add an `Assisted-By:` trailer naming the tool.** Example:

```
Assisted-By: <tool name>
```

Use a short, plain tool name. No emails, no URLs, no marketing taglines. One trailer per commit is enough.

If your default commit template includes a `Co-Authored-By:` for an AI tool, strip it before committing and replace it with `Assisted-By: <tool>` instead. This rule applies to all branches and to rewrites.

## Versioning

**Never change the version declared in any project file** (`build.gradle`, `gradle.properties`, `pom.xml`, etc.) that is already stored locally.

If you need to publish a patched or experimental build, append a suffix to the existing version string instead of bumping the base version. Examples:

- `4.0.0-RC6` → `4.0.0-RC6-fix1`
- `4.0.0-RC6` → `4.0.0-RC6-SNAPSHOT`

Never remove or increment the base version number. The human decides when the official version changes.


<claude-mem-context>
# Memory Context

# [rqueue] recent context, 2026-05-09 9:08pm GMT+5:30

No previous sessions found.
</claude-mem-context>