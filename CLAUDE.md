# Project rules for AI tools

## Commits

**Never add `Co-Authored-By:` trailers crediting any AI tool to a git commit.**

This includes Claude, Claude Code, GitHub Copilot, Cursor, Codeium, and any other AI assistant. Commits must list a human author only — AI tools are not co-authors.

**You may add an `Assisted-By:` trailer naming the tool.** Example:

```
Assisted-By: Claude Code
```

Use a short, plain tool name. No emails, no URLs, no marketing taglines (`(1M context)`, `Generated with ...`). One trailer per commit is enough.

If you are running as an agent or sub-agent and your default commit template includes a `Co-Authored-By:` for an AI tool, strip it before `git commit` and replace it with `Assisted-By: <tool>` instead. This rule applies to all branches and to rewrites.
