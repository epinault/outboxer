@AGENTS.md

# Claude Code notes

## Plugin setup

Run once after cloning:

```bash
claude plugin install elixir@claude-code-elixir
claude plugin install superpowers@claude-plugins-official
claude plugin install code-review@claude-plugins-official
claude plugin install claude-mem@thedotmack
claude plugin install git+https://gitlab.com/epinault/elixir-scaffold.git
```

## Working here

- The substance is in `AGENTS.md`; keep this file thin.
- Durable project knowledge goes in `docs/` (see `docs/README.md`), committed to git.
- A hook formats Elixir files after each edit, so re-read a file before a second edit to the same
  region.
