export const meta = {
  name: 'enact-book-ingestion-plan',
  description: 'Execute the 14-task book-ingestion TDD plan: implement → dual review → fix per task, then full verification',
  phases: [
    { title: 'Task 1', detail: 'Schema extensions + init script import fixes' },
    { title: 'Task 2', detail: 'Book identity (isbn > title)' },
    { title: 'Task 3', detail: 'Per-page parsing + TOC + fixture book PDF' },
    { title: 'Task 4', detail: 'Outline → chapter/section tree' },
    { title: 'Task 5', detail: 'Partitions, BOOKS_SOURCE_DIR, raw+parsed assets' },
    { title: 'Task 6', detail: 'Book metadata + ISBN regex' },
    { title: 'Task 7', detail: 'book_structure asset + chapter partition registration' },
    { title: 'Task 8', detail: 'Page-aware section chunking + book_chunks' },
    { title: 'Task 9', detail: 'Structure write Cypher + book_structure_write' },
    { title: 'Task 10', detail: 'Chapter extraction + Definition.name' },
    { title: 'Task 11', detail: 'book_chapter_resolved (shared resolution)' },
    { title: 'Task 12', detail: 'Statement write + cross-chapter DEPENDS_ON' },
    { title: 'Task 13', detail: 'Jobs, sensors, definitions, env/compose wiring' },
    { title: 'Task 14', detail: 'Integration tests (book e2e + Lévy shared concept)' },
    { title: 'Finalize', detail: 'Full unit suite, compose config, definitions load' },
  ],
}

const PLAN = 'docs/superpowers/plans/2026-07-05-book-ingestion.md'

const IMPL_SCHEMA = {
  type: 'object',
  properties: {
    tests_passed: { type: 'boolean', description: 'All test commands for this task pass' },
    commit: { type: 'string', description: 'SHA of the commit made for this task (empty if none)' },
    summary: { type: 'string' },
    deviations: { type: 'array', items: { type: 'string' }, description: 'Any places implementation had to differ from the plan, with why' },
  },
  required: ['tests_passed', 'commit', 'summary', 'deviations'],
}

const REVIEW_SCHEMA = {
  type: 'object',
  properties: {
    approved: { type: 'boolean' },
    issues: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          severity: { type: 'string', enum: ['critical', 'minor'] },
          file: { type: 'string' },
          description: { type: 'string' },
        },
        required: ['severity', 'file', 'description'],
      },
    },
  },
  required: ['approved', 'issues'],
}

const FINAL_SCHEMA = {
  type: 'object',
  properties: {
    unit_tests_passed: { type: 'boolean' },
    compose_valid: { type: 'boolean' },
    definitions_load: { type: 'boolean' },
    integration_status: { type: 'string', description: 'ran+passed | ran+failed(<why>) | pending(<why>)' },
    notes: { type: 'string' },
  },
  required: ['unit_tests_passed', 'compose_valid', 'definitions_load', 'integration_status', 'notes'],
}

const GUARDRAILS = `
Hard rules:
- Work in the current directory (a git worktree). NEVER push, never open PRs, never touch git config, never run 'docker compose up/down', never 'git add -A' or 'git add .' (the user has unrelated uncommitted files: notebooks/smoke_test.ipynb, .playwright-mcp/, notebooks/paper_summaries.ipynb — do not stage, commit, or modify them).
- Stage exactly the files your task's commit step lists (plus uv.lock if uv changed it).
- Follow the plan's TDD steps IN ORDER: write the failing tests first, run them and confirm they fail for the expected reason, then implement, then confirm they pass, then run the wider test commands the task lists, then commit with the exact message given.
- The plan's Global Constraints section binds every task — read it first.
- If the plan's code conflicts with reality (API signature differs, a helper is named differently), adapt MINIMALLY, keep the plan's tests as the source of truth for behavior, and record the change in 'deviations'. Do not weaken or delete planned tests to make them pass.
- If after honest debugging you cannot get the task's tests green, STOP, do not commit broken work (leave changes unstaged), and return tests_passed=false with a precise explanation.`

const TASKS = [
  { n: 1, title: 'Schema extensions + fix broken init scripts' },
  { n: 2, title: 'Book identity (pipeline/books/identity.py)' },
  { n: 3, title: 'Per-page parsing + TOC extraction and the fixture book PDF' },
  { n: 4, title: 'Outline → Chapter/Section tree (pipeline/books/outline.py)' },
  { n: 5, title: 'Partitions, book source dir, book_raw_blob + book_parsed assets' },
  { n: 6, title: 'Book metadata (pipeline/books/metadata.py + book_metadata asset)' },
  { n: 7, title: 'book_structure asset (registers chapter partitions)' },
  { n: 8, title: 'Page-aware section chunking + book_chunks asset' },
  { n: 9, title: 'Structure write Cypher + book_structure_write asset' },
  { n: 10, title: 'Chapter extraction + Definition.name + book_chapter_extraction asset' },
  { n: 11, title: 'book_chapter_resolved asset (shared resolution)' },
  { n: 12, title: 'Statement write + book_chapter_graph_write asset' },
  { n: 13, title: 'Jobs, sensors, definitions, env + compose wiring' },
  { n: 14, title: 'Integration tests — book end-to-end + Lévy shared-concept test' },
]

const results = []
let aborted = false

for (const t of TASKS) {
  phase(`Task ${t.n}`)
  log(`Task ${t.n}: ${t.title}`)

  const extra = t.n === 14
    ? `\nTask-14 specifics: Step 2 (unit run green + integration tests collected-but-SKIPPED without --run-integration) is the pass gate for tests_passed. For Step 3, first check whether live services are reachable ('docker compose ps' shows postgres+minio running AND .env has NEO4J_NEW_URI/OPENAI_API_KEY set). If reachable, attempt the integration run as the plan describes and report the outcome in your summary; if not reachable or it would require credentials you lack, note it as pending in the summary — that does NOT make tests_passed false.`
    : ''

  const impl = await agent(
    `You are executing ONE task from a TDD implementation plan for the knowledge-retrieval repo (Dagster + Neo4j GraphRAG pipeline; a book-ingestion lane is being added alongside the existing paper lane).

Read ${PLAN} — first its 'Global Constraints' and 'File Structure' sections, then the section '### Task ${t.n}: ${t.title}'. Implement ONLY that task, completing every checkbox step exactly as written (the plan contains the full code and commands).${extra}
${GUARDRAILS}

Return: tests_passed, the commit SHA you created, a 2-4 sentence summary, and deviations (empty array if none).`,
    { label: `impl:task${t.n}`, phase: `Task ${t.n}`, schema: IMPL_SCHEMA },
  )

  if (!impl || !impl.tests_passed || !impl.commit) {
    results.push({ task: t.n, status: 'FAILED', detail: impl ? impl.summary : 'agent died/skipped' })
    aborted = true
    log(`Task ${t.n} FAILED — stopping the pipeline here so later tasks do not build on broken work.`)
    break
  }

  const reviewCtx = `The commit under review is ${impl.commit} in the current repo (a worktree). The plan is ${PLAN}, section '### Task ${t.n}: ${t.title}'. Implementer's summary: ${impl.summary}. Deviations they declared: ${JSON.stringify(impl.deviations)}.
You are READ-ONLY: inspect with git show/diff, Read, Grep, and you may run 'uv run pytest <paths>' to check claims — but make NO edits and NO commits.
Report issues with severity 'critical' ONLY for: broken behavior, a planned test weakened/omitted, paper-pipeline behavior changed beyond the plan's allowed shared-file edits, non-idempotent Neo4j writes, or staging/committing the user's unrelated files. Style nits are 'minor'. If a declared deviation is reasonable, it is not an issue.`

  const reviews = await parallel([
    () => agent(
      `Adversarial plan-adherence review. ${reviewCtx}
Check: every checkbox step of the task actually done; test code matches the plan (or deviations justify differences); commit contains exactly the intended files; Global Constraints respected.`,
      { label: `review:plan:${t.n}`, phase: `Task ${t.n}`, schema: REVIEW_SCHEMA },
    ),
    () => agent(
      `Adversarial bug hunt. ${reviewCtx}
Ignore plan-conformance; hunt real defects in the committed code: off-by-one page arithmetic, id/key mismatches between producer and consumer artifacts, Cypher that can't match (wrong property/label names), mutation of paper-path behavior, unsafe assumptions (empty lists, missing dict keys), test assertions that pass vacuously.`,
      { label: `review:bugs:${t.n}`, phase: `Task ${t.n}`, schema: REVIEW_SCHEMA },
    ),
  ])

  const critical = reviews.filter(Boolean).flatMap(r => r.issues || []).filter(i => i.severity === 'critical')

  if (critical.length === 0) {
    results.push({ task: t.n, status: 'done', commit: impl.commit, deviations: impl.deviations })
    continue
  }

  log(`Task ${t.n}: ${critical.length} critical review finding(s) — dispatching fixer`)
  const fix = await agent(
    `You are fixing confirmed review findings on commit ${impl.commit} for plan task ${t.n} (${PLAN}, section '### Task ${t.n}: ${t.title}').
Findings (fix ALL, but verify each is real first — if one is a false positive, say so in deviations instead of "fixing" it):
${JSON.stringify(critical, null, 2)}
${GUARDRAILS}
After fixing: re-run the task's test commands AND 'uv run pytest' (full unit suite), then commit staged fix files with message 'fix(books): address review findings on task ${t.n}'. Return the fix commit SHA.`,
    { label: `fix:task${t.n}`, phase: `Task ${t.n}`, schema: IMPL_SCHEMA },
  )

  if (!fix || !fix.tests_passed) {
    results.push({ task: t.n, status: 'FIX_FAILED', commit: impl.commit, findings: critical, detail: fix ? fix.summary : 'fixer died/skipped' })
    aborted = true
    log(`Task ${t.n} fix FAILED — stopping.`)
    break
  }
  results.push({ task: t.n, status: 'done+fixed', commit: impl.commit, fix_commit: fix.commit, findings_fixed: critical.length, deviations: [...impl.deviations, ...fix.deviations] })
}

phase('Finalize')
let final = null
if (!aborted) {
  final = await agent(
    `Final verification of the completed book-ingestion implementation in the current repo (worktree). READ-ONLY plus test commands — no edits, no commits, no push.
Run and report honestly:
1. 'uv run pytest' — full unit suite (integration auto-excluded). unit_tests_passed = exit 0.
2. 'docker compose config --quiet' — compose_valid = exit 0.
3. 'uv run python -c "from pipeline.definitions import defs; print(len(list(defs.get_asset_graph().get_all_asset_keys())))"' (adapt the accessor if the Dagster 1.9.5 API differs) — definitions_load = prints an asset count without error; include the count in notes.
4. 'uv run pytest tests/integration/ -v' (WITHOUT --run-integration) — confirm all integration tests are collected and SKIPPED, not erroring; fold into notes.
5. integration_status: report what task 14's implementer said about the live integration run if visible in git log/commit messages, else 'pending (needs live services + INTEGRATION_BOOK_HASH)'.
6. 'git log --oneline' — list the commits added for this plan in notes.`,
    { label: 'final-verify', phase: 'Finalize', schema: FINAL_SCHEMA },
  )
}

return { aborted, results, final }
