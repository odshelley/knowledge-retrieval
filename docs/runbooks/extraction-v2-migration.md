# Extraction v2 migration — Williams wipe + re-ingest

Run AFTER `feat/extraction-v2` is merged to main. **Nothing in this runbook has been executed
yet** — the wipe and re-run are deliberately deferred (decision 2026-07-12). Order matters.

Williams identifiers used below:

- book id: `title:probability with martingales`
- content sha (dynamic-partition key): `5eca18493325e7d0108ad8f09ccf5ec99f91c9690217a46a266c1cb0339a09f5`
- fixture book id (smoke-test leftover, delete too): `isbn:9783161484100`

## Steps

1. **Merge + restart** so the containers pick up the new code (bind-mounted code loads only on
   restart):

   ```sh
   git -C ~/Projects/knowledge-retrieval pull
   docker compose -f ~/Projects/knowledge-retrieval/docker-compose.yml restart dagster_webserver dagster_daemon
   ```

2. **Constraints:** `uv run python scripts/init_neo4j.py` (idempotent; adds `notation_id`,
   `proof_id` on top of the existing 14).

3. **Wipe graph data** (dry-run first, then live):

   ```sh
   uv run python scripts/wipe_book.py --book-id "title:probability with martingales" --dry-run
   uv run python scripts/wipe_book.py --book-id "title:probability with martingales"
   uv run python scripts/wipe_book.py --book-id "isbn:9783161484100"
   ```

   Optional: stale Postgres resolution rows (concept embeddings / alias_map) for wiped-out
   concepts are harmless and shared-scoped; leave them unless auditing.

4. **Clear Dagster partitions** so the sensors treat the PDF as new (inside the webserver
   container):

   ```sh
   docker exec kr_dagster_webserver sh -c 'cd /opt/code && uv run python -c "
   from dagster import DagsterInstance
   from pipeline.runtime.partitions import BOOKS_PARTITION, BOOK_CHAPTERS_PARTITION
   inst = DagsterInstance.get()
   SHA = \"5eca18493325e7d0108ad8f09ccf5ec99f91c9690217a46a266c1cb0339a09f5\"
   for ck in [k for k in inst.get_dynamic_partitions(BOOK_CHAPTERS_PARTITION) if k.startswith(SHA)]:
       inst.delete_dynamic_partition(BOOK_CHAPTERS_PARTITION, ck)
   inst.delete_dynamic_partition(BOOKS_PARTITION, SHA)
   print(\"partitions cleared\")
   "'
   ```

   Then two UI steps (version-proof; the Python APIs for these differ across Dagster releases):

   - **Wipe materializations:** Assets → select the ten `book_*` assets + `book_link_resolution`
     → Wipe materializations.
   - **Reset sensor cursors:** Sensors → `books_sensor` → reset cursor (its `run_key=<sha>`
     memory would otherwise suppress the re-request). Same for `book_chapters_sensor` and
     `book_links_sensor`.

5. **Re-ingest:** the PDF is already in `BOOKS_SOURCE_DIR`; `books_sensor` re-registers it
   within ~5 minutes. Pipeline: `ingest_book` (structure now carries chapter roles; front/back
   matter gets no partitions) → `extract_book_chapter` per content chapter → `book_links_sensor`
   fires `resolve_book_links` once all chapters land. Watch localhost:3000. Expected cost ≈ $3–5
   (Opus extraction + sketches).

6. **Verify** against the spec's success criteria (all Cypher against the Aura DB):

   ```cypher
   // Notation nodes exist (incl. the notation guide's a.e./CF/DF entries)
   MATCH (b:Book {id:"title:probability with martingales"})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->(s)
   MATCH (n:Notation)-[:INTRODUCED_IN]->(s) RETURN count(DISTINCT n);

   // Zero glossary lines as Definitions
   MATCH (b:Book {id:"title:probability with martingales"})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(d:Definition)
   WHERE d.term STARTS WITH "a.e." OR d.term STARTS WITH "CF:" OR d.term STARTS WITH "DF:"
   RETURN count(d);                                        // expect 0

   // Cross-chapter DEPENDS_ON > 50 (was 7)
   MATCH (b:Book {id:"title:probability with martingales"})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(:Result)-[e:DEPENDS_ON]->()
   RETURN count(e);

   // PROVED_IN + proof sketches on the majority of theorems
   MATCH (b:Book {id:"title:probability with martingales"})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(r:Result)
   RETURN count(r) AS results,
          count{ (r)-[:PROVED_IN]->() } AS proved_in,
          count{ (r)-[:HAS_PROOF]->() } AS sketches;

   // Zero heading-echo statements
   MATCH (b:Book {id:"title:probability with martingales"})-[:HAS_CHAPTER]->()-[:HAS_SECTION]->()-[:STATES]->(r:Result)
   WHERE r.statement = r.name RETURN count(r);             // expect 0

   // Front/back matter skipped entirely
   MATCH (b:Book {id:"title:probability with martingales"})-[:HAS_CHAPTER]->(ch)
   WHERE ch.role IN ["front_matter","back_matter"]
   MATCH (ch)-[:HAS_SECTION]->()-[:STATES|PART_OF]-(x) RETURN count(x);  // expect 0 STATES; chunks aren't created for skipped roles either
   ```

7. **Paper pipeline still green:** `uv run pytest -q`, and confirm the next paper ingestion run
   succeeds end-to-end (the shared prompt/schema changes apply to papers too).

## Coordination note (2026-07-12)

PR #15 (`plan/graphrag-augmentations`, other session) plans an eval baseline and two LLM
backfills over the existing corpus. Run THIS migration first: the baseline and backfill spend
should land on the corpus's post-v2 shape, and its Tasks 4/5a must be implemented against the
post-merge `merge_results`/`extraction.py`.
