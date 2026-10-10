# Application Audit

Audit date: 2026-10-10. Scope: every service, the compose file, Dockerfiles, `mysql/init.sql`, the shared `utils` package, and the bundled dataset. No code was changed as part of this audit; this document records findings and recommendations only.

Severity scale: **High** = data loss, data corruption, or exposed secret. **Medium** = correctness or operability problem that will bite under normal use. **Low** = hygiene, performance, or documentation.

## Summary

| # | Severity | Area | Finding |
|---|----------|------|---------|
| 1 | High | Security | `.env` with MySQL passwords is committed and pushed to a public GitHub repo |
| 2 | High | Data integrity | Re-running the producer duplicates every record in Kafka and MySQL; no dedupe key exists |
| 3 | High | Data integrity | Failed batches are silently skipped after 5 retries, so offsets commit and rows are lost |
| 4 | High | Data quality | Bundled dataset is 91% duplicate lines (206 unique of 2,277) |
| 5 | Medium | Correctness | Producer never checks send results; failed sends are invisible |
| 6 | Medium | Correctness | Malformed JSON becomes an all-null row in MySQL instead of being dropped or counted |
| 7 | Medium | Correctness | Partial JDBC write + retry duplicates rows |
| 8 | Medium | Operability | No `restart` policy on the worker; a crash stops the pipeline permanently |
| 9 | Medium | Supply chain | Base images and `kafka-python` are end-of-life or at risk of disappearing |
| 10 | Medium | Performance | `batch_df.count()` computes every batch twice |
| 11 | Low | Security | Worker runs as root; MySQL and Kafka ports published to host; password on healthcheck command line |
| 12 | Low | Correctness | Tokenizer emits empty tokens on repeated whitespace; non-ASCII letters are stripped |
| 13 | Low | Correctness | Diagnostic consumer commits offsets, so it never shows the same "first 3" twice |
| 14 | Low | Hygiene | `ping_kafka_cluster` retries without sleeping; dead `key_serializer`; misnamed `broker_id` |
| 15 | Low | Docs | README is stale (wrong dataset, wrong clone URL, old example output, no mention of `.env` or MySQL) |
| 16 | Low | Testing | No tests, lint, or CI of any kind |

## Findings

### 1. Secrets committed to a public repository (High)

`.env` is tracked in git (`git ls-files .env` returns it), has been in history since commit `436ee67`, and the `origin` remote is a public GitHub repository. It contains `MYSQL_PASSWORD` and `MYSQL_ROOT_PASSWORD`, and both values are weak defaults. `.gitignore` does not exclude it.

Recommendation:
- Add `.env` to `.gitignore` and `git rm --cached .env`.
- Commit a `.env.example` with placeholder values so `docker-compose up` still has a documented contract.
- Treat the current passwords as burned. They are only used by local containers today, but rotate them anyway and do not reuse them elsewhere.
- Optionally rewrite history with `git filter-repo` if the repo is ever meant to be a portfolio piece; otherwise rotation is sufficient.

### 2. Producer re-runs duplicate the whole dataset (High)

The producer is a one-shot job with no idempotency guard. Running `docker-compose up` a second time without `down -v` restarts the exited `producer` container, which republishes all 2,277 lines as new Kafka offsets. The Spark checkpoint correctly resumes after the old offsets, so the worker faithfully appends 2,277 more rows to MySQL. `processed_reviews` has no unique key, so nothing stops this.

Recommendation:
- Add a natural key to `mysql/init.sql`, for example `UNIQUE KEY uq_review (reviewer_id, asin, unix_review_time)`, and write with `INSERT IGNORE` semantics. Spark's JDBC writer cannot do `INSERT IGNORE` directly; the two practical options are (a) write each batch into a staging table and run `INSERT IGNORE INTO processed_reviews SELECT ... FROM staging` via `mysql.connector`, or (b) drop the JDBC writer and do the insert in `foreachBatch` with `executemany` on an `INSERT IGNORE` statement.
- Independently, make the producer refuse to publish if the topic already has data (check end offsets with `KafkaConsumer.end_offsets`), or give it `restart: "no"` plus a documented "producer is run once" contract. The unique key is the real fix; the producer guard is defense in depth.
- See finding 4 before choosing the key: the dataset already contains 5 key collisions that are not exact duplicates.

### 3. Failed batches are dropped silently (High)

`write_transformed_batch` catches every exception, retries 5 times, then prints "Skipping batch" and returns normally. Because `foreachBatch` returned without raising, Structured Streaming commits the batch's offsets to the checkpoint. The rows in that batch are gone and will never be reprocessed. A MySQL outage longer than about 10 seconds therefore means permanent data loss with only a log line as evidence.

Recommendation:
- After the last retry, re-raise the exception. The query fails, the container exits non-zero, and on restart Spark replays the batch from the checkpoint. Combined with finding 2's unique key this gives at-least-once delivery with no duplicates.
- Pair this with finding 8 (`restart: on-failure`) so the restart actually happens.

### 4. Bundled dataset is mostly duplicates (High)

`producer/data/Appliances_5.json.gz` has 2,277 lines but only 206 distinct lines. Ten records are each repeated 204 times and 41 records appear twice. Among the 56 groups that share `(reviewerID, asin, unixReviewTime)`, 51 are byte-identical and 5 differ in content, so that triple is close to but not quite a unique key in the source.

Any sentiment statistic computed from this table is dominated by ten reviews. This may be an artifact of how the sample file was built rather than a bug in the pipeline, but it changes what "correct" output looks like.

Recommendation:
- Decide whether duplicates in the source should be collapsed. If yes, the unique key from finding 2 handles it and the expected row count becomes about 206 to 211. If no, add a `source_offset` column (Kafka partition + offset) and make that the unique key instead, so the table is still idempotent on replay without collapsing source rows.
- Either way, record the expected row count in a test (see Tests below) so the next person knows which behavior is intended.
- Consider regenerating the sample from the full Appliances dataset with `sort -u`.

### 5. Producer ignores send failures (Medium)

`producer.send()` returns a future that is never checked. With `retries=5` and `acks='all'`, a message that still fails is logged nowhere, and the final "Produced N records" line counts attempts, not acknowledgments. `producer.flush()` waits for in-flight sends but does not surface their errors.

Recommendation: collect the futures and call `.get(timeout=...)` after the loop, or attach `add_errback` and count failures. Exit non-zero if any send failed.

### 6. Malformed JSON becomes a null row (Medium)

`from_json` returns a null struct for unparseable input. The projection then yields a row where every review column is null and `text_lower` is `""`. That row is written to MySQL. The dataset today is clean (0 unparseable lines), so this is latent, but anything else landing on the topic will show up as ghost rows.

Recommendation: filter `review IS NULL` out before the projection, and count the dropped rows per batch (or write them to a `rejected_messages` table with the raw `value`). A test with a deliberately broken payload should assert it does not reach `processed_reviews`.

### 7. Partial write plus retry duplicates rows (Medium)

A JDBC batch write runs one transaction per DataFrame partition, not one per batch. If partition 1 commits and partition 2 fails, the retry loop rewrites partition 1. With a single-partition topic this is unlikely today, but `local[*]` and any future repartition make it real. The unique key from finding 2 removes the problem entirely; until then, set `.option("numPartitions", 1)` on the write or `coalesce(1)` so a batch is one transaction.

### 8. No restart policy on the worker (Medium)

`pyspark-worker` has no `restart:` setting. If Spark exits for any reason (OOM, Kafka rebalance error, finding 3 once it re-raises), the pipeline stays down until someone notices. Add `restart: on-failure` to the worker. Do not add it to the producer; a producer that fails mid-run and restarts would republish the prefix (finding 2).

### 9. End-of-life and at-risk dependencies (Medium)

- `python:3.9-slim`: Python 3.9 reached end of life in October 2025.
- `kafka-python==2.0.2`: last release 2020, unmaintained, known to fail to import on Python 3.12 (`kafka.vendor.six.moves`). Upgrading the base image without replacing this pin will break all three services.
- `bitnami/spark:3.4.1` and `bitnami/kafka:3.5.1`: Bitnami moved versioned tags out of the free `bitnami/` namespace in late 2025. Verify these tags still pull; if not, switch to `bitnamilegacy/...` short term and `apache/spark` + `apache/kafka` long term.
- Spark 3.4.x is past its maintenance window.
- `pyspark==3.4.1` is pip-installed into an image that already ships Spark 3.4.1 with PySpark. It is redundant, adds roughly 300 MB to the image, and risks shadowing the bundled copy. Remove it; keep `numpy` because `pyspark.ml` imports it.

Recommendation: move to `python:3.12-slim` with `kafka-python-ng` (drop-in) or `confluent-kafka`, and plan a Spark 3.5 bump with matching JARs in `pyspark-worker/jars/`.

### 10. Every batch is computed twice (Medium)

`batch_df.count()` at the top of `write_transformed_batch` forces a full evaluation (Kafka read, JSON parse, tokenize, stopword removal), then `.write.jdbc` does it all again. Use `batch_df.isEmpty()` (available since Spark 3.3) or `persist()` the batch. For this dataset the cost is small; for anything larger it doubles the worker's work.

Related: there is no `maxOffsetsPerTrigger`, so the first batch after a checkpoint wipe is the entire topic. Set it to bound batch size and memory.

### 11. Container hardening (Low)

- `pyspark-worker` runs with `user: root`. It almost certainly only needs write access to `/checkpoints`. Create the directory and `chown` it to the Bitnami user in the Dockerfile instead.
- `mysql` publishes `3306:3306` and `kafka` publishes `9092:9092` to the host. The Kafka mapping is also non-functional: the advertised listener is `kafka:9092`, which host clients cannot resolve. Remove both mappings, or add a separate `EXTERNAL://localhost:29092` listener for Kafka if host access is wanted.
- The MySQL healthcheck passes `-p${MYSQL_PASSWORD}` on the command line, which is visible in `docker inspect` and process listings. Use `MYSQL_PWD` in the healthcheck environment or `mysqladmin ping` against the root socket instead.
- `KAFKA_KRAFT_CLUSTER_ID` is hard-coded. Fine for a single-node dev cluster; note it in a comment so nobody copies it into a second cluster.

### 12. Text cleaning edge cases (Low)

- `Tokenizer` splits on single whitespace characters. After `regexp_replace` removes punctuation, "great.  Looks" becomes "great  looks" with two spaces and yields an empty token between them. Empty tokens survive `StopWordsRemover` and produce double spaces in the stored string. Use `RegexTokenizer(pattern="\\s+")` or collapse whitespace in the `regexp_replace` step.
- `[^a-zA-Z0-9\s]` strips every non-ASCII letter. The current dataset has no non-ASCII text, but any accented or non-English review will lose characters mid-word. Use `\p{L}\p{N}` if that is not intended.
- Contractions lose their apostrophe before stopword removal, so "don't" becomes "dont", which is not in the English stopword list and is kept.

### 13. Diagnostic consumer is not repeatable (Low)

`consumer.py` uses a fixed `group_id` with default auto-commit, so each run advances the group's offset. The second run prints messages 4 to 6, not "the first 3" the comment promises. Set `enable_auto_commit=False` or omit `group_id`.

### 14. Small hygiene items (Low)

- `ping_kafka_cluster` retries in a tight loop with no `time.sleep`. Each attempt is bounded by the admin client's own timeout, so it is not a hot spin, but a one-second sleep makes the logs and behavior predictable.
- `producer.py` configures a `key_serializer` but never passes a key, so it is dead config. If ordering per product matters later, send with `key=record["asin"]`.
- `write_transformed_batch(batch_df, broker_id)`: the second argument is the micro-batch id, not a broker id. Rename to `batch_id`.
- `preview_mysql_rows` prints `row[1]`, which is `event_timestamp` under the current schema. The README's example output shows the old concatenated-text column, so the preview no longer prints anything useful.
- `.gitignore` lists `kafka-data/`, which is a named volume, not a directory in the repo.

### 15. README is stale (Low)

The README says "Amazon book reviews" (the data is Appliances), gives a placeholder clone URL, shows example output from a previous schema, and does not mention `.env`, MySQL, the debug profile, or how to inspect results. `CLAUDE.md` is accurate and could be the source for a rewrite.

### 16. No tests or CI (Low severity, high leverage)

There is no test suite, no linter, and no CI. Every finding above was found by reading; none would be caught automatically on the next change.

## Recommended tests

Ordered by value per hour of effort.

1. **Schema contract test.** Parse the column list out of `mysql/init.sql` and assert it equals the DataFrame projection in `pyspark-worker.py` (minus the `id` autoincrement). This is the one invariant `CLAUDE.md` warns about in bold and it is currently enforced by nothing. Requires extracting the projection into a function (see refactor note below).

2. **Transformation unit tests** with a local `SparkSession` and no Kafka. Feed a small DataFrame of `(timestamp, value)` string rows through the parse/clean/tokenize/flatten pipeline and assert:
   - a known review produces the expected `tokens` and `tokens_no_stopwords` strings;
   - `reviewText: null` yields empty token strings and no exception;
   - a malformed JSON `value` is dropped (after finding 6 is fixed) rather than producing a null row;
   - repeated whitespace does not produce empty tokens (after finding 12);
   - non-ASCII input behaves the way you decide it should.

3. **Idempotency test** (integration). Bring the stack up, wait for `producer` to exit 0, poll MySQL until the row count is stable, record it, then re-run the producer container and assert the count is unchanged. This fails today and is the acceptance test for findings 2 and 4. It also pins the expected row count (206 or 2,277 depending on the decision in finding 4).

4. **Failure-path test for the worker.** With the stack running, `docker stop mysql` for 30 seconds, then start it, and assert that no rows are missing once the worker catches up. This fails today (finding 3) and passes once the worker re-raises and restarts.

5. **Producer unit tests** with `KafkaProducer` mocked: sent count equals line count of the gzip; each value JSON-decodes back to the source record; a failing future causes a non-zero exit (after finding 5).

6. **`utils` unit tests.** `wait_for_mysql` against a throwaway listening socket returns `True`, against a closed port with a 1-second timeout returns `False` in about a second. `ping_kafka_cluster` with a mocked `KafkaAdminClient` that raises twice then succeeds returns `True`.

7. **Compose sanity in CI.** `docker compose config --quiet` with a `.env.example` copied to `.env`, plus `ruff check .`. Cheap, and catches interpolation typos before anyone builds.

Refactor note: tests 1 and 2 need the Spark transformation to be callable without Kafka. Move the parse-through-select chain in `main()` into a `transform(df: DataFrame) -> DataFrame` function that takes the raw Kafka DataFrame. `main()` calls it on the stream; tests call it on a batch DataFrame. This is a pure extraction with no behavior change.

Suggested layout: `tests/unit/` (pytest, runs anywhere with `pip install pyspark pytest`), `tests/integration/` (pytest marked `integration`, requires Docker, run in a GitHub Actions job with the compose stack).

## Suggested order of work

1. Finding 1 (secrets) - ten minutes, do it first.
2. Findings 2, 3, 4, 7 together - they are one design decision: pick the unique key, make the write idempotent, make failures raise, add `restart: on-failure`. Then write the idempotency and failure-path tests to lock it in.
3. Extract `transform()` and add the schema contract and transformation unit tests.
4. Finding 9 (dependency bumps) - do after tests exist so the bump is verifiable.
5. Everything else as convenient.
