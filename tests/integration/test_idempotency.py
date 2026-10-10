"""Idempotency check for the MySQL sink.

Requires the compose stack to be running from a clean state:

    docker compose down -v && docker compose up --build -d
    python3 tests/integration/test_idempotency.py      # or: pytest tests/integration

Waits for the first producer run to land in MySQL, re-runs the one-shot
producer (which republishes every record to Kafka), waits for the worker to
process the replay, and asserts the row count did not change.
"""
import os
import subprocess
import time

# Distinct lines in producer/data/Appliances_5.json.gz. The file has 2,277
# lines but only 206 unique records; review_hash collapses the rest.
EXPECTED_ROWS = 206

REPO_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")


def load_env():
    values = {}
    with open(os.path.join(REPO_ROOT, ".env")) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, value = line.split("=", 1)
                values[key] = value
    return values


def run(*args):
    return subprocess.run(args, capture_output=True, text=True, check=True).stdout


def row_counts(env):
    out = run(
        "docker", "exec", "-e", f"MYSQL_PWD={env['MYSQL_PASSWORD']}", "mysql",
        "mysql", "-N", "-u", env["MYSQL_USER"], env["MYSQL_DATABASE"],
        "-e", "SELECT COUNT(*), COUNT(DISTINCT review_hash) FROM processed_reviews",
    )
    total, distinct = (int(x) for x in out.split())
    return total, distinct


def container_state(name):
    out = run("docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", name)
    status, code = out.split()
    return status, int(code)


def batches_written():
    logs = subprocess.run(["docker", "logs", "pyspark-worker"], capture_output=True, text=True)
    return (logs.stdout + logs.stderr).count("successfully written to MySQL")


def wait_for(description, predicate, timeout, interval=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout}s waiting for {description}")


def test_rerunning_producer_does_not_duplicate_rows():
    env = load_env()

    wait_for("first producer run to exit 0",
             lambda: container_state("producer") == ("exited", 0), timeout=300)
    wait_for(f"{EXPECTED_ROWS} rows in processed_reviews",
             lambda: row_counts(env)[0] >= EXPECTED_ROWS, timeout=300)
    assert row_counts(env) == (EXPECTED_ROWS, EXPECTED_ROWS)

    before = batches_written()
    run("docker", "start", "-a", "producer")
    assert container_state("producer") == ("exited", 0)
    wait_for("worker to process the replayed messages",
             lambda: batches_written() > before, timeout=180)

    assert row_counts(env) == (EXPECTED_ROWS, EXPECTED_ROWS)


if __name__ == "__main__":
    test_rerunning_producer_does_not_duplicate_rows()
    print("OK: re-running the producer left processed_reviews at", EXPECTED_ROWS, "rows")
