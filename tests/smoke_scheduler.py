"""Exercise the persisted Silver-to-Gold scheduler fixture in de-compat only."""

import os
import sys


def main():
    assert os.environ.get("COMPOSE_PROJECT_NAME") == "de-compat", "Only the isolated de-compat project is allowed"
    assert os.environ.get("AUTH_DB_HOST") == "postgres-app", "Only the isolated local app database is allowed"
    assert not os.environ.get("AUTH_DATABASE_URL"), "External database URLs are forbidden for this smoke test"
    sys.path.insert(0, "/app")

    from psycopg2.extras import RealDictCursor
    from ui.app import _trigger_scheduled_job
    from ui.db import get_fresh_conn

    connection = get_fresh_conn()
    job_id = None
    try:
        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """SELECT id, application_name, gold_table, oracle_table, load_strategy
                   FROM public.scheduled_jobs
                   WHERE application_name LIKE 'compat_ui_%_transformed'
                     AND application_name ~ '^compat_ui_[0-9a-f]{10}_transformed$'
                     AND layer_type = 'silver' AND is_active = FALSE
                   ORDER BY id DESC LIMIT 1"""
            )
            fixture = cursor.fetchone()
            assert fixture, "Run smoke_ui.py first to create the inactive scheduler fixture"
            assert fixture["gold_table"] == fixture["application_name"], "Fixture must publish its own local Gold table"
            assert not fixture["oracle_table"], "Fixture must not contain an Oracle destination"
            assert fixture["load_strategy"] == "replace", "Fixture must use replay-safe replacement"
            job_id = fixture["id"]
            cursor.execute("SELECT COALESCE(MAX(id), 0) AS run_id FROM public.scheduled_job_runs WHERE job_id = %s", (job_id,))
            previous_run_id = cursor.fetchone()["run_id"]
            cursor.execute("UPDATE public.scheduled_jobs SET is_active = TRUE WHERE id = %s AND is_active = FALSE", (job_id,))
            assert cursor.rowcount == 1, "Fixture activation changed unexpectedly"
        connection.commit()

        _trigger_scheduled_job(job_id)

        with connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """SELECT id, status, logs FROM public.scheduled_job_runs
                   WHERE job_id = %s AND id > %s ORDER BY id DESC LIMIT 1""",
                (job_id, previous_run_id),
            )
            result = cursor.fetchone()
            assert result, "Scheduler did not persist a new execution record"
            logs = result["logs"] or ""
            assert result["status"] == "success", logs
            assert "--- Gold Exit 0 ---" in logs, logs
            assert "Publish completed" in logs, logs
            lowered = logs.lower()
            assert "oracle-load" not in lowered and "launching oracle" not in lowered, logs
            print(f"PASS: actual scheduler job {job_id}, run {result['id']} -> DBT Silver -> local Iceberg Gold -> persisted success; Oracle not attempted")
    finally:
        connection.rollback()
        if job_id is not None:
            with connection.cursor() as cursor:
                cursor.execute("UPDATE public.scheduled_jobs SET is_active = FALSE WHERE id = %s", (job_id,))
            connection.commit()
        connection.close()


if __name__ == "__main__":
    main()
