from datetime import datetime, timezone
from threading import Event
import unittest

from ui.job_runtime import background_job_stream, scheduled_run_date


class JobRuntimeTests(unittest.TestCase):
    def test_scheduler_date_uses_india_date_across_utc_midnight(self):
        instant = datetime(2026, 10, 25, 19, 0, tzinfo=timezone.utc)
        self.assertEqual(scheduled_run_date("Asia/Kolkata", instant), "2026-10-26")
        self.assertEqual(scheduled_run_date("UTC", instant), "2026-10-25")

    def test_connected_stream_preserves_output_order(self):
        self.assertEqual(list(background_job_stream(lambda: iter(["one", "two"]))), ["one", "two"])

    def test_disconnecting_does_not_cancel_job_or_downstream_phase(self):
        release_job = Event()
        downstream_done = Event()
        cleaned = Event()

        def job():
            try:
                yield "ingestion started"
                self.assertTrue(release_job.wait(2))
                yield "ingestion complete"
                downstream_done.set()
                yield "bronze complete"
            finally:
                cleaned.set()

        stream = background_job_stream(job)
        self.assertEqual(next(stream), "ingestion started")
        stream.close()
        release_job.set()
        self.assertTrue(downstream_done.wait(2))
        self.assertTrue(cleaned.wait(2))

    def test_disconnected_worker_drains_more_than_queue_capacity(self):
        release_job = Event()
        done = Event()

        def job():
            yield "started"
            release_job.wait(2)
            for i in range(1000):
                yield str(i)
            done.set()

        stream = background_job_stream(job)
        next(stream)
        stream.close()
        release_job.set()
        self.assertTrue(done.wait(2))

    def test_worker_failure_is_reported_to_connected_browser(self):
        def job():
            yield "started"
            raise ValueError("job failed")

        stream = background_job_stream(job)
        self.assertEqual(next(stream), "started")
        with self.assertRaisesRegex(ValueError, "job failed"):
            next(stream)


if __name__ == "__main__":
    unittest.main()
