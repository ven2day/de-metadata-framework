from concurrent.futures import ThreadPoolExecutor
import os
from threading import Barrier
import unittest
from unittest.mock import patch

from flask import Flask
from psycopg2.extensions import TRANSACTION_STATUS_IDLE, TRANSACTION_STATUS_INERROR

from ui import db
from ui.auth import _local_next_url


class FakeConnection:
    def __init__(self):
        self.closed = False
        self.status = TRANSACTION_STATUS_IDLE
        self.rollbacks = 0

    def get_transaction_status(self):
        return self.status

    def rollback(self):
        self.rollbacks += 1
        self.status = TRANSACTION_STATUS_IDLE

    def close(self):
        self.closed = True


class AuthDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.add_url_rule("/", endpoint="index", view_func=lambda: "index")
        self.app.teardown_appcontext(db.close_connection)

    def test_database_connections_are_isolated_and_closed_per_request(self):
        barrier = Barrier(2)

        def request():
            with self.app.app_context():
                conn = db.get_conn()
                barrier.wait(timeout=2)
                self.assertIs(db.get_conn(), conn)
                return conn

        with patch.object(db, "get_fresh_conn", side_effect=lambda: FakeConnection()):
            with ThreadPoolExecutor(max_workers=2) as pool:
                results = list(pool.map(lambda _: request(), range(2)))
        self.assertIsNot(results[0], results[1])
        self.assertTrue(all(conn.closed for conn in results))
        self.assertTrue(all(conn.rollbacks == 1 for conn in results))

    def test_handled_constraint_failure_does_not_poison_followup_read(self):
        conn = FakeConnection()
        with patch.object(db, "get_fresh_conn", return_value=conn):
            with self.app.app_context():
                self.assertIs(db.get_conn(), conn)
                conn.status = TRANSACTION_STATUS_INERROR
                self.assertIs(db.get_conn(), conn)
                self.assertEqual(conn.rollbacks, 1)

    def test_login_redirect_accepts_only_local_paths(self):
        with self.app.test_request_context():
            self.assertEqual(_local_next_url("/users?tab=all"), "/users?tab=all")
            for value in (None, "https://evil.test/", "//evil.test/", "/\\evil.test/"):
                self.assertEqual(_local_next_url(value), "/")

    def test_existing_session_cannot_load_deactivated_account(self):
        with patch.dict(os.environ, {"FLASK_SECRET_KEY": "test-secret"}):
            from ui.app import load_user
        inactive = type("InactiveUser", (), {"is_active": False})()
        with patch("ui.models.User.get_by_id", return_value=inactive):
            self.assertIsNone(load_user("123"))
        self.assertIsNone(load_user("invalid"))

    def test_health_is_unauthenticated_and_does_not_consume_login_limit(self):
        with patch.dict(os.environ, {"FLASK_SECRET_KEY": "test-secret"}):
            from ui.app import app
        with app.test_client() as client:
            for _ in range(10):
                response = client.get("/health")
                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.get_json(), {"status": "ok"})
            self.assertEqual(client.get("/login").status_code, 200)


if __name__ == "__main__":
    unittest.main()
