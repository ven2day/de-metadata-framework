"""Check real HTTP login, users rendering, CSRF and PostgreSQL user creation.

Only runs against the isolated compatibility stack's loopback port and test
credentials. Retains uniquely named test accounts in its throwaway database.
"""

import argparse
from html.parser import HTMLParser
import json
import os
from uuid import uuid4

import requests
import trino.dbapi


class CsrfParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.token = None

    def handle_starttag(self, tag, attrs):
        fields = dict(attrs)
        if tag == "input" and fields.get("name") == "csrf_token":
            self.token = fields.get("value")


def csrf(response):
    response.raise_for_status()
    parser = CsrfParser()
    parser.feed(response.text)
    assert parser.token, "Page did not render a CSRF token"
    return parser.token


def check_local_gold(session, base_url, pipeline_page):
    token = csrf(pipeline_page)
    assert "Oracle ADW" not in pipeline_page.text, "Pipeline still presents Oracle as its Gold target"
    headers = {"X-CSRFToken": token}
    catalog = os.environ.get("ICEBERG_CATALOG", "minio")
    silver_schema = os.environ.get("SILVER_DATABASE", "de_silver")
    gold_schema = os.environ.get("GOLD_DATABASE", "de_gold")
    metadata_bucket = os.environ.get("ICEBERG_METADATA_BUCKET", "de-iceberg-warehouse-bucket")
    name = "compat_ui_" + uuid4().hex[:10]
    transformed = name + "_transformed"
    source = f"{catalog}.{silver_schema}.{name}"
    destination = f"{catalog}.{gold_schema}.{name}"
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="compatibility-ui", catalog=catalog, request_timeout=90,
    )
    cursor = connection.cursor()

    def sql(statement):
        cursor.execute(statement)
        return cursor.fetchall()

    try:
        sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{silver_schema} "
            f"WITH (location='s3://{metadata_bucket}/{silver_schema}/')")
        sql(f"CREATE TABLE {source} AS SELECT * FROM (VALUES "
            "('1333', DOUBLE '100.5', DATE '2026-10-21'), "
            "('11536', DOUBLE '200.5', DATE '2026-10-21')) "
            "AS t(security_id, last_price, snapshot_date)")
        for _ in range(2):
            response = session.post(base_url + "/run-gold", headers=headers, data={
                "application_name": name, "silver_table": name, "gold_table": name,
                "run_date": "2026-10-21", "load_strategy": "replace",
            }, timeout=180)
            response.raise_for_status()
            assert "--- Gold Exit 0 ---" in response.text, response.text
            assert sql(f"SELECT count(*) FROM {destination}")[0][0] == 2

        retired = session.post(base_url + "/run-oracle", headers=headers, timeout=15)
        assert retired.status_code == 410, "Legacy Oracle endpoint must not launch a job"

        payload = {
            "target_table": transformed, "strategy": "overwrite",
            "sources": [{"fqn": source, "alias": "src"}],
            "columns": [
                {"source_expr": "src.security_id", "target": "security_id"},
                {"source_expr": "src.last_price * 2", "target": "last_price"},
                {"source_expr": "src.snapshot_date", "target": "snapshot_date"},
            ],
        }
        payload_json = json.dumps(payload)
        response = session.post(base_url + "/run-transform-load", headers=headers, json={
            "app_name": transformed, "silver_payload": payload_json,
            "silver_table": transformed, "gold_table": transformed,
            "run_date": "2026-10-21", "load_strategy": "replace",
        }, timeout=300)
        response.raise_for_status()
        assert "--- Silver Exit 0 ---" in response.text, response.text
        assert "--- Gold Exit 0 ---" in response.text, response.text
        assert dict(sql(f"SELECT security_id,last_price FROM {catalog}.{gold_schema}.{transformed}")) == {
            "1333": 201.0, "11536": 401.0,
        }, "UI chain did not publish the transformed Silver values"

        response = session.post(base_url + "/scheduled-jobs", headers=headers, json={
            "application_name": transformed, "layer_type": "silver",
            "cron_expression": "0 0 1 1 *", "silver_sql": "SELECT fixture",
            "silver_payload": payload_json, "gold_table": transformed,
            "load_strategy": "replace",
        }, timeout=30)
        assert response.status_code == 201, response.text
        job_id = response.json()["id"]
        jobs = session.get(base_url + "/scheduled-jobs", timeout=30)
        jobs.raise_for_status()
        job = next(item for item in jobs.json() if item["id"] == job_id)
        assert job["gold_table"] == transformed and job["load_strategy"] == "replace"
        assert not job.get("oracle_table"), "New local schedule populated a legacy Oracle destination"
        disabled = session.delete(base_url + f"/scheduled-jobs/{job_id}", headers=headers, timeout=30)
        disabled.raise_for_status()
        print("PASS: HTTP Gold replace/replay -> actual DBT Silver/Gold chain -> schedule persistence -> legacy Oracle disabled")
    finally:
        cursor.close()
        connection.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:15001")
    base_url = parser.parse_args().base_url.rstrip("/")
    assert base_url in {"http://127.0.0.1:15001", "http://ui:5000"}, (
        "This test only accepts the isolated compatibility UI"
    )
    session = requests.Session()
    login_page = session.get(base_url + "/login", timeout=15)
    response = session.post(
        base_url + "/login",
        data={
            "username": "root", "password": "CompatOnly!Passw0rd2026",
            "csrf_token": csrf(login_page),
        },
        allow_redirects=False, timeout=15,
    )
    assert response.status_code == 302, "Root login failed"
    pipeline_page = session.get(base_url + "/", timeout=15)
    assert pipeline_page.status_code == 200, "Pipeline page failed to render"
    check_local_gold(session, base_url, pipeline_page)
    users_page = session.get(base_url + "/users", timeout=15)
    assert users_page.status_code == 200, "Users template failed to render"
    username = "compat_" + uuid4().hex[:10]
    password = "CompatOnly!User2026"
    response = session.post(
        base_url + "/users",
        data={
            "username": username, "email": username + "@example.invalid",
            "password": password, "confirm_password": password,
            "csrf_token": csrf(users_page),
        },
        timeout=15,
    )
    response.raise_for_status()
    assert "created successfully" in response.text, "PostgreSQL user creation failed"
    assert username in response.text, "New account is absent from the users page"
    duplicate = session.post(
        base_url + "/users",
        data={
            "username": username + "_duplicate", "email": username + "@example.invalid",
            "password": password, "confirm_password": password,
            "csrf_token": csrf(response),
        },
        timeout=15,
    )
    assert duplicate.status_code == 200 and "Could not create user" in duplicate.text, (
        "Duplicate email was not handled cleanly"
    )
    assert session.get(base_url + "/users", timeout=15).status_code == 200, (
        "Failed user creation left the database connection unusable"
    )
    response = session.post(base_url + "/users", data={"username": "without_csrf"}, timeout=15)
    assert response.status_code == 400, "Users POST accepted a missing CSRF token"

    user_session = requests.Session()
    login_page = user_session.get(base_url + "/login", timeout=15)
    response = user_session.post(
        base_url + "/login",
        data={"username": username, "password": password, "csrf_token": csrf(login_page)},
        allow_redirects=False, timeout=15,
    )
    assert response.status_code == 302, "The newly created account cannot sign in"
    assert user_session.get(base_url + "/users", timeout=15).status_code == 403, (
        "Regular account could access root user administration"
    )
    print("PASS: HTTP login -> users.html -> PostgreSQL create -> new-user login -> role/CSRF checks")


if __name__ == "__main__":
    main()
