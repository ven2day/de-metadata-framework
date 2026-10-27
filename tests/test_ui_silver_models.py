import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

with patch.dict(os.environ, {"FLASK_SECRET_KEY": "test-secret"}):
    import ui.app as ui_app


class SilverModelGenerationTests(unittest.TestCase):
    def _payload(self, strategy, target="nse_summary"):
        return json.dumps({
            "target_table": target,
            "strategy": strategy,
            "sources": [{"fqn": "minio.de_bronze.nse_ticks", "alias": "src"}],
            "columns": [{"source_expr": "src.security_id", "target": "security_id"}],
        })

    def _generate(self, strategy):
        with tempfile.TemporaryDirectory() as folder:
            fake_app_file = str(Path(folder) / "ui" / "app.py")
            with patch.object(ui_app, "__file__", fake_app_file):
                target = ui_app._write_silver_dbt_model(self._payload(strategy), "test")
            return (Path(folder) / "transformation" / "models" / "silver" / f"{target}.sql").read_text()

    def test_every_strategy_sets_locations_for_first_table_creation(self):
        for strategy in ("overwrite", "append", "insert_overwrite"):
            with self.subTest(strategy=strategy):
                sql = self._generate(strategy)
                self.assertIn('"data_location"', sql)
                self.assertIn(f"s3://{ui_app.SILVER_DATA_BUCKET}/nse_summary/", sql)
                self.assertIn(f"s3://{ui_app.ICEBERG_METADATA_BUCKET}/{ui_app.SILVER_DATABASE}/nse_summary/", sql)

    def test_overwrite_uses_atomic_replace_without_predeleting_data(self):
        self.assertIn("on_table_exists='replace'", self._generate("overwrite"))
        with patch("boto3.client", side_effect=AssertionError("must not delete storage")), patch("trino.dbapi.connect", side_effect=AssertionError("must not drop table")):
            self.assertIn("retained", "".join(ui_app._overwrite_silver_cleanup("nse_summary")))

    def test_unknown_strategy_and_path_like_table_fail_before_writing(self):
        with self.assertRaisesRegex(ValueError, "strategy"):
            ui_app._write_silver_dbt_model(self._payload("invalid"), "test")
        with self.assertRaisesRegex(ValueError, "identifier"):
            ui_app._write_silver_dbt_model(self._payload("overwrite", "../../bad"), "test")

    def test_cron_validation_rejects_invalid_values_before_persistence(self):
        with self.assertRaises(ValueError):
            ui_app._cron_trigger("99 * * * *")
        self.assertIsNotNone(ui_app._cron_trigger("0 9 * * mon-fri"))


if __name__ == "__main__":
    unittest.main()
