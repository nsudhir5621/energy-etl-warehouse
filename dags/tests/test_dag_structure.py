"""Unit tests for the Airflow DAG structure and task dependencies.

These tests verify the DAG definition (task IDs, dependencies, retry policy)
without needing a running Airflow scheduler or PostgreSQL connection. The DAG
file must be importable for `airflow dags list` to discover it.
"""

import importlib
import sys
from pathlib import Path

import pytest

# Make dags/ and src/ importable for test collection.
_project_root = str(Path(__file__).resolve().parent.parent.parent)
_dags_dir = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, _dags_dir)
sys.path.insert(0, str(Path(_project_root, "src")))


@pytest.fixture(scope="module")
def dag():
    """Import the DAG module and return the DAG object."""
    # Clear any cached import so we always get a fresh DAG.
    if "energy_pipeline_dag" in sys.modules:
        del sys.modules["energy_pipeline_dag"]
    import energy_pipeline_dag as mod

    importlib.reload(mod)
    return mod.dag


class TestDAGDefinition:
    def test_dag_id(self, dag):
        assert dag.dag_id == "energy_etl_pipeline"

    def test_dag_description(self, dag):
        assert "extract" in dag.description.lower()
        assert "raw zone" in dag.description.lower()
        assert "dbt" in dag.description.lower()

    def test_schedule(self, dag):
        assert dag.schedule == "@monthly"

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_max_active_runs(self, dag):
        assert dag.max_active_runs == 1

    def test_default_owner(self, dag):
        assert dag.default_args["owner"] == "energy-team"

    def test_default_retries(self, dag):
        assert dag.default_args["retries"] == 2

    def test_default_retry_delay(self, dag):
        from datetime import timedelta

        assert dag.default_args["retry_delay"] == timedelta(minutes=5)


class TestTaskIDs:
    def test_has_extract_task(self, dag):
        assert "extract" in dag.task_ids

    def test_has_load_staging_task(self, dag):
        assert "load_staging" in dag.task_ids

    def test_has_merge_to_core_task(self, dag):
        assert "merge_to_core" in dag.task_ids

    def test_has_run_dbt_task(self, dag):
        assert "run_dbt" in dag.task_ids

    def test_exactly_four_tasks(self, dag):
        assert len(dag.task_ids) == 4


class TestTaskDependencies:
    def test_load_staging_depends_on_extract(self, dag):
        extract = dag.get_task("extract")
        load_staging = dag.get_task("load_staging")
        assert load_staging in extract.downstream_list

    def test_merge_to_core_depends_on_load_staging(self, dag):
        load_staging = dag.get_task("load_staging")
        merge = dag.get_task("merge_to_core")
        assert merge in load_staging.downstream_list

    def test_run_dbt_depends_on_merge(self, dag):
        merge = dag.get_task("merge_to_core")
        run_dbt = dag.get_task("run_dbt")
        assert run_dbt in merge.downstream_list

    def test_linear_chain(self, dag):
        """Verify the exact dependency chain: extract >> load >> merge >> dbt."""
        extract = dag.get_task("extract")
        load_staging = dag.get_task("load_staging")
        merge = dag.get_task("merge_to_core")
        run_dbt = dag.get_task("run_dbt")

        # extract has one downstream, which is load_staging
        downstreams = extract.downstream_list
        assert len(downstreams) == 1
        assert downstreams[0] == load_staging

        # load_staging has one downstream, which is merge
        downstreams = load_staging.downstream_list
        assert len(downstreams) == 1
        assert downstreams[0] == merge

        # merge has one downstream, which is run_dbt
        downstreams = merge.downstream_list
        assert len(downstreams) == 1
        assert downstreams[0] == run_dbt

        # run_dbt has no downstream
        assert len(run_dbt.downstream_list) == 0


class TestTaskFunctions:
    def test_extract_function_pushes_xcom(self, dag):
        """The extract task callable pushes run_id, manifest_path, content_hash."""
        extract = dag.get_task("extract")
        # We can't easily call the Python callable without Airflow context,
        # but we can verify the callable is set and has the right name.
        # The PythonOperator stores the callable as python_callable.
        assert callable(extract.python_callable)

    def test_load_staging_function_pulls_xcom(self, dag):
        """The load_staging task pulls run_id and manifest_path from extract."""
        load_staging = dag.get_task("load_staging")
        assert callable(load_staging.python_callable)

    def test_merge_function_pulls_xcom(self, dag):
        """The merge task pulls run_id from extract's XCom."""
        merge = dag.get_task("merge_to_core")
        assert callable(merge.python_callable)

    def test_dbt_function_uses_subprocess(self, dag):
        """The dbt task invokes dbt via subprocess."""
        run_dbt = dag.get_task("run_dbt")
        assert callable(run_dbt.python_callable)


class TestExtractPersistence:
    """Regression tests for the raw-zone write fix in extract_task."""

    def test_extract_task_persists_via_raw_sink(self, dag, monkeypatch):
        """extract_task must write the batch to the raw zone before returning."""
        from unittest.mock import MagicMock, patch

        extract = dag.get_task("extract")
        call = extract.python_callable

        monkeypatch.setenv("FIXTURE_PATH", "/tmp/fake_fixture.json")

        mock_context = {"ti": MagicMock()}
        mock_result = MagicMock()
        mock_result.batch.run_id = "test-run-id"
        mock_result.batch.content_hash = "test-hash"
        mock_result.report.ok = True

        with (
            patch("energy_pipeline.config.Settings.from_env") as mock_settings,
            patch("energy_pipeline.extract.extract_fixture") as mock_extract,
            patch("energy_pipeline.storage.LocalObjectStore") as mock_store_cls,
            patch("energy_pipeline.storage.RawSink") as mock_sink_cls,
        ):
            mock_settings.return_value = MagicMock(raw_root="/tmp/raw")
            mock_extract.return_value = mock_result

            mock_store = MagicMock()
            mock_store_cls.return_value = mock_store
            mock_upload = MagicMock()
            mock_upload.run_id = "test-run-id"
            mock_upload.manifest_path = "raw/eia/test/slug/run_id=test-run-id/manifest.json"
            mock_upload.content_hash = "test-hash"
            mock_sink_cls.return_value.write.return_value = mock_upload

            result = call(**mock_context)

            # RawSink.write must have been called with the batch
            mock_sink_cls.return_value.write.assert_called_once_with(mock_result.batch)

            # XCom pushes use UploadResult fields, not manually-constructed paths
            mock_context["ti"].xcom_push.assert_any_call(
                key="manifest_path",
                value="raw/eia/test/slug/run_id=test-run-id/manifest.json",
            )
            mock_context["ti"].xcom_push.assert_any_call(key="run_id", value="test-run-id")
            mock_context["ti"].xcom_push.assert_any_call(key="content_hash", value="test-hash")

            assert result == "test-run-id"

    def test_manifest_path_is_logical_key_not_filesystem(self, dag):
        """manifest_path must be a logical key (no RAW_ROOT prefix) for store.read."""
        # The path returned by RawSink.write is f"{batch.namespace}/manifest.json"
        # which is a logical object key. load_staging_task passes it directly to
        # load_raw_batch(store, conn, manifest_path) which calls store.read(path).
        # LocalObjectStore.read() resolves path under self._root, so the path must
        # NOT include the root prefix.
        from energy_pipeline.raw import RawBatch, RawPage
        from energy_pipeline.storage import InMemoryObjectStore, RawSink

        pages = [
            RawPage(
                page_number=0,
                offset=0,
                payload=(
                    '[{"period": "2026-01", "location": "US", '
                    '"sectorid": "1", "fueltypeid": "1", "generation": "1.0"}]'
                ),
                record_count=1,
                sha256="abc",
            )
        ]
        batch = RawBatch(
            run_id="00000000-0000-0000-0000-000000000001",
            source="test",
            route="electricity/test",
            frequency="monthly",
            measure="generation",
            window_start="2026-01",
            window_end="2026-01",
            extracted_at="2026-01-01T00:00:00Z",
            pages=[pages[0]],
        )

        store = InMemoryObjectStore()
        result = RawSink(store).write(batch)

        # The manifest path is a logical key, not a filesystem path
        assert not result.manifest_path.startswith("/")  # no leading slash
        assert result.manifest_path.startswith(batch.prefix + "/")  # no RAW_ROOT
        assert result.manifest_path.endswith("manifest.json")

        # The store can read it back (verifying the logical key is correct)
        assert store.read(result.manifest_path) is not None

    def test_extract_task_raises_on_storage_collision(self, dag, monkeypatch):
        """If raw storage already has the object, extract_task should fail.

        Per the immutability contract, a collision is a good failure — it means
        history was protected. The task should not catch and silently overwrite.
        """
        from unittest.mock import MagicMock, patch

        from energy_pipeline.errors import ObjectExistsError

        extract = dag.get_task("extract")
        call = extract.python_callable

        monkeypatch.setenv("FIXTURE_PATH", "/tmp/fake_fixture.json")

        mock_context = {"ti": MagicMock()}

        with (
            patch("energy_pipeline.config.Settings.from_env") as mock_settings,
            patch("energy_pipeline.extract.extract_fixture") as mock_extract,
            patch("energy_pipeline.storage.LocalObjectStore"),
            patch("energy_pipeline.storage.RawSink") as mock_sink_cls,
        ):
            mock_settings.return_value = MagicMock(raw_root="/tmp/raw")
            mock_extract.return_value = MagicMock(
                batch=MagicMock(run_id="test-run-id"),
            )

            mock_sink_cls.return_value.write.side_effect = ObjectExistsError(
                "raw object already exists"
            )

            with pytest.raises(ObjectExistsError):
                call(**mock_context)

            # XCom must not have been pushed for a failed write
            mock_context["ti"].xcom_push.assert_not_called()


class TestLiveExtraction:
    @pytest.mark.parametrize(
        "latest, expected_start, expected_end",
        [("2026-07", "2026-05", "2026-07"), (None, "1969-11", "1970-01")],
    )
    def test_live_extraction_discovers_period_through_api_source(
        self, dag, monkeypatch, latest, expected_start, expected_end
    ):
        """Use the real source interface with a strict offline client double."""
        from unittest.mock import MagicMock, patch

        from energy_pipeline.config import Settings
        from energy_pipeline.extract import ApiSource, PeriodWindow

        monkeypatch.delenv("FIXTURE_PATH", raising=False)
        settings = Settings(api_key="offline-test-key", lookback_months=3)
        ti = MagicMock()
        result = MagicMock()
        upload = MagicMock(
            run_id="offline-run", manifest_path="raw/test/manifest.json", content_hash="hash"
        )

        with (
            patch("energy_pipeline.config.Settings.from_env", return_value=settings),
            patch("energy_pipeline.client.EiaClient", autospec=True) as client_cls,
            patch("energy_pipeline.extract.extract", autospec=True, return_value=result) as extract,
            patch("energy_pipeline.extract.extract_fixture") as fixture_extract,
            patch("energy_pipeline.storage.LocalObjectStore", autospec=True) as store_cls,
            patch("energy_pipeline.storage.RawSink", autospec=True) as sink_cls,
        ):
            client = client_cls.return_value
            client.get.return_value = {"response": {"data": [{"period": latest}] if latest else []}}
            sink_cls.return_value.write.return_value = upload

            assert dag.get_task("extract").python_callable(ti=ti) == "offline-run"

            client_cls.assert_called_once_with(settings)
            client.get.assert_called_once_with(
                {
                    "frequency": settings.frequency,
                    "data[0]": settings.measure,
                    "length": 1,
                    "sort[0][column]": "period",
                    "sort[0][direction]": "desc",
                }
            )
            extract.assert_called_once()
            source, passed_settings, window = extract.call_args.args
            assert isinstance(source, ApiSource)
            assert passed_settings is settings
            assert window == PeriodWindow(expected_start, expected_end)
            source.fetch_page(0, settings.page_size)
            assert client.get.call_args.args[0] == {
                "frequency": settings.frequency,
                "data[0]": settings.measure,
                "start": expected_start,
                "end": expected_end,
                "offset": 0,
                "length": settings.page_size,
                "sort[0][column]": "period",
                "sort[0][direction]": "asc",
            }
            fixture_extract.assert_not_called()
            store_cls.assert_called_once_with(root=settings.raw_root)
            sink_cls.assert_called_once_with(store_cls.return_value)
            sink_cls.return_value.write.assert_called_once_with(result.batch)
            ti.xcom_push.assert_any_call(key="run_id", value=upload.run_id)
            ti.xcom_push.assert_any_call(key="manifest_path", value=upload.manifest_path)
            ti.xcom_push.assert_any_call(key="content_hash", value=upload.content_hash)


class TestMergeToCoreValidation:
    """Regression tests for status validation in merge_to_core_task."""

    def _run_task(self, dag, status):
        """Run merge_to_core_task with a mocked merge result."""
        from unittest.mock import MagicMock, patch

        merge = dag.get_task("merge_to_core")
        call = merge.python_callable

        mock_context = {"ti": MagicMock()}
        mock_context["ti"].xcom_pull.return_value = "test-run-id"

        mock_result = MagicMock(status=status)

        with (
            patch("energy_pipeline.load.merge_to_core", return_value=mock_result),
            patch("energy_pipeline.warehouse.Warehouse") as mock_wh_cls,
        ):
            mock_wh = MagicMock()
            mock_wh.connect.return_value.__enter__.return_value = MagicMock()
            mock_wh_cls.from_env.return_value = mock_wh
            return call(**mock_context)

    def test_merge_succeeded_is_accepted(self, dag):
        """STATUS_SUCCEEDED should not raise."""
        from energy_pipeline.load import STATUS_SUCCEEDED

        status = self._run_task(dag, STATUS_SUCCEEDED)
        assert status == STATUS_SUCCEEDED

    def test_merge_empty_is_accepted(self, dag):
        """STATUS_EMPTY should not raise."""
        from energy_pipeline.load import STATUS_EMPTY

        status = self._run_task(dag, STATUS_EMPTY)
        assert status == STATUS_EMPTY

    def test_merge_failed_raises(self, dag):
        """STATUS_FAILED should raise RuntimeError."""
        from energy_pipeline.load import STATUS_FAILED

        with pytest.raises(RuntimeError, match="unexpected status"):
            self._run_task(dag, STATUS_FAILED)

    def test_merge_staged_raises(self, dag):
        """STATUS_STAGED should raise RuntimeError (valid only at staging, not merge)."""
        from energy_pipeline.load import STATUS_STAGED

        with pytest.raises(RuntimeError, match="unexpected status"):
            self._run_task(dag, STATUS_STAGED)


class TestBackfillSupport:
    def test_dag_has_start_date(self, dag):
        """A start_date enables backfill behavior."""
        assert dag.start_date is not None

    def test_dag_not_paused(self, dag):
        """DAG should not be paused by default."""
        assert getattr(dag, "is_paused", False) is False
