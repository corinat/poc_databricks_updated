"""This file configures pytest, initializes Databricks Connect, and provides fixtures for Spark and loading test data."""

import os, sys, pathlib
from contextlib import contextmanager


try:
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    from pyspark.sql import SparkSession
    import pytest
    import json
    import csv
    import os
except ImportError:
    raise ImportError(
        "Test dependencies not found.\n\nRun tests using 'uv run pytest'. See http://docs.astral.sh/uv to learn more about uv."
    )


@pytest.fixture()
def spark() -> SparkSession:
    """Provide a SparkSession fixture for tests.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    return DatabricksSession.builder.getOrCreate()


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Provide a callable to load JSON or CSV from fixtures/ directory.

    Example usage:

        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


def _compute_is_configured() -> bool:
    """Whether this environment already names compute for Databricks Connect.

    Compute is never enabled implicitly. Enabling it here would mean every
    pytest invocation starting a serverless session before collecting a single
    test, including the tests that touch no Spark at all, so a plain
    `uv run pytest` would block for minutes on a machine with no compute.

    To run the tests that need Spark, name it in your Databricks profile, for
    example

        serverless_compute_id = auto

    or set a cluster_id, or export SPARK_REMOTE. Without any of those, the
    tests that need a session skip themselves and the rest run in seconds.
    """
    try:
        conf = WorkspaceClient().config
    except Exception:
        # No usable profile. Nothing to connect to, so nothing to initialize.
        return False

    return bool(
        conf.serverless_compute_id
        or conf.cluster_id
        or os.environ.get("SPARK_REMOTE")
    )


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config):
    """Configure pytest session.

    The eager session is created only when compute is already configured. It is
    kept for that case because it makes the session available to a test that
    calls SparkSession.builder.getOrCreate() directly, and it validates version
    compatibility with the remote cluster up front rather than mid-test.

    With no compute configured this returns immediately and opens no
    connection. See _compute_is_configured.
    """
    if not _compute_is_configured():
        return

    with _allow_stderr_output(config):
        # For DB Connect 15+, validate version compatibility with the remote
        # cluster.
        if hasattr(DatabricksSession.builder, "validateSession"):
            DatabricksSession.builder.validateSession().getOrCreate()
        else:
            DatabricksSession.builder.getOrCreate()
