# Tests

Eight tests over the four transformations in
[src/common/transforms.py](../src/common/transforms.py). What each one covers
and why it is worth having is in [Tests](../README.md#tests) in the project
README; this file is about running them.

They are optional. No requirement asks for tests and no deliverable depends on
them.

| File | What it is |
|---|---|
| `test_transforms.py` | the eight tests |
| `conftest.py` | the bundle template's scaffolding, unmodified. It provides a `spark` fixture from Databricks Connect and opens the session before collection. `test_transforms.py` defines its own `spark` fixture, so it runs with or without this one. |

## Locally, with no workspace

The way to run them with nothing deployed and no compute. Verified in this form.

```bash
export JAVA_HOME=/path/to/jdk-17        # 17 or 21
uv run --no-project --with pyspark --with pytest \
    pytest tests/test_transforms.py -q --noconftest
```

```
........                                                      [100%]
8 passed in 31s
```

Four things that command is particular about:

**A JDK 17 or 21.** Spark 4 does not run on a newer one — JDK 26 fails at
startup with `ClassNotFoundException` on `jdk.internal.ref.Cleaner`, a class
those JDKs no longer have. `/usr/libexec/java_home -V` lists what is installed
on macOS. Set `JAVA_HOME` the way your shell does: `export` in bash or zsh,
`$env:JAVA_HOME = "..."` in PowerShell, `set JAVA_HOME=...` in cmd.

**`--noconftest`.** `conftest.py` imports Databricks Connect and opens a
session before collection, so without the flag the run waits on compute it does
not need.

**`--no-project`.** `uv` builds a throwaway environment with OSS pyspark. The
project's own environment has `databricks-connect`, which replaces pyspark and
refuses a local session.

**No virtual environment active.** `databricks` is a namespace package, so a
`VIRTUAL_ENV` pointing at an environment that has `databricks-connect` leaves it
on `sys.path` even under `--no-project`, where it collides with the pyspark
installed alongside it. If the run fails with `cannot import name
'PythonUDFEnvironment'`, that is what happened; prefix the command with
`env -u VIRTUAL_ENV`.

Only `pyspark` and `pytest` are needed. The transformations import neither DQX
nor pandas.

## On the workspace, with no JDK

Spark runs in Databricks instead of locally, so nothing is installed here. Add
one line to the profile the bundle target uses, in `~/.databrickscfg`:

```ini
[dev]
serverless_compute_id = auto
```

Then no flags:

```bash
uv run pytest tests/test_transforms.py -v
```

This pays a serverless start-up on every run and uses compute, and it cannot
run without a workspace — so no offline, and no CI without credentials. It needs
nothing deployed: the tests build their inputs in memory and read no table.

Two deprecation warnings appear in this form and not in the other. One is
pyspark calling `logging.warn`, the other is `jupyter_client` migrating to
platformdirs, pulled in by the `ipykernel` dev dependency. Neither comes from
this project and neither is actionable here. `--disable-warnings` hides the
summary if the noise is in the way.

## If the tests skip

The `spark` fixture tries a local session, falls back to Databricks Connect, and
skips if it can have neither, with the reason in the skip message:

```
8 skipped
SKIPPED [1] no Spark session available: Cluster id or serverless are required
```

That means no JDK for a local session and no compute named in the profile.
Either is enough; neither is configured.

## In CI

[.github/workflows/tests.yml](../.github/workflows/tests.yml) runs the suite on
every pull request into `main`, and on pushes to `main`.

It runs the command from [Locally, with no workspace](#locally-with-no-workspace),
with `-v` in place of `-q` so the log names each test, and the JDK pinned to 17.
Nothing else is substituted and no dependency is pinned, so a green run is
evidence that the documented command still works rather than only that some
CI-only variant of it does.
The workflow needs no credentials: these tests reach no workspace.

Two differences from a local run, neither of them a problem:

- The `jupyter_core` warning does not appear. It comes from this repository's
  `.venv`, which is gitignored and therefore absent on the runner.
- `uv` supplies its own Python, so the runner's Python version does not matter
  and there is no `setup-python` step.
