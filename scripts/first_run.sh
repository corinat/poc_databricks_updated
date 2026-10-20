#!/usr/bin/env bash
#
# Deploys the bundle and runs every flow in dependency order, on a workspace
# where the tables do not exist yet.
#
# Usage:  scripts/first_run.sh [target]      (target defaults to dev)
#
# Two things make this more than a list of `bundle run` calls.
#
# Two jobs cannot be created until the tables they watch exist. A table update
# trigger is validated when the job is created, so `build_silver_exchange_rate`
# needs `bronze_currency` and `build_gold_marts` needs the seven star-schema
# tables. The first deploy therefore fails for those two and succeeds for
# everything else, which is why this script deploys three times: once at the
# start, once after the tables that `build_silver_exchange_rate` watches exist,
# and once after `fact_exchange_rate` completes the seven.
#
# The file arrival jobs are run explicitly. Their triggers fire when a file
# appears, so they fire on an upload into a volume that already has the job
# watching it. On a rebuild the data is already in place and no file arrives,
# so nothing would start them.
#
# What this script does not do is drop anything. Resetting means dropping 29
# tables and deleting the Auto Loader state in the volume, and that belongs in
# a statement you run deliberately, not in a script you might run by habit.
# See the reset section of the project README.

set -u -o pipefail

TARGET="${1:-dev}"

step() { printf '\n\033[1m── %s\033[0m\n' "$*"; }

die() {
    printf '\n\033[1m%s\033[0m\n' "$1" >&2
    shift
    for line in "$@"; do printf '  %s\n' "$line" >&2; done
    exit 1
}

# A deploy is tolerant while a job's trigger still names a table that does not
# exist, and strict once none of them do. There are two such deploys before the
# last one, because the two triggered jobs become creatable at different
# points: build_silver_exchange_rate once bronze_currency exists, and
# build_gold_marts only after build_silver_exchange_rate has run and produced
# fact_exchange_rate.
#
# A tolerant deploy hides a real failure, so the run that follows it is what
# checks the job was actually created: `bundle run` on a job that does not
# exist fails, and run() stops the script.
deploy_partial() {
    step "deploy ($1)"
    databricks bundle deploy -t "$TARGET" || true
}

deploy_strict() {
    step "deploy ($1)"
    databricks bundle deploy -t "$TARGET" || die \
        "deploy failed, so the job it was meant to create does not exist." \
        "Nothing after this point can run. The error above is the reason."
}

# The steps are a dependency chain, so a failure stops the script rather than
# producing a cascade of failures that buries the one error worth reading.
run() {
    step "run $1"
    databricks bundle run "$@" -t "$TARGET" || die \
        "$1 failed, and the steps after it read what it writes. Stopping here." \
        "The error above is the reason. The most common one on a fresh" \
        "workspace is an empty volume: the deploy creates the volume, but the" \
        "input files are uploaded by hand, and a load that finds no file fails" \
        "on the path. See the First run section of the project README."
}

# ── 1. Everything except the two jobs whose triggers need existing tables ──
deploy_partial "both triggered jobs are expected to fail here"

# ── 2. The loads that create the dimensions, in dependency order ──
# silver_order's quality gate reads silver_customer, bronze_product and
# bronze_currency, so those three come first and it fails loudly if they are
# empty.
run load_bronze_currency
run load_bronze_product
run load_bronze_customer
run load_bronze_seller

# ── 3. Orders, then everything that reads silver_order ──
# load_historical=true is required on a first run and is not the default: it is
# what loads orders.parquet, the thirty-order backfill.
run load_order --params load_historical=true
run load_order_status
run load_bronze_invoice

# ── 4. bronze_currency exists now, so the exchange rate job can be created ──
deploy_partial "creates build_silver_exchange_rate; build_gold_marts still cannot be created"
run build_silver_exchange_rate

# ── 5. The seven star-schema tables exist now, so the mart job can be created ──
deploy_strict "creates build_gold_marts"
run build_gold_marts

step "done"
cat <<'EOF'
Expected end state:

  silver_dq_quarantine   2 rows, both from orders_stream_bad_data.parquet
  fact_order             30 orders
  the four marts         30 / 24 / 17 / 120 rows
  both alerts            triggered, and a notification for each

The alerts are red by design: every order is dated January 2026, so today has
none, and the bad-data file is there so a run produces a real quarantine row.
EOF
