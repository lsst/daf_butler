#!/bin/sh
# Phase 1 must not change which tests exist, only how they are set up.
#
# Collects tests/test_butler.py and tests/test_datastore.py and diffs the node
# ids against the pre-migration set. Any output is a defect in the phase-1
# conversion: nothing is dropped, renamed or parametrized until phase 2.
set -e
here=$(dirname "$0")
.venv/bin/python -m pytest tests/test_butler.py tests/test_datastore.py \
    --collect-only -q 2>&1 | grep '::' | sort > /tmp/phase1_now.ids
if diff -u "$here/phase1_baseline_nodeids.txt" /tmp/phase1_now.ids; then
    echo "phase 1 intact: $(wc -l < /tmp/phase1_now.ids | tr -d ' ') node ids match the baseline"
else
    echo "PHASE 1 DRIFT: the node id set changed" >&2
    exit 1
fi
