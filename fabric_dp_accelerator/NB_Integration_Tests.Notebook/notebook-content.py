# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "69777b32-f6cf-4286-88c8-c96ec30a177c",
# META       "default_lakehouse_name": "metadata_lakehouse",
# META       "default_lakehouse_workspace_id": "76e43058-b99b-403a-8603-174631f16837",
# META       "known_lakehouses": [
# META         {
# META           "id": "69777b32-f6cf-4286-88c8-c96ec30a177c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip install pytest==8.4.2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import os
import glob
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

test_file_names = ""
metadata_warehouse_name = "metadata_warehouse"
metadata_warehouse_endpoint = ""
metadata_lakehouse_name = "metadata_lakehouse"
# Optional run isolation for local/agent runs. When run_id is set, files and
# JSON results live under Files/integration_tests/<run_id>/.
run_id = ""
batch_id = ""
# Optional comma-separated substring filter applied to fully-qualified test
# names (e.g. 'TestBatchProcessingE2EParallel.test_scenario_07', or simply
# 'scenario_07,scenario_28'). Empty = run every test discovered — backwards
# compatible with all existing callers.
test_filter = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Helper_Functions_1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Helper_Functions_2

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Helper_Functions_3

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run NB_Helper_Functions_Lineage

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get test files specified by the pipeline parameter (comma-separated).
# Filter out empty entries so an unset/blank parameter doesn't produce
# an invalid path like ".../integration_tests/" that open() would treat
# as a directory.
requested_files = [f.strip() for f in test_file_names.split(",") if f.strip()]
if not requested_files:
    raise ValueError(
        "No test files provided. Set the 'test_file_names' pipeline parameter "
        "to a comma-separated list of notebook filenames."
    )
# When run_id is provided, isolate this run's files in a per-run subfolder
# so concurrent batches don't trample each other.
_run_subfolder = f"/{run_id}" if run_id else ""
_test_base_path = f"/lakehouse/default/Files/integration_tests{_run_subfolder}"
test_files_paths = [f"{_test_base_path}/{f}" for f in requested_files]

print(f"🎯 Running {len(test_files_paths)} test files")
print(f"   run_id   : {run_id or '(none)'}")
print(f"   batch_id : {batch_id or '(none)'}")
print(f"   Files    : {requested_files}")

for test_file_path in test_files_paths:
    with open(test_file_path, 'r') as test_file_handle:
        content = test_file_handle.read()

    print(f"\n🔄 Instantiating test functions in {test_file_path}")
    print("-" * 40)

    # Execute the test code in the current notebook namespace.
    ipython = get_ipython()
    result = ipython.run_cell(content)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create a test suite
loader = unittest.TestLoader()
suite = unittest.TestSuite()

# Get all objects from the current global namespace
# FIXED: Create a list copy to avoid "dictionary changed size during iteration" error
current_globals = list(globals().items())

# Find all TestCase classes
test_classes = []
for name, obj in current_globals:
    # Check if it's a class and inherits from unittest.TestCase
    if (isinstance(obj, type) and 
        issubclass(obj, unittest.TestCase) and 
        obj is not unittest.TestCase):
        test_classes.append((name, obj))

# Sort by name for consistent ordering
test_classes.sort(key=lambda x: x[0])

# Optional substring filter. When test_filter is empty, run every test in
# every discovered TestCase — default behaviour. When test_filter is set,
# only test methods whose fully-qualified name (`ClassName.method_name`)
# contains at least one of the comma-separated substrings are added.
_filters = [token.strip() for token in (test_filter or "").split(",") if token.strip()]
if _filters:
    print(f"⚡ Filter active. Only tests matching one of {_filters} will run.")

# Add all test classes to the suite
print(f"Found {len(test_classes)} test classes:")
_total_added = 0
for name, test_class in test_classes:
    discovered = loader.loadTestsFromTestCase(test_class)
    if not _filters:
        suite.addTests(discovered)
        method_count = discovered.countTestCases()
        _total_added += method_count
        print(f"  - {name} ({method_count} tests)")
        continue
    # Walk the discovered TestSuite and keep only matching test methods.
    kept_methods = []
    for test_case in discovered:
        # _testMethodName is the standard unittest attribute; using it avoids
        # a brittle parse of the str(test_case) representation.
        method_name = getattr(test_case, "_testMethodName", "")
        fully_qualified = f"{name}.{method_name}"
        if any(token in fully_qualified for token in _filters):
            kept_methods.append(test_case)
    if kept_methods:
        for tc in kept_methods:
            suite.addTest(tc)
        _total_added += len(kept_methods)
        print(f"  - {name} ({len(kept_methods)} of {discovered.countTestCases()} tests after filter)")
    else:
        print(f"  - {name} (skipped, no tests matched filter)")

if _filters and _total_added == 0:
    raise RuntimeError(
        f"test_filter={test_filter!r} matched no tests across {len(test_classes)} classes. "
        f"Either remove the filter or pick a substring that matches a real test name."
    )

print(f"\n{'='*70}")
print(f"Running all tests...")
print(f"{'='*70}\n")

# Run the tests
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)

# Print summary
print(f"\n{'='*70}")
print(f"SUMMARY")
print(f"{'='*70}")
print(f"Tests run: {result.testsRun}")
print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
print(f"Failures: {len(result.failures)}")
print(f"Errors: {len(result.errors)}")
print(f"{'='*70}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Get full details of failures and errors
tests_with_exceptions = []
exceptions = []
if result.failures:
    print("\n" + "="*70)
    print("FAILURES:")
    print("="*70)
    for test, traceback in result.failures:
        print(f"\nTest: {test}")
        print(traceback)
        exceptions.append((test, traceback))
        tests_with_exceptions.append(test)

if result.errors:
    print("\n" + "="*70)
    print("ERRORS:")
    print("="*70)
    for test, traceback in result.errors:
        print(f"\nTest: {test}")
        print(traceback)
        exceptions.append((test, traceback))
        tests_with_exceptions.append(test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Write structured results for run_local_tests.py. Legacy DevOps callers do not
# pass run_id, so their behavior is unchanged.
if run_id:
    try:
        _results_dir = f"/lakehouse/default/Files/integration_tests/{run_id}"
        os.makedirs(_results_dir, exist_ok=True)
        _safe_batch = batch_id or "0"
        _results_path = f"{_results_dir}/results_batch_{_safe_batch}.json"
        _payload = {
            "run_id": run_id,
            "batch_id": _safe_batch,
            "test_files": requested_files,
            "tests_run": result.testsRun,
            "successes": result.testsRun - len(result.failures) - len(result.errors),
            "failures": [{"test": str(t), "traceback": tb} for t, tb in result.failures],
            "errors": [{"test": str(t), "traceback": tb} for t, tb in result.errors],
        }
        with open(_results_path, "w") as _f:
            json.dump(_payload, _f, indent=2)
        print(f"\n📄 Wrote results to {_results_path}")
    except Exception as _e:
        print(f"\n⚠️ Failed to write results JSON: {_e!r}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Always raise on test failures so the Fabric job status reflects reality.
# Local/agent runs (run_id set) additionally have the structured JSON payload
# above for richer reporting; the runner trusts the JSON over the Fabric job
# status, so raising here doesn't break it.
if exceptions:
    detailed_error = f"\n{len(tests_with_exceptions)} test(s) failed:\n" + "\n".join(str(t) for t in tests_with_exceptions)
    raise Exception(detailed_error)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
