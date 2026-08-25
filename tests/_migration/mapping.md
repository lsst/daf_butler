# DM-55822 test mapping

Migration scaffolding. Deleted before merge, after being pasted into the ticket.

The diff is too large to review by reading, and test count cannot serve as a
check because deduplication reduces it deliberately.
This file records what happened to every test in the pre-migration
`tests/test_butler.py` and `tests/test_datastore.py`, so a reviewer can check
the dropped list against the coverage evidence.

The coverage gate proves nothing was lost.
This file explains why each removal was safe.

## Conventions

- **Moved:** new nodeid, when a test simply changed file or name.
- **Parametrized:** new nodeid with the parameter set, when several old nodeids
  collapse into one function.
- **Dropped:** `dropped: <axis> — marginal set empty (N lines, N arcs checked)`,
  citing the `coverage_tool.py marginal` output that justified it.
- **Findings:** anything noticed but deliberately not fixed here, because no
  library change may ride on this branch.

## Renames and behavior notes

| Item | Note |
| --- | --- |
| `ButlerPutGetTests.addDatasetType` | now `fixtures.add_dataset_type` |
| `ButlerPutGetTests.create_butler(storageClass, datasetTypeName)` | now `ButlerHarness.create_butler(storage_class, dataset_type_name)` |
| `ButlerTests.default_run` | now `fixtures.DEFAULT_RUN`, value unchanged (`ingésτ😺`) |
| `ClonedSqliteButlerTestCase.create_butler` | called `butler.clone(run=run)` without metrics, while `ClonedPostgresPosixDatastoreButlerTestCase` passed `metrics=metrics`. `ClonedButlerHarness` uses the postgres form for both. A pre-existing inconsistency rather than a change this branch is making; the gate confirms it costs no coverage. |
| `ButlerServerTests.postgres` class attribute | replaced by the `registry_backend` axis |
| `DatastoreCacheTestCase.assertCache` | now module-level `_assert_cache(cache_manager, cache)` |
| `DatastoreCacheTestCase.assertExpiration` | now module-level `_assert_expiration(cache_manager, cache, n_datasets, n_retained)` |
| `DatastoreCacheTestCase.setUpClass` storage classes | now the `cache_storage_class_factory` fixture, named distinctly from the plugin's `storage_class_factory` because it loads `storageClasses.yaml` rather than the Butler configs |
| `DatastoreCacheTestCase.setUp`/`tearDown` | now the `cache` fixture returning a `CacheFixtures` dataclass; `tempfile.mkdtemp()` plus manual `shutil.rmtree` replaced by `tmp_path` |
| `PostgresPosixDatastoreButlerTestCase.setUp` temp yaml | the original wrote the postgres-patched config to a `NamedTemporaryFile` and re-read it, because `setUp` could only communicate through `self.configFile`. The fixture passes the patched `Config` to `make_repo_for_test` directly, which is equivalent and leaves no temp file behind. |

## Findings for separate tickets

| Finding | Where | Why not fixed here |
| --- | --- | --- |
| `useTempRoot` is a dead class attribute | `tests/test_butler.py:625` sets it `True`, `:2697` sets it `False`, and nothing ever reads it | Dropped rather than ported. Test-only, so no library change and no ticket needed. |
| `self.id = 0` is dead state | `DatastoreCacheTestCase.setUp` assigned it and nothing read it; `DatasetTestHelper.makeDatasetRef` does not use instance state | Dropped rather than ported. Test-only. |

## Test mapping

| Original nodeid | New nodeid(s) or disposition |
| --- | --- |
| `tests/test_datastore.py::DatastoreCacheTestCase::testNoCacheDir` | `tests/test_datastore_cache.py::test_no_cache_dir` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testNoCacheDirReversed` | `tests/test_datastore_cache.py::test_no_cache_dir_reversed` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testEnvvarCacheDir` | `tests/test_datastore_cache.py::test_envvar_cache_dir` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testExplicitCacheDir` | `tests/test_datastore_cache.py::test_explicit_cache_dir` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testUnexpectedFilesInCacheDir` | `tests/test_datastore_cache.py::test_unexpected_files_in_cache_dir` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testNoCache` | `tests/test_datastore_cache.py::test_no_cache` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testCacheExpiryFiles` | `tests/test_datastore_cache.py::test_cache_expiry_files` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testCacheExpiryDatasets` | `tests/test_datastore_cache.py::test_cache_expiry_datasets` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testCacheExpiryDatasetsFromDisabled` | `tests/test_datastore_cache.py::test_cache_expiry_datasets_from_disabled` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testExpirationModeOverride` | `tests/test_datastore_cache.py::test_expiration_mode_override` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testMissingThreshold` | `tests/test_datastore_cache.py::test_missing_threshold` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testCacheExpiryDatasetsComposite` | `tests/test_datastore_cache.py::test_cache_expiry_datasets_composite` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testCacheExpirySize` | `tests/test_datastore_cache.py::test_cache_expiry_size` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testDisabledCache` | `tests/test_datastore_cache.py::test_disabled_cache` |
| `tests/test_datastore.py::DatastoreCacheTestCase::testCacheExpiryAge` | `tests/test_datastore_cache.py::test_cache_expiry_age` |
