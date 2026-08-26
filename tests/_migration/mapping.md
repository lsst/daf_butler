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
| `ButlerPutGetTests.runPutGetTest` | now `butler_test_support.run_put_get_test(harness, storage_class, dataset_type_name)`. `tests/test_butler.py` keeps a one-line `runPutGetTest` that forwards to it, so the classes still awaiting conversion share the single copy. |
| `ButlerPutGetTests.assertGetComponents` | now `butler_test_support.assert_get_components`, forwarded the same way |
| `ButlerPutGetTests.storageClassFactory` | the shim adds a `storage_class_factory` property so `self` satisfies the part of `ButlerHarness` the helper uses |
| `runPutGetTest`'s `self.subTest(args=...)` | dropped; the loop identity moved into the assertion messages (`f"put with args {args!r}"`). Subtest count for these tests falls from 263 to 0, which is why the reported subtest total drops. |
| `ChainedDatastoreButlerTestCase.testComponentFromOverriddenStorageClassWarns` | the empty override became an early `return` guarded on `datastore_type == "chained"`, so the execution is conserved. Task 21 may drop the axis with coverage evidence. |
| `FileDatastoreButlerTests.runImportExportTest` | now module-level `_run_import_export_test(butler_harness, storage_class_name, test_directory)`; it takes the storage class by name because both callers looked it up from the factory. |
| `FileDatastoreButlerTests.checkFileExists` | now module-level `_check_file_exists`, private to `tests/test_butler_import_export.py`, its only caller. |
| `FileDatastoreButlerTests.remove_dataset_out_of_band` and the `ButlerServerTests` override | already present as `ButlerHarness.remove_dataset_out_of_band` and `ServerButlerHarness`'s override; both copies deleted rather than ported. |
| `runImportExportTest`'s `self.subTest(ref=repr(ref))` | dropped; the ref moved into the assertion message. The first missing dataset now ends the loop instead of reporting all of them, which is accepted because the loop is inside one test either way. |
| `testImportExportVirtualComposite`'s `@unittest.expectedFailure` | now `@pytest.mark.xfail` without `raises=`, matching `expectedFailure`'s any-exception behavior. The observed cause on the posix axis is `NotImplementedError("Can not export disassembled datasets ...")`. `xfail_strict` is on, so an unexpected pass still fails. |
| `ChainedDatastoreButlerTestCase.testPruneDatasets` | the empty override became an early `return` guarded on `datastore_type == "chained"`, so the execution is conserved. Task 21 may drop the axis with coverage evidence. |
| `testPruneDatasets`'s `butler._datastore` accesses | narrowed with `cast(FileDatastore, ...)` inside the trust-mode block. The original went unchecked only because `create_empty_butler` had no return annotation and so returned `Any`. |
| `ButlerTests._setup_to_test_collection_chain`, `._check_chain`, `._test_common_chain_functionality` | now module-level `_setup_to_test_collection_chain(butler_harness)`, `_check_chain(butler, expected)` and `_check_common_chain_functionality(...)`. The last was renamed from `_test_` because pytest collects anything named `test_*` at module level. |
| `ButlerServerTests.testGetDatasetTypes` | the empty override became an early `return` guarded on `butler_client == "server"`, so the execution is conserved. Task 22 may drop the axes with coverage evidence. |
| `ButlerTests.validationCanFail` | now `butler_harness.profile.validation_can_fail`. |
| `testGetDatasetTypes`'s `len(butler.registry.queryDatasetTypes("metric*"))` | the result is materialized into a `list` first. `queryDatasetTypes` is annotated `Iterable[DatasetType]`, so the `len` only typechecked before because `create_empty_butler` returned `Any`. |
| `InMemoryDatastoreButlerTestCase.testIngest` and `.test_ingest_zip` | the empty overrides became excluded axis values: the ingest tests use `FILE_DATASTORE_AXES`, which is `BUTLER_TESTS_AXES` without the two ephemeral entries. **This drops 4 executions, not the 2 the plan predicted** — `ClonedSqliteButlerTestCase` inherits both overrides from `InMemoryDatastoreButlerTestCase`, so two classes carried them. The collected count for `tests/test_butler*.py` goes 363 to 359 here, and 359 is the reference from Task 10 onward. |
| `test_temporary_for_ingest` and `test_specialized_file_datasets_functions` | the plan called them posix-only; they in fact ran under `ButlerExplicitRootTestCase` too, so they carry a `repo_layout` parametrization rather than no parametrize. |
| `testIngest`'s `try/except AttributeError` around `getStoredItemsInfo` | now `contextlib.suppress(AttributeError)`, which is what `SIM105` requires and what the block meant. |
| `test_specialized_file_datasets_functions`'s `Butler(target_repo_config, writeable=True)` | now `Butler.from_config(...)`. The test is not exercising the constructor, and `Butler` is abstract as far as mypy is concerned. |
| `repo.addDataset(repo.ref1.dataId, ...)` | now passes `dict(repo.ref1.dataId.required)`. `MetricTestRepo.addDataset` is annotated `dict[str, Any]` but forwards straight to `Butler.put`, which takes any data ID. |
| `clean_environment` and `setup_module` | now a module-scoped autouse `_clean_environment` fixture in `tests/test_butler_lifecycle.py`, which saves and restores the variable rather than only popping it. Only this file reads `DAF_BUTLER_REPOSITORY_INDEX`; the other split files inherited the old module-level cleanup but never depended on it. |
| `TransactionTestError` | moved verbatim into `tests/test_butler_lifecycle.py`, its only user. |
| `ButlerServerTests.testPickle`'s `@unittest.expectedFailure` | now a per-parameter `pytest.mark.xfail` built by `PICKLE_AXES`, so only the two server axes are expected to fail while the other eight must pass. |
| `ButlerServerTests.testConstructor`, `.testDafButlerRepositories`, `.testTransaction` | the empty overrides became early `return`s guarded on `butler_client == "server"`. `.testStringification` had its own assertion, so it became a branch in the one function. |
| `ButlerTests.registryStr` | derived in the test from `registry_backend` rather than carried on `DatastoreProfile`: it described the registry backend, not the datastore. |
| `testClose` and `testGarbageCollection`'s `is_direct_butler` flag | narrowed with `isinstance` instead, since mypy cannot narrow through a bool. In `test_garbage_collection` the narrowing is deliberately inline rather than a second name, so no extra strong reference outlives the `del`. |
| `testPickle`'s `assertIsInstance(butlerOut, Butler)` | now asserts `DirectButler`, which the very next line already assumed by reading `_config`. |
| `testTransaction`'s `pytest.raises` block | carries `# noqa: PT012`. The block is inherently multi-statement: the test exists to show that everything inside the transaction rolls back. |
| `StoredFileInfoTestCase.storageClassFactory` class attribute and `DatasetTestHelper` base | now a local `StorageClassFactory()` and `DatasetTestHelper()` inside the one test that needs them, matching what `tests/test_datastore_cache.py` already does. |
| `DatasetRefURIsTestCase.testSequenceAccess`'s item assignments | carry `# type: ignore[index]`. The assignments are the point of the test: `DatasetRefURIs` rejects them at run time, and mypy rejects them statically for the same reason. |
| `NullDatastoreTestCase.test_basics`'s `null.validateConfiguration(ref)` | now passes `[ref]`. The parameter is `Iterable[DatasetRef | DatasetType | StorageClass]` and a `DatasetRef` is not iterable; the call only ever worked because `NullDatastore.validateConfiguration` is `pass`. |
| `NullDatastoreTestCase.test_basics`'s `null.transfer_from(null, [ref])` | now passes `{}` as the first argument. `transfer_from` takes a `FileTransferMap`, not a source datastore; the call only ever worked because the body raises `NotImplementedError` before looking at it. |
| The five concrete constraint classes | now one `CONSTRAINT_DATASTORES` list of `(config file, can ingest, needs a root)`, reproducing every current combination. |
| `DatastoreConstraintsTests.testConstraints`'s `subTest` loop | now the `CONSTRAINT_CASES` parametrize list, and `ChainedDatastorePerStoreConstraintsTests`'s is `PER_STORE_CASES`. This is why the collected count rises: **194 to 213**, a rise of 19, which is 5 classes x 4 cases minus the 5 methods they replace, plus 5 cases minus the 1 method. Subtests fall from 104 to 79, exactly the 25 new tests. |
| `DatastoreTestsBase.setUpClass`/`setUpDatastoreTests`/`makeDatastore` | collapsed into a local `_make_datastore(config_file, root)`. It reproduces the same three steps: import the datastore class named in the config, apply `setConfigRoot` when the layout needs a root, and build from a copy of the config against a fresh `DummyRegistry`. |
| `tests/test_datastore.py::makeExampleMetrics` | now `butler_test_support.make_datastore_metrics`. Deliberately not `fixtures.make_example_metrics`: the datastore tests use a different data array and one needs the array absent. |
| `PosixDatastoreConstraintsTestCase.setUp`'s `tempfile.mkdtemp()` | now pytest's `tmp_path_factory`, which cleans up on its own; the old `tearDown` did the `shutil.rmtree`. |
| The seven concrete datastore classes | now `DatastoreTestProfile` and the `PROFILES` dict, one entry per class. `trash` and `posix-no-checksums` were subclasses of the posix case and so rerun every shared test; Task 21 reduces that with coverage evidence. |
| `DatastoreTests.canIngestNoTransferAuto` on the ephemeral classes | it was never defined there. `testIngestNoTransfer` reads it only after `"auto" in self.ingestTransferModes`, which is `False` for an empty tuple, so Python's short-circuit kept it from ever being evaluated. The profile gives it a default and says so. |
| `testDisassembly`, `testIngestNoTransfer` and `testIngestTransfer` subTest loops | now parametrize lists (6, 2 and 7 cases). This is why the collected count rises: **213 to 285**, a rise of 72, which is (6-1) + (2-1) + (7-1) cases across 6 profiles. |
| `testIngestNoTransfer`'s `continue` for chained | now `pytest.skip("Datastore supports auto but cannot transfer in place.")`. This is the one legitimate rise in the skip count, from 30 to 31 across the suite. |
| `DatastoreTests`'s two `raise unittest.SkipTest` calls | now `pytest.skip`, with the same message and the same 8 executions skipped. |
| `PosixDatastoreTestCase.setUp`'s `os.path.realpath` | kept, on the `ds` fixture's temporary root. The original comment explains why: on macOS a temporary file can be under either `/var/folders` or `/private/var/folders`, and `relsymlink` cannot traverse between the two forms. |
| `testBasicTransaction` and `testNestedTransaction`'s `pytest.raises` blocks | carry `# noqa: PT012`, like `test_transaction` in the butler lifecycle file: the block is inherently multi-statement because the test exists to show the transaction rolls back. |
| `ButlerTests.testMakeRepo`'s `if self.fullConfigKey is None: return` | now reads `butler_harness.profile.full_config_key`, which `_make_explicit_root_repo` already overrides to `None` for the explicit-root layout, so that axis still no-ops as it did. |
| `PostgresPosixDatastoreButlerTestCase.testMakeRepo`'s `raise unittest.SkipTest` | now `pytest.skip` guarded on `registry_backend == "postgres"`, still reported as 2 skips. |
| `ButlerServerTests.testMakeRepo` and `.testPutTemplates` | the empty overrides became early `return`s guarded on `butler_client == "server"`, so the executions are conserved. Task 22 may drop the axes with coverage evidence. |
| `ButlerMakeRepoOutfileDirTestCase.testConfigExistence`'s `self.tmpConfigFile = os.path.join(...)` | the override appended `butler.yaml` to the directory before calling `super()`; now a branch on `repo_layout == "outfile_dir"` inside the one test. |
| `testPutTemplates`'s inner `assertLogs` inside `assertRaises(KeyError)` | dropped. The `KeyError` propagates out of the `assertLogs` context, so `assertLogs` never checked anything; only the `pytest.raises` survives. |
| `DatastoreCacheTestCase.assertCache` | now module-level `_assert_cache(cache_manager, cache)` |
| `DatastoreCacheTestCase.assertExpiration` | now module-level `_assert_expiration(cache_manager, cache, n_datasets, n_retained)` |
| `DatastoreCacheTestCase.setUpClass` storage classes | now the `cache_storage_class_factory` fixture, named distinctly from the plugin's `storage_class_factory` because it loads `storageClasses.yaml` rather than the Butler configs |
| `NullDatastoreTestCase.setUpClass`/`setUp`/`tearDown` | replaced wholesale by the plugin's `butler_repo` and `storage_class_factory` fixtures; the class built exactly the default axis combination (sqlite, posix, in_repo) by hand |
| `DatastoreTransfers.create_butler` | now `TransferHarness.create_butler`, kept local to `tests/test_butler_transfers.py`. Unrelated to `ButlerHarness.create_butler`: it builds a repo pair with a chosen dataset record storage manager. |
| `DatastoreTransfers.assertButlerTransfers` | now module-level `_assert_butler_transfers(tr, ...)` |
| `makeExampleMetrics`, `_get_test_data_path` | moved to `fixtures.make_example_metrics` and `fixtures.get_test_data_path`, since several split files need them |
| `fixtures.TestRepo` | renamed `fixtures.ButlerRepo`. pytest tries to collect anything named `Test*` as a test class and warns that it cannot because the dataclass has a constructor. The warning appeared in every file importing it. |
| `DatastoreCacheTestCase.setUp`/`tearDown` | now the `cache` fixture returning a `CacheFixtures` dataclass; `tempfile.mkdtemp()` plus manual `shutil.rmtree` replaced by `tmp_path` |
| `PostgresPosixDatastoreButlerTestCase.setUp` temp yaml | the original wrote the postgres-patched config to a `NamedTemporaryFile` and re-read it, because `setUp` could only communicate through `self.configFile`. The fixture passes the patched `Config` to `make_repo_for_test` directly, which is equivalent and leaves no temp file behind. |

## Deviations from the plan

| Deviation | Why |
| --- | --- |
| Task 11 executed before Task 7 | Task 7's `ButlerMakeRepoOutfile*` classes call `runPutGetTest`, which Task 11 is the task that extracts. Task 7 could not be written without it. Both tasks still hold the collected count at 363. |
| Axis lists and shared assertions live in `tests/butler_test_support.py`, not `tests/conftest.py` | The plan offered `conftest.py` "so nothing imports across test modules", but a `parametrize` list has to be importable at collection time, which a fixture cannot supply, so a cross-module import happens either way. A named module keeps `conftest.py` for configuration and makes the import obvious. `conftest.py` calls `pytest.register_assert_rewrite` on it so its assertions still report values. |
| `conftest.py` does not import `lsst.daf.butler.tests.fixtures` at module level | Importing the plugin from `conftest.py` beats pytest to it and raises `PytestAssertRewriteWarning: Module already imported so cannot be rewritten` on every run, silently disabling assertion rewriting inside the shipped fixture module. |

## The conversion gate, and why the baseline had to be retaken

The first run of Task 17's gate against `~/dm55822/baseline.coverage` reported
419 lost lines and 1093 lost arcs.
Almost all of it was an artifact, and the residue was one real defect.

**The branch was rebased after the baseline was recorded.**
The baseline commit named in this directory's README, `de89a4fce`, is not an
ancestor of the current branch; `a8421a2fa` is its rewritten form.
The rebase pulled in two upstream commits, `0ee9a6323` and `99acbecaf`, which
between them changed seven library files.
The gate compares line *numbers*, so every covered line in those files moved and
was reported as both lost and gained:

| File | Lost lines | Gained lines |
| --- | --- | --- |
| `datastore/stored_file_info.py` | 100 | 95 |
| `dimensions/_elements.py` | 91 | 91 |
| `column_spec.py` | 62 | 53 |
| `dimensions/_schema.py` | 47 | 47 |
| `dimensions/_record_table.py` | 43 | 40 |
| `remote_butler/server/handlers/_query_streaming.py` | 42 | 34 |
| `datastore/record_data.py` | 28 | 23 |
| `version.py` | 5 | 1 |

Excluding those eight files leaves **one lost line and three lost arcs**, which
is what the gate was actually for.

**`dimensions/_config.py`, arc 138 to 134: not a real loss.**
That arc is the branch where a search path does not contain the config file and
the loop tries the next one.
The baseline attributed it to a long list of tests including
`tests/test_astropyTableFormatter.py`, which this migration does not touch.
Run on its own against the current tree that test still covers `138 -> 139` and
still does not cover `138 -> 134`, so the arc belongs to no test: whether the
loop takes a second iteration depends on state accumulated across the session,
and the suite's file layout changed. A coverage flake, not a dropped path.

**`datastore/cache_manager.py:1232`: a real loss, and the reason this gate
exists.**
That line is `DatastoreDisabledCacheManager.__str__`, and the baseline query
named exactly one covering test,
`tests/test_datastore.py::DatastoreCacheTestCase::testNoCache`.
Its assertion was `self.assertIsNone(found, msg=f"{cache_manager}")`.
`unittest` builds the message eagerly, so the f-string ran on every iteration
whether or not the assertion failed, and that was the only caller of `__str__`
in the whole suite.
A bare `assert found is None, f"{cache_manager}"` formats the message *only on
failure*, so the call disappeared.

This is a property of the `PT009` codemod, not of one test: every
`self.assertX(..., msg=...)` it rewrites becomes lazy.
The fix keeps the pytest idiom and restores the original evaluation order by
computing the message before the loop.
The other converted site that formats a bare object,
`test_cache_expiry_datasets_from_disabled`, needs nothing: it already calls
`str(cache_manager)` explicitly on the next line.

**`coverage_tool.py` now stores paths relative to the package root.**
`_load` applied `_relative` only in `summary`, so `gate` compared absolute
paths and could not compare two checkouts of the same source.
The change is not a way of making the gate pass: with it applied, the gate
still reports the same 419 and 1093 against the stale baseline, and
`summary` still reproduces `baseline_summary.json` exactly.

**The corrected baseline** is taken from `a5f25a878`, whose library is identical
to the branch head apart from the added `fixtures.py`, and which still has the
original `tests/test_butler.py` and `tests/test_datastore.py`.
It reports `2060 passed, 30 skipped, 10 xfailed, 1440 subtests`, one more pass
than the original baseline's 2059, which is the test the two upstream commits
added.

Gating the pre-fix conversion against it gives the picture the stale database
hid: **37077 baseline lines against 37077 new lines**, and a lost set of exactly
one line and two arcs, all of them `cache_manager.py:1232`.
The `_config.py` arc is not lost against this baseline at all, which confirms it
was an artifact of the stale database's session rather than a dropped path.

**With the fix applied the gate passes.**

| | Corrected baseline | Post-conversion | Lost |
| --- | --- | --- | --- |
| Lines | 37077 | 37078 | 0 |
| Arcs | 54830 | 54832 | 0 |

The single gained line is `datastore/_datastore.py:463`, `Datastore.__repr__`,
which the converted suite reaches and the original did not.

Skips go from 30 to 31, and the one addition names the reason the plan
predicted: `tests/test_datastore_file.py:968: Datastore supports auto but cannot
transfer in place.`

## Findings for separate tickets

| Finding | Where | Why not fixed here |
| --- | --- | --- |
| `useTempRoot` is a dead class attribute | `tests/test_butler.py:625` sets it `True`, `:2697` sets it `False`, and nothing ever reads it | Dropped rather than ported. Test-only, so no library change and no ticket needed. |
| `assert type(a) != type(b)` trips E721 | `assertButlerTransfers`, from the ruff autofix of `assertNotEqual(type(a), type(b))` | Rewritten `is not`, which is what the comparison meant. |
| `self.id = 0` is dead state | `DatastoreCacheTestCase.setUp` assigned it and nothing read it; `DatasetTestHelper.makeDatasetRef` does not use instance state | Dropped rather than ported. Test-only. |
| `runPutGetTest`'s `args = tuple[DatasetRef] \| tuple[str \| DatasetType, DataCoordinate]` | `tests/test_butler.py:306`, immediately before the loop that rebinds `args` | A type expression assigned as a value, so it did nothing. Written as the annotation it was meant to be. Test-only, so no library change and no ticket needed. |
| `retrieveArtifacts(transfer="move")` raises different messages per client | `FileDatastore` says "Can not move artifacts out of datastore", `RemoteButler` says "Only 'copy' and 'auto' transfer modes are supported" | The original bare `assertRaises(ValueError)` hid this. `PT011` forces a `match`, so the converted assertion names both. Not a bug, but the two implementations could usefully agree; not worth a ticket on its own. |
| `fullConfigKey` is now a dead class attribute | six classes in `tests/test_butler.py` set it and, since `testMakeRepo` moved out, nothing reads it | Left in place rather than churned out of a file Task 12 deletes. Not ported: the new files read `DatastoreProfile.full_config_key`. |
| `Butler.from_config` on a config with `configFile = None` | raises "Required to replace &lt;butlerRoot&gt; ... but a replacement has not been defined", not a message about the config file | The original bare `assertRaises(ValueError)` hid which failure was being provoked. `PT011` forces a `match`, so the converted assertion names the real one. |
| `predictionSupported` and `trustModeSupported` are now dead class attributes | `tests/test_butler.py` still sets them; the last readers left with Tasks 11 and 8 | Left in place like `fullConfigKey`, rather than churned out of a file Task 12 deletes. Not ported: the new files read `ButlerHarness.prediction_supported` and `.trust_mode_supported`. |
| `Butler.exists` on a ref with a colliding UUID | raises "... has the same dataset ID as one in registry but has different incompatible values" | Another bare `assertRaises(ValueError)` that `PT011` forced to name its real message. |
| `test_provenance`'s bad-input-ID assertion does not test what its comment says | `tests/test_butler.py::PosixDatastoreButlerTestCase::test_provenance`, the `prov_dict["input 0 id"] = uuid.uuid4()` case | The added key separates its words with spaces while the rest of the header separates with ".", so `from_flat_dict` raises "Inconsistent values found for separators" before it ever looks the input ID up. The bare `assertRaises(ValueError)` hid this. The converted test asserts the message that actually occurs and carries a comment; making the test check what it intended is a change of test behavior and belongs on its own ticket. |
| `CleanupPosixDatastoreTestCase.testCleanup`'s two formatter cases are order-dependent | `tests/test_datastore.py::CleanupPosixDatastoreTestCase::testCleanup` | The second case asserts the datastore directory exists, but that directory is created by the *first* case's failed put. Parametrizing the two, as the plan asked, makes the `BadNoWriteFormatter` case fail on its own. The loop is kept, with a comment saying why. Making the second case independent is a change of test behavior and belongs on its own ticket. |
| `unittest` assertion messages are eager, bare `assert` messages are lazy | every `self.assertX(..., msg=...)` the `PT009` codemod rewrote | Only one site's message had a side effect worth keeping (`DatastoreDisabledCacheManager.__str__`), and it is fixed. Worth knowing for any future conversion: a message that was the only caller of a `__repr__` silently stops calling it. |
| `dimensions/_config.py` arc 138 to 134 belongs to no test | the search-path loop's "not in this directory, try the next" branch | Whether it is taken depends on state accumulated across a whole session, so which test covers it moves with the file layout. Not caused by this branch; noted so a future coverage comparison does not chase it. |

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
| `tests/test_butler.py::NullDatastoreTestCase::test_fallback` | `tests/test_butler_null_datastore.py::test_fallback` |
| `tests/test_butler.py::PosixDatastoreTransfers::testTransferUuidToUuid` | `tests/test_butler_transfers.py::test_transfer_uuid_to_uuid[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testTransferUuidToUuid` | `tests/test_butler_transfers.py::test_transfer_uuid_to_uuid[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testTransferFromChainedUuidToUuid` | `tests/test_butler_transfers.py::test_transfer_from_chained_uuid_to_uuid[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testTransferFromChainedUuidToUuid` | `tests/test_butler_transfers.py::test_transfer_from_chained_uuid_to_uuid[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testTransferFromIncompatibleUuidToUuid` | `tests/test_butler_transfers.py::test_transfer_from_incompatible_uuid_to_uuid[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testTransferFromIncompatibleUuidToUuid` | `tests/test_butler_transfers.py::test_transfer_from_incompatible_uuid_to_uuid[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testTransferFromIncompatibleChainUuidToUuid` | `tests/test_butler_transfers.py::test_transfer_from_incompatible_chain_uuid_to_uuid[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testTransferFromIncompatibleChainUuidToUuid` | `tests/test_butler_transfers.py::test_transfer_from_incompatible_chain_uuid_to_uuid[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testTransferFromFileUuidToUuid` | `tests/test_butler_transfers.py::test_transfer_from_file_uuid_to_uuid[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testTransferFromFileUuidToUuid` | `tests/test_butler_transfers.py::test_transfer_from_file_uuid_to_uuid[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testTransferMissing` | `tests/test_butler_transfers.py::test_transfer_missing[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testTransferMissing` | `tests/test_butler_transfers.py::test_transfer_missing[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testTransferMissingDisassembly` | `tests/test_butler_transfers.py::test_transfer_missing_disassembly[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testTransferMissingDisassembly` | `tests/test_butler_transfers.py::test_transfer_missing_disassembly[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testTransferDifferingStorageClasses` | `tests/test_butler_transfers.py::test_transfer_differing_storage_classes[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testTransferDifferingStorageClasses` | `tests/test_butler_transfers.py::test_transfer_differing_storage_classes[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testTransferDifferingStorageClassesDisassembly` | `tests/test_butler_transfers.py::test_transfer_differing_storage_classes_disassembly[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testTransferDifferingStorageClassesDisassembly` | `tests/test_butler_transfers.py::test_transfer_differing_storage_classes_disassembly[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testUnsafeDirectTransfer` | `tests/test_butler_transfers.py::test_unsafe_direct_transfer[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testUnsafeDirectTransfer` | `tests/test_butler_transfers.py::test_unsafe_direct_transfer[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testAbsoluteURITransferDirect` | `tests/test_butler_transfers.py::test_absolute_u_r_i_transfer_direct[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testAbsoluteURITransferDirect` | `tests/test_butler_transfers.py::test_absolute_u_r_i_transfer_direct[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testAbsoluteURITransferUnsafeDirect` | `tests/test_butler_transfers.py::test_absolute_u_r_i_transfer_unsafe_direct[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testAbsoluteURITransferUnsafeDirect` | `tests/test_butler_transfers.py::test_absolute_u_r_i_transfer_unsafe_direct[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::testAbsoluteURITransferCopy` | `tests/test_butler_transfers.py::test_absolute_u_r_i_transfer_copy[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::testAbsoluteURITransferCopy` | `tests/test_butler_transfers.py::test_absolute_u_r_i_transfer_copy[chained]` |
| `tests/test_butler.py::PosixDatastoreTransfers::test_shared_dimension_group` | `tests/test_butler_transfers.py::test_shared_dimension_group[posix]` |
| `tests/test_butler.py::ChainedDatastoreTransfers::test_shared_dimension_group` | `tests/test_butler_transfers.py::test_shared_dimension_group[chained]` |
| `tests/test_butler.py::ButlerServerDatastoreTransfers::test_transfers_from_remote_to_direct` | `tests/test_butler_transfers.py::test_transfers_from_remote_to_direct[posix]` |
| `tests/test_butler.py::TransferDatasetsInPlace::test_file_datastore` | `tests/test_butler_transfers.py::test_file_datastore` |
| `tests/test_butler.py::TransferDatasetsInPlace::test_chained_datastore` | `tests/test_butler_transfers.py::test_chained_datastore` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testBasicPutGet` | `tests/test_butler_put_get.py::test_basic_put_get[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testComponentFromOverriddenStorageClass` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testComponentFromOverriddenStorageClassWarns` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class_warns[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testCompositePutGetConcrete` | `tests/test_butler_put_get.py::test_composite_put_get_concrete[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testCompositePutGetVirtual` | `tests/test_butler_put_get.py::test_composite_put_get_virtual[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testPytypePutCoercion` | `tests/test_butler_put_get.py::test_pytype_put_coercion[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testStorageClassOverrideGet` | `tests/test_butler_put_get.py::test_storage_class_override_get[posix]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testBasicPutGet` | `tests/test_butler_put_get.py::test_basic_put_get[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testComponentFromOverriddenStorageClass` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testComponentFromOverriddenStorageClassWarns` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class_warns[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testCompositePutGetConcrete` | `tests/test_butler_put_get.py::test_composite_put_get_concrete[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testCompositePutGetVirtual` | `tests/test_butler_put_get.py::test_composite_put_get_virtual[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testPytypePutCoercion` | `tests/test_butler_put_get.py::test_pytype_put_coercion[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testStorageClassOverrideGet` | `tests/test_butler_put_get.py::test_storage_class_override_get[postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testBasicPutGet` | `tests/test_butler_put_get.py::test_basic_put_get[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testComponentFromOverriddenStorageClass` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testComponentFromOverriddenStorageClassWarns` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class_warns[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testCompositePutGetConcrete` | `tests/test_butler_put_get.py::test_composite_put_get_concrete[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testCompositePutGetVirtual` | `tests/test_butler_put_get.py::test_composite_put_get_virtual[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testPytypePutCoercion` | `tests/test_butler_put_get.py::test_pytype_put_coercion[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testStorageClassOverrideGet` | `tests/test_butler_put_get.py::test_storage_class_override_get[cloned-postgres]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testBasicPutGet` | `tests/test_butler_put_get.py::test_basic_put_get[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testComponentFromOverriddenStorageClass` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testCompositePutGetConcrete` | `tests/test_butler_put_get.py::test_composite_put_get_concrete[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testCompositePutGetVirtual` | `tests/test_butler_put_get.py::test_composite_put_get_virtual[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testPytypePutCoercion` | `tests/test_butler_put_get.py::test_pytype_put_coercion[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testStorageClassOverrideGet` | `tests/test_butler_put_get.py::test_storage_class_override_get[in-memory]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testBasicPutGet` | `tests/test_butler_put_get.py::test_basic_put_get[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testComponentFromOverriddenStorageClass` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testCompositePutGetConcrete` | `tests/test_butler_put_get.py::test_composite_put_get_concrete[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testCompositePutGetVirtual` | `tests/test_butler_put_get.py::test_composite_put_get_virtual[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testPytypePutCoercion` | `tests/test_butler_put_get.py::test_pytype_put_coercion[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testStorageClassOverrideGet` | `tests/test_butler_put_get.py::test_storage_class_override_get[cloned-sqlite]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testBasicPutGet` | `tests/test_butler_put_get.py::test_basic_put_get[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testComponentFromOverriddenStorageClass` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testComponentFromOverriddenStorageClassWarns` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class_warns[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testCompositePutGetConcrete` | `tests/test_butler_put_get.py::test_composite_put_get_concrete[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testCompositePutGetVirtual` | `tests/test_butler_put_get.py::test_composite_put_get_virtual[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testPytypePutCoercion` | `tests/test_butler_put_get.py::test_pytype_put_coercion[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testStorageClassOverrideGet` | `tests/test_butler_put_get.py::test_storage_class_override_get[chained]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testBasicPutGet` | `tests/test_butler_put_get.py::test_basic_put_get[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testComponentFromOverriddenStorageClass` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testComponentFromOverriddenStorageClassWarns` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class_warns[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testCompositePutGetConcrete` | `tests/test_butler_put_get.py::test_composite_put_get_concrete[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testCompositePutGetVirtual` | `tests/test_butler_put_get.py::test_composite_put_get_virtual[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testPytypePutCoercion` | `tests/test_butler_put_get.py::test_pytype_put_coercion[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testStorageClassOverrideGet` | `tests/test_butler_put_get.py::test_storage_class_override_get[explicit-root]` |
| `tests/test_butler.py::ButlerMakeRepoOutfileTestCase::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[outfile]` |
| `tests/test_butler.py::ButlerMakeRepoOutfileDirTestCase::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[outfile-dir]` |
| `tests/test_butler.py::ButlerMakeRepoOutfileUriTestCase::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[outfile-uri]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testBasicPutGet` | `tests/test_butler_put_get.py::test_basic_put_get[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testComponentFromOverriddenStorageClass` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testComponentFromOverriddenStorageClassWarns` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class_warns[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testCompositePutGetConcrete` | `tests/test_butler_put_get.py::test_composite_put_get_concrete[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testCompositePutGetVirtual` | `tests/test_butler_put_get.py::test_composite_put_get_virtual[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testPytypePutCoercion` | `tests/test_butler_put_get.py::test_pytype_put_coercion[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testStorageClassOverrideGet` | `tests/test_butler_put_get.py::test_storage_class_override_get[remote-test]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testBasicPutGet` | `tests/test_butler_put_get.py::test_basic_put_get[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testComponentFromOverriddenStorageClass` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testComponentFromOverriddenStorageClassWarns` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class_warns[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testCompositePutGetConcrete` | `tests/test_butler_put_get.py::test_composite_put_get_concrete[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testCompositePutGetVirtual` | `tests/test_butler_put_get.py::test_composite_put_get_virtual[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testPytypePutCoercion` | `tests/test_butler_put_get.py::test_pytype_put_coercion[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testStorageClassOverrideGet` | `tests/test_butler_put_get.py::test_storage_class_override_get[server-sqlite]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testBasicPutGet` | `tests/test_butler_put_get.py::test_basic_put_get[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testComponentFromOverriddenStorageClass` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testComponentFromOverriddenStorageClassWarns` | `tests/test_butler_put_get.py::test_component_from_overridden_storage_class_warns[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testCompositePutGetConcrete` | `tests/test_butler_put_get.py::test_composite_put_get_concrete[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testCompositePutGetVirtual` | `tests/test_butler_put_get.py::test_composite_put_get_virtual[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testDeferredCollectionPassing` | `tests/test_butler_put_get.py::test_deferred_collection_passing[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testPytypePutCoercion` | `tests/test_butler_put_get.py::test_pytype_put_coercion[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testStorageClassOverrideGet` | `tests/test_butler_put_get.py::test_storage_class_override_get[server-postgres]` |
| `tests/test_butler.py::ButlerConfigTests::testSearchPath` | `tests/test_butler_config_repo.py::test_search_path` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testFileLocations` | `tests/test_butler_config_repo.py::test_file_locations[explicit_root]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testMakeRepo` | `tests/test_butler_config_repo.py::test_make_repo[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testPutTemplates` | `tests/test_butler_config_repo.py::test_put_templates[posix]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testMakeRepo` | `tests/test_butler_config_repo.py::test_make_repo[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testPutTemplates` | `tests/test_butler_config_repo.py::test_put_templates[postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testMakeRepo` | `tests/test_butler_config_repo.py::test_make_repo[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testPutTemplates` | `tests/test_butler_config_repo.py::test_put_templates[cloned-postgres]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testMakeRepo` | `tests/test_butler_config_repo.py::test_make_repo[in-memory]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testMakeRepo` | `tests/test_butler_config_repo.py::test_make_repo[cloned-sqlite]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testMakeRepo` | `tests/test_butler_config_repo.py::test_make_repo[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testPutTemplates` | `tests/test_butler_config_repo.py::test_put_templates[chained]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testMakeRepo` | `tests/test_butler_config_repo.py::test_make_repo[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testPutTemplates` | `tests/test_butler_config_repo.py::test_put_templates[explicit-root]` |
| `tests/test_butler.py::ButlerMakeRepoOutfileTestCase::testConfigExistence` | `tests/test_butler_config_repo.py::test_config_existence[outfile]` |
| `tests/test_butler.py::ButlerMakeRepoOutfileTestCase::testPutGet` | `tests/test_butler_config_repo.py::test_put_get[outfile]` |
| `tests/test_butler.py::ButlerMakeRepoOutfileDirTestCase::testConfigExistence` | `tests/test_butler_config_repo.py::test_config_existence[outfile_dir]` |
| `tests/test_butler.py::ButlerMakeRepoOutfileDirTestCase::testPutGet` | `tests/test_butler_config_repo.py::test_put_get[outfile_dir]` |
| `tests/test_butler.py::ButlerMakeRepoOutfileUriTestCase::testConfigExistence` | `tests/test_butler_config_repo.py::test_config_existence[outfile_uri]` |
| `tests/test_butler.py::ButlerMakeRepoOutfileUriTestCase::testPutGet` | `tests/test_butler_config_repo.py::test_put_get[outfile_uri]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testMakeRepo` | `tests/test_butler_config_repo.py::test_make_repo[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testPutTemplates` | `tests/test_butler_config_repo.py::test_put_templates[remote-test]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testMakeRepo` | `tests/test_butler_config_repo.py::test_make_repo[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testPutTemplates` | `tests/test_butler_config_repo.py::test_put_templates[server-sqlite]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testMakeRepo` | `tests/test_butler_config_repo.py::test_make_repo[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testPutTemplates` | `tests/test_butler_config_repo.py::test_put_templates[server-postgres]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testExportTransferCopy` | `tests/test_butler_import_export.py::test_export_transfer_copy[in_repo]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testImportExport` | `tests/test_butler_import_export.py::test_import_export[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testImportExportVirtualComposite` | `tests/test_butler_import_export.py::test_import_export_virtual_composite[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testPruneDatasets` | `tests/test_butler_import_export.py::test_prune_datasets[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testRemoveRuns` | `tests/test_butler_import_export.py::test_remove_runs[posix]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testImportExport` | `tests/test_butler_import_export.py::test_import_export[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testImportExportVirtualComposite` | `tests/test_butler_import_export.py::test_import_export_virtual_composite[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testPruneDatasets` | `tests/test_butler_import_export.py::test_prune_datasets[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testRemoveRuns` | `tests/test_butler_import_export.py::test_remove_runs[postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testImportExport` | `tests/test_butler_import_export.py::test_import_export[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testImportExportVirtualComposite` | `tests/test_butler_import_export.py::test_import_export_virtual_composite[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testPruneDatasets` | `tests/test_butler_import_export.py::test_prune_datasets[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testRemoveRuns` | `tests/test_butler_import_export.py::test_remove_runs[cloned-postgres]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testImportExport` | `tests/test_butler_import_export.py::test_import_export[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testImportExportVirtualComposite` | `tests/test_butler_import_export.py::test_import_export_virtual_composite[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testPruneDatasets` | `tests/test_butler_import_export.py::test_prune_datasets[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testRemoveRuns` | `tests/test_butler_import_export.py::test_remove_runs[chained]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testExportTransferCopy` | `tests/test_butler_import_export.py::test_export_transfer_copy[explicit_root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testImportExport` | `tests/test_butler_import_export.py::test_import_export[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testImportExportVirtualComposite` | `tests/test_butler_import_export.py::test_import_export_virtual_composite[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testPruneDatasets` | `tests/test_butler_import_export.py::test_prune_datasets[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testRemoveRuns` | `tests/test_butler_import_export.py::test_remove_runs[explicit-root]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testImportExport` | `tests/test_butler_import_export.py::test_import_export[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testImportExportVirtualComposite` | `tests/test_butler_import_export.py::test_import_export_virtual_composite[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testPruneDatasets` | `tests/test_butler_import_export.py::test_prune_datasets[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testRemoveRuns` | `tests/test_butler_import_export.py::test_remove_runs[remote-test]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testImportExport` | `tests/test_butler_import_export.py::test_import_export[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testImportExportVirtualComposite` | `tests/test_butler_import_export.py::test_import_export_virtual_composite[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testPruneDatasets` | `tests/test_butler_import_export.py::test_prune_datasets[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testRemoveRuns` | `tests/test_butler_import_export.py::test_remove_runs[server-sqlite]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testImportExport` | `tests/test_butler_import_export.py::test_import_export[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testImportExportVirtualComposite` | `tests/test_butler_import_export.py::test_import_export_virtual_composite[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testPruneDatasets` | `tests/test_butler_import_export.py::test_prune_datasets[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testRemoveRuns` | `tests/test_butler_import_export.py::test_remove_runs[server-postgres]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testCollectionChainExtend` | `tests/test_butler_collections.py::test_collection_chain_extend[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testCollectionChainPrepend` | `tests/test_butler_collections.py::test_collection_chain_prepend[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testCollectionChainRedefine` | `tests/test_butler_collections.py::test_collection_chain_redefine[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testCollectionChainRemove` | `tests/test_butler_collections.py::test_collection_chain_remove[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testGetDatasetCollectionCaching` | `tests/test_butler_collections.py::test_get_dataset_collection_caching[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testGetDatasetTypes` | `tests/test_butler_collections.py::test_get_dataset_types[posix]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testCollectionChainExtend` | `tests/test_butler_collections.py::test_collection_chain_extend[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testCollectionChainPrepend` | `tests/test_butler_collections.py::test_collection_chain_prepend[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testCollectionChainRedefine` | `tests/test_butler_collections.py::test_collection_chain_redefine[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testCollectionChainRemove` | `tests/test_butler_collections.py::test_collection_chain_remove[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testGetDatasetCollectionCaching` | `tests/test_butler_collections.py::test_get_dataset_collection_caching[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testGetDatasetTypes` | `tests/test_butler_collections.py::test_get_dataset_types[postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testCollectionChainExtend` | `tests/test_butler_collections.py::test_collection_chain_extend[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testCollectionChainPrepend` | `tests/test_butler_collections.py::test_collection_chain_prepend[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testCollectionChainRedefine` | `tests/test_butler_collections.py::test_collection_chain_redefine[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testCollectionChainRemove` | `tests/test_butler_collections.py::test_collection_chain_remove[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testGetDatasetCollectionCaching` | `tests/test_butler_collections.py::test_get_dataset_collection_caching[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testGetDatasetTypes` | `tests/test_butler_collections.py::test_get_dataset_types[cloned-postgres]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testCollectionChainExtend` | `tests/test_butler_collections.py::test_collection_chain_extend[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testCollectionChainPrepend` | `tests/test_butler_collections.py::test_collection_chain_prepend[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testCollectionChainRedefine` | `tests/test_butler_collections.py::test_collection_chain_redefine[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testCollectionChainRemove` | `tests/test_butler_collections.py::test_collection_chain_remove[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testGetDatasetCollectionCaching` | `tests/test_butler_collections.py::test_get_dataset_collection_caching[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testGetDatasetTypes` | `tests/test_butler_collections.py::test_get_dataset_types[in-memory]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testCollectionChainExtend` | `tests/test_butler_collections.py::test_collection_chain_extend[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testCollectionChainPrepend` | `tests/test_butler_collections.py::test_collection_chain_prepend[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testCollectionChainRedefine` | `tests/test_butler_collections.py::test_collection_chain_redefine[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testCollectionChainRemove` | `tests/test_butler_collections.py::test_collection_chain_remove[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testGetDatasetCollectionCaching` | `tests/test_butler_collections.py::test_get_dataset_collection_caching[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testGetDatasetTypes` | `tests/test_butler_collections.py::test_get_dataset_types[cloned-sqlite]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testCollectionChainExtend` | `tests/test_butler_collections.py::test_collection_chain_extend[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testCollectionChainPrepend` | `tests/test_butler_collections.py::test_collection_chain_prepend[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testCollectionChainRedefine` | `tests/test_butler_collections.py::test_collection_chain_redefine[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testCollectionChainRemove` | `tests/test_butler_collections.py::test_collection_chain_remove[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testGetDatasetCollectionCaching` | `tests/test_butler_collections.py::test_get_dataset_collection_caching[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testGetDatasetTypes` | `tests/test_butler_collections.py::test_get_dataset_types[chained]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testCollectionChainExtend` | `tests/test_butler_collections.py::test_collection_chain_extend[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testCollectionChainPrepend` | `tests/test_butler_collections.py::test_collection_chain_prepend[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testCollectionChainRedefine` | `tests/test_butler_collections.py::test_collection_chain_redefine[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testCollectionChainRemove` | `tests/test_butler_collections.py::test_collection_chain_remove[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testGetDatasetCollectionCaching` | `tests/test_butler_collections.py::test_get_dataset_collection_caching[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testGetDatasetTypes` | `tests/test_butler_collections.py::test_get_dataset_types[explicit-root]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testCollectionChainExtend` | `tests/test_butler_collections.py::test_collection_chain_extend[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testCollectionChainPrepend` | `tests/test_butler_collections.py::test_collection_chain_prepend[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testCollectionChainRedefine` | `tests/test_butler_collections.py::test_collection_chain_redefine[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testCollectionChainRemove` | `tests/test_butler_collections.py::test_collection_chain_remove[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testGetDatasetCollectionCaching` | `tests/test_butler_collections.py::test_get_dataset_collection_caching[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testGetDatasetTypes` | `tests/test_butler_collections.py::test_get_dataset_types[remote-test]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testCollectionChainExtend` | `tests/test_butler_collections.py::test_collection_chain_extend[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testCollectionChainPrepend` | `tests/test_butler_collections.py::test_collection_chain_prepend[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testCollectionChainRedefine` | `tests/test_butler_collections.py::test_collection_chain_redefine[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testCollectionChainRemove` | `tests/test_butler_collections.py::test_collection_chain_remove[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testGetDatasetCollectionCaching` | `tests/test_butler_collections.py::test_get_dataset_collection_caching[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testGetDatasetTypes` | `tests/test_butler_collections.py::test_get_dataset_types[server-sqlite]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testCollectionChainExtend` | `tests/test_butler_collections.py::test_collection_chain_extend[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testCollectionChainPrepend` | `tests/test_butler_collections.py::test_collection_chain_prepend[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testCollectionChainRedefine` | `tests/test_butler_collections.py::test_collection_chain_redefine[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testCollectionChainRemove` | `tests/test_butler_collections.py::test_collection_chain_remove[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testGetDatasetCollectionCaching` | `tests/test_butler_collections.py::test_get_dataset_collection_caching[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testGetDatasetTypes` | `tests/test_butler_collections.py::test_get_dataset_types[server-postgres]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testIngest` | `tests/test_butler_ingest.py::test_ingest[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::test_ingest_zip` | `tests/test_butler_ingest.py::test_ingest_zip[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::test_specialized_file_datasets_functions` | `tests/test_butler_ingest.py::test_specialized_file_datasets_functions[in_repo]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::test_temporary_for_ingest` | `tests/test_butler_ingest.py::test_temporary_for_ingest[in_repo]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testIngest` | `tests/test_butler_ingest.py::test_ingest[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::test_ingest_zip` | `tests/test_butler_ingest.py::test_ingest_zip[postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testIngest` | `tests/test_butler_ingest.py::test_ingest[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::test_ingest_zip` | `tests/test_butler_ingest.py::test_ingest_zip[cloned-postgres]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testIngest` | dropped: empty override, InMemoryDatastore cannot ingest |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::test_ingest_zip` | dropped: empty override, InMemoryDatastore cannot ingest |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testIngest` | dropped: empty override, InMemoryDatastore cannot ingest |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::test_ingest_zip` | dropped: empty override, InMemoryDatastore cannot ingest |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testIngest` | `tests/test_butler_ingest.py::test_ingest[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::test_ingest_zip` | `tests/test_butler_ingest.py::test_ingest_zip[chained]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testIngest` | `tests/test_butler_ingest.py::test_ingest[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::test_ingest_zip` | `tests/test_butler_ingest.py::test_ingest_zip[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::test_specialized_file_datasets_functions` | `tests/test_butler_ingest.py::test_specialized_file_datasets_functions[explicit_root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::test_temporary_for_ingest` | `tests/test_butler_ingest.py::test_temporary_for_ingest[explicit_root]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testIngest` | `tests/test_butler_ingest.py::test_ingest[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::test_ingest_zip` | `tests/test_butler_ingest.py::test_ingest_zip[remote-test]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testIngest` | `tests/test_butler_ingest.py::test_ingest[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::test_ingest_zip` | `tests/test_butler_ingest.py::test_ingest_zip[server-sqlite]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testIngest` | `tests/test_butler_ingest.py::test_ingest[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::test_ingest_zip` | `tests/test_butler_ingest.py::test_ingest_zip[server-postgres]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testButlerRewriteDataId` | `tests/test_butler_lifecycle.py::test_butler_rewrite_data_id[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testClose` | `tests/test_butler_lifecycle.py::test_close[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testConstructor` | `tests/test_butler_lifecycle.py::test_constructor[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testDafButlerRepositories` | `tests/test_butler_lifecycle.py::test_daf_butler_repositories[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testGarbageCollection` | `tests/test_butler_lifecycle.py::test_garbage_collection[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testPathConstructor` | `tests/test_butler_lifecycle.py::test_path_constructor[in_repo]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testPickle` | `tests/test_butler_lifecycle.py::test_pickle[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testPytypeCoercion` | `tests/test_butler_lifecycle.py::test_pytype_coercion[in_repo]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testStringification` | `tests/test_butler_lifecycle.py::test_stringification[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::testTransaction` | `tests/test_butler_lifecycle.py::test_transaction[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::test_butler_metrics` | `tests/test_butler_lifecycle.py::test_butler_metrics[posix]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::test_provenance` | `tests/test_butler_lifecycle.py::test_provenance[in_repo]` |
| `tests/test_butler.py::PosixDatastoreButlerTestCase::test_transfer_dimension_records_from` | `tests/test_butler_lifecycle.py::test_transfer_dimension_records_from[posix]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testButlerRewriteDataId` | `tests/test_butler_lifecycle.py::test_butler_rewrite_data_id[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testClose` | `tests/test_butler_lifecycle.py::test_close[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testConstructor` | `tests/test_butler_lifecycle.py::test_constructor[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testDafButlerRepositories` | `tests/test_butler_lifecycle.py::test_daf_butler_repositories[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testGarbageCollection` | `tests/test_butler_lifecycle.py::test_garbage_collection[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testPickle` | `tests/test_butler_lifecycle.py::test_pickle[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testStringification` | `tests/test_butler_lifecycle.py::test_stringification[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::testTransaction` | `tests/test_butler_lifecycle.py::test_transaction[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::test_butler_metrics` | `tests/test_butler_lifecycle.py::test_butler_metrics[postgres]` |
| `tests/test_butler.py::PostgresPosixDatastoreButlerTestCase::test_transfer_dimension_records_from` | `tests/test_butler_lifecycle.py::test_transfer_dimension_records_from[postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testButlerRewriteDataId` | `tests/test_butler_lifecycle.py::test_butler_rewrite_data_id[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testClose` | `tests/test_butler_lifecycle.py::test_close[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testConstructor` | `tests/test_butler_lifecycle.py::test_constructor[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testDafButlerRepositories` | `tests/test_butler_lifecycle.py::test_daf_butler_repositories[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testGarbageCollection` | `tests/test_butler_lifecycle.py::test_garbage_collection[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testPickle` | `tests/test_butler_lifecycle.py::test_pickle[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testStringification` | `tests/test_butler_lifecycle.py::test_stringification[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::testTransaction` | `tests/test_butler_lifecycle.py::test_transaction[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::test_butler_metrics` | `tests/test_butler_lifecycle.py::test_butler_metrics[cloned-postgres]` |
| `tests/test_butler.py::ClonedPostgresPosixDatastoreButlerTestCase::test_transfer_dimension_records_from` | `tests/test_butler_lifecycle.py::test_transfer_dimension_records_from[cloned-postgres]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testButlerRewriteDataId` | `tests/test_butler_lifecycle.py::test_butler_rewrite_data_id[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testClose` | `tests/test_butler_lifecycle.py::test_close[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testConstructor` | `tests/test_butler_lifecycle.py::test_constructor[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testDafButlerRepositories` | `tests/test_butler_lifecycle.py::test_daf_butler_repositories[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testGarbageCollection` | `tests/test_butler_lifecycle.py::test_garbage_collection[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testPickle` | `tests/test_butler_lifecycle.py::test_pickle[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testStringification` | `tests/test_butler_lifecycle.py::test_stringification[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::testTransaction` | `tests/test_butler_lifecycle.py::test_transaction[in-memory]` |
| `tests/test_butler.py::InMemoryDatastoreButlerTestCase::test_transfer_dimension_records_from` | `tests/test_butler_lifecycle.py::test_transfer_dimension_records_from[in-memory]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testButlerRewriteDataId` | `tests/test_butler_lifecycle.py::test_butler_rewrite_data_id[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testClose` | `tests/test_butler_lifecycle.py::test_close[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testConstructor` | `tests/test_butler_lifecycle.py::test_constructor[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testDafButlerRepositories` | `tests/test_butler_lifecycle.py::test_daf_butler_repositories[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testGarbageCollection` | `tests/test_butler_lifecycle.py::test_garbage_collection[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testPickle` | `tests/test_butler_lifecycle.py::test_pickle[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testStringification` | `tests/test_butler_lifecycle.py::test_stringification[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::testTransaction` | `tests/test_butler_lifecycle.py::test_transaction[cloned-sqlite]` |
| `tests/test_butler.py::ClonedSqliteButlerTestCase::test_transfer_dimension_records_from` | `tests/test_butler_lifecycle.py::test_transfer_dimension_records_from[cloned-sqlite]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testButlerRewriteDataId` | `tests/test_butler_lifecycle.py::test_butler_rewrite_data_id[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testClose` | `tests/test_butler_lifecycle.py::test_close[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testConstructor` | `tests/test_butler_lifecycle.py::test_constructor[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testDafButlerRepositories` | `tests/test_butler_lifecycle.py::test_daf_butler_repositories[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testGarbageCollection` | `tests/test_butler_lifecycle.py::test_garbage_collection[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testPickle` | `tests/test_butler_lifecycle.py::test_pickle[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testStringification` | `tests/test_butler_lifecycle.py::test_stringification[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::testTransaction` | `tests/test_butler_lifecycle.py::test_transaction[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::test_butler_metrics` | `tests/test_butler_lifecycle.py::test_butler_metrics[chained]` |
| `tests/test_butler.py::ChainedDatastoreButlerTestCase::test_transfer_dimension_records_from` | `tests/test_butler_lifecycle.py::test_transfer_dimension_records_from[chained]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testButlerRewriteDataId` | `tests/test_butler_lifecycle.py::test_butler_rewrite_data_id[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testClose` | `tests/test_butler_lifecycle.py::test_close[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testConstructor` | `tests/test_butler_lifecycle.py::test_constructor[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testDafButlerRepositories` | `tests/test_butler_lifecycle.py::test_daf_butler_repositories[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testGarbageCollection` | `tests/test_butler_lifecycle.py::test_garbage_collection[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testPathConstructor` | `tests/test_butler_lifecycle.py::test_path_constructor[explicit_root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testPickle` | `tests/test_butler_lifecycle.py::test_pickle[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testPytypeCoercion` | `tests/test_butler_lifecycle.py::test_pytype_coercion[explicit_root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testStringification` | `tests/test_butler_lifecycle.py::test_stringification[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::testTransaction` | `tests/test_butler_lifecycle.py::test_transaction[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::test_butler_metrics` | `tests/test_butler_lifecycle.py::test_butler_metrics[explicit-root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::test_provenance` | `tests/test_butler_lifecycle.py::test_provenance[explicit_root]` |
| `tests/test_butler.py::ButlerExplicitRootTestCase::test_transfer_dimension_records_from` | `tests/test_butler_lifecycle.py::test_transfer_dimension_records_from[explicit-root]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testButlerRewriteDataId` | `tests/test_butler_lifecycle.py::test_butler_rewrite_data_id[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testClose` | `tests/test_butler_lifecycle.py::test_close[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testConstructor` | `tests/test_butler_lifecycle.py::test_constructor[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testDafButlerRepositories` | `tests/test_butler_lifecycle.py::test_daf_butler_repositories[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testGarbageCollection` | `tests/test_butler_lifecycle.py::test_garbage_collection[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testPickle` | `tests/test_butler_lifecycle.py::test_pickle[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testStringification` | `tests/test_butler_lifecycle.py::test_stringification[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::testTransaction` | `tests/test_butler_lifecycle.py::test_transaction[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::test_butler_metrics` | `tests/test_butler_lifecycle.py::test_butler_metrics[remote-test]` |
| `tests/test_butler.py::RemoteTestDatastoreButlerTestCase::test_transfer_dimension_records_from` | `tests/test_butler_lifecycle.py::test_transfer_dimension_records_from[remote-test]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testButlerRewriteDataId` | `tests/test_butler_lifecycle.py::test_butler_rewrite_data_id[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testClose` | `tests/test_butler_lifecycle.py::test_close[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testConstructor` | `tests/test_butler_lifecycle.py::test_constructor[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testDafButlerRepositories` | `tests/test_butler_lifecycle.py::test_daf_butler_repositories[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testGarbageCollection` | `tests/test_butler_lifecycle.py::test_garbage_collection[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testPickle` | `tests/test_butler_lifecycle.py::test_pickle[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testStringification` | `tests/test_butler_lifecycle.py::test_stringification[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::testTransaction` | `tests/test_butler_lifecycle.py::test_transaction[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::test_butler_metrics` | `tests/test_butler_lifecycle.py::test_butler_metrics[server-sqlite]` |
| `tests/test_butler.py::ButlerServerSqliteTests::test_transfer_dimension_records_from` | `tests/test_butler_lifecycle.py::test_transfer_dimension_records_from[server-sqlite]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testButlerRewriteDataId` | `tests/test_butler_lifecycle.py::test_butler_rewrite_data_id[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testClose` | `tests/test_butler_lifecycle.py::test_close[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testConstructor` | `tests/test_butler_lifecycle.py::test_constructor[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testDafButlerRepositories` | `tests/test_butler_lifecycle.py::test_daf_butler_repositories[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testGarbageCollection` | `tests/test_butler_lifecycle.py::test_garbage_collection[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testPickle` | `tests/test_butler_lifecycle.py::test_pickle[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testStringification` | `tests/test_butler_lifecycle.py::test_stringification[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::testTransaction` | `tests/test_butler_lifecycle.py::test_transaction[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::test_butler_metrics` | `tests/test_butler_lifecycle.py::test_butler_metrics[server-postgres]` |
| `tests/test_butler.py::ButlerServerPostgresTests::test_transfer_dimension_records_from` | `tests/test_butler_lifecycle.py::test_transfer_dimension_records_from[server-postgres]` |
| `tests/test_datastore.py::DatasetRefURIsTestCase::testSequenceAccess` | `tests/test_datastore_records.py::test_sequence_access` |
| `tests/test_datastore.py::DatasetRefURIsTestCase::testRepr` | `tests/test_datastore_records.py::test_repr` |
| `tests/test_datastore.py::StoredFileInfoTestCase::test_StoredFileInfo` | `tests/test_datastore_records.py::test_stored_file_info` |
| `tests/test_datastore.py::StoredFileInfoTestCase::test_make_datastore_path_relative` | `tests/test_datastore_records.py::test_make_datastore_path_relative` |
| `tests/test_datastore.py::StoredFileInfoTestCase::test_datastore_record_data_json_types` | `tests/test_datastore_records.py::test_datastore_record_data_json_types` |
| `tests/test_datastore.py::TestDatastoreRecordTable::test_empty_datastore_records_table` | `tests/test_datastore_records.py::test_empty_datastore_records_table` |
| `tests/test_datastore.py::TestDatastoreRecordTable::test_stored_file_info_table_records` | `tests/test_datastore_records.py::test_stored_file_info_table_records` |
| `tests/test_datastore.py::NullDatastoreTestCase::test_basics` | `tests/test_datastore_null.py::test_basics` |
| `tests/test_datastore.py::PosixDatastoreConstraintsTestCase::testConstraints` | `tests/test_datastore_constraints.py::` `test_constraints[posix-metric]`, `test_constraints[posix-metric5]`, `test_constraints[posix-metric33]`, `test_constraints[posix-metric5-json]` (four former subtests) |
| `tests/test_datastore.py::InMemoryDatastoreConstraintsTestCase::testConstraints` | `tests/test_datastore_constraints.py::` `test_constraints[in-memory-metric]`, `test_constraints[in-memory-metric5]`, `test_constraints[in-memory-metric33]`, `test_constraints[in-memory-metric5-json]` (four former subtests) |
| `tests/test_datastore.py::ChainedDatastoreConstraintsNativeTestCase::testConstraints` | `tests/test_datastore_constraints.py::` `test_constraints[chained-native-metric]`, `test_constraints[chained-native-metric5]`, `test_constraints[chained-native-metric33]`, `test_constraints[chained-native-metric5-json]` (four former subtests) |
| `tests/test_datastore.py::ChainedDatastoreConstraintsTestCase::testConstraints` | `tests/test_datastore_constraints.py::` `test_constraints[chained-metric]`, `test_constraints[chained-metric5]`, `test_constraints[chained-metric33]`, `test_constraints[chained-metric5-json]` (four former subtests) |
| `tests/test_datastore.py::ChainedDatastoreMemoryConstraintsTestCase::testConstraints` | `tests/test_datastore_constraints.py::` `test_constraints[chained-memory-metric]`, `test_constraints[chained-memory-metric5]`, `test_constraints[chained-memory-metric33]`, `test_constraints[chained-memory-metric5-json]` (four former subtests) |
| `tests/test_datastore.py::ChainedDatastorePerStoreConstraintsTests::testConstraints` | `tests/test_datastore_constraints.py::` `test_per_store_constraints[metric]`, `test_per_store_constraints[metric5]`, `test_per_store_constraints[metric5-hsc]`, `test_per_store_constraints[metric33]`, `test_per_store_constraints[metric5-json]` (five former subtests) |
| `tests/test_datastore.py::PosixDatastoreTestCase::testConfigRoot` | `tests/test_datastore_file.py::test_config_root[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testConstructor` | `tests/test_datastore_file.py::test_constructor[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testConfigurationValidation` | `tests/test_datastore_file.py::test_configuration_validation[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testParameterValidation` | `tests/test_datastore_file.py::test_parameter_validation[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testBasicPutGet` | `tests/test_datastore_file.py::test_basic_put_get[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testTrustGetRequest` | `tests/test_datastore_file.py::test_trust_get_request[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testDisassembly` | `tests/test_datastore_file.py::test_disassembly[posix-...]` (6 former subtests) |
| `tests/test_datastore.py::PosixDatastoreTestCase::testRemove` | `tests/test_datastore_file.py::test_remove[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testForget` | `tests/test_datastore_file.py::test_forget[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testTransfer` | `tests/test_datastore_file.py::test_transfer[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testBasicTransaction` | `tests/test_datastore_file.py::test_basic_transaction[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testNestedTransaction` | `tests/test_datastore_file.py::test_nested_transaction[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testIngestNoTransfer` | `tests/test_datastore_file.py::test_ingest_no_transfer[posix-...]` (2 former subtests) |
| `tests/test_datastore.py::PosixDatastoreTestCase::testIngestTransfer` | `tests/test_datastore_file.py::test_ingest_transfer[posix-...]` (7 former subtests) |
| `tests/test_datastore.py::PosixDatastoreTestCase::testIngestSymlinkOfSymlink` | `tests/test_datastore_file.py::test_ingest_symlink_of_symlink[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testExportImportRecords` | `tests/test_datastore_file.py::test_export_import_records[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testExportImportTable` | `tests/test_datastore_file.py::test_export_import_table[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testExportPredictedRecords` | `tests/test_datastore_file.py::test_export_predicted_records[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testExport` | `tests/test_datastore_file.py::test_export[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::test_pydantic_dict_storage_class_conversions` | `tests/test_datastore_file.py::test_pydantic_dict_storage_class_conversions[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::test_simple_class_put_get` | `tests/test_datastore_file.py::test_simple_class_put_get[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::test_dataclass_put_get` | `tests/test_datastore_file.py::test_dataclass_put_get[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::test_pydantic_put_get` | `tests/test_datastore_file.py::test_pydantic_put_get[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::test_tuple_put_get` | `tests/test_datastore_file.py::test_tuple_put_get[posix]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testConfigRoot` | `tests/test_datastore_file.py::test_config_root[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testConstructor` | `tests/test_datastore_file.py::test_constructor[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testConfigurationValidation` | `tests/test_datastore_file.py::test_configuration_validation[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testParameterValidation` | `tests/test_datastore_file.py::test_parameter_validation[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testBasicPutGet` | `tests/test_datastore_file.py::test_basic_put_get[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testTrustGetRequest` | `tests/test_datastore_file.py::test_trust_get_request[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testDisassembly` | `tests/test_datastore_file.py::test_disassembly[posix-no-checksums-...]` (6 former subtests) |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testRemove` | `tests/test_datastore_file.py::test_remove[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testForget` | `tests/test_datastore_file.py::test_forget[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testTransfer` | `tests/test_datastore_file.py::test_transfer[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testBasicTransaction` | `tests/test_datastore_file.py::test_basic_transaction[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testNestedTransaction` | `tests/test_datastore_file.py::test_nested_transaction[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testIngestNoTransfer` | `tests/test_datastore_file.py::test_ingest_no_transfer[posix-no-checksums-...]` (2 former subtests) |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testIngestTransfer` | `tests/test_datastore_file.py::test_ingest_transfer[posix-no-checksums-...]` (7 former subtests) |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testIngestSymlinkOfSymlink` | `tests/test_datastore_file.py::test_ingest_symlink_of_symlink[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testExportImportRecords` | `tests/test_datastore_file.py::test_export_import_records[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testExportImportTable` | `tests/test_datastore_file.py::test_export_import_table[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testExportPredictedRecords` | `tests/test_datastore_file.py::test_export_predicted_records[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testExport` | `tests/test_datastore_file.py::test_export[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::test_pydantic_dict_storage_class_conversions` | `tests/test_datastore_file.py::test_pydantic_dict_storage_class_conversions[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::test_simple_class_put_get` | `tests/test_datastore_file.py::test_simple_class_put_get[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::test_dataclass_put_get` | `tests/test_datastore_file.py::test_dataclass_put_get[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::test_pydantic_put_get` | `tests/test_datastore_file.py::test_pydantic_put_get[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::test_tuple_put_get` | `tests/test_datastore_file.py::test_tuple_put_get[posix-no-checksums]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testConfigRoot` | `tests/test_datastore_file.py::test_config_root[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testConstructor` | `tests/test_datastore_file.py::test_constructor[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testConfigurationValidation` | `tests/test_datastore_file.py::test_configuration_validation[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testParameterValidation` | `tests/test_datastore_file.py::test_parameter_validation[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testBasicPutGet` | `tests/test_datastore_file.py::test_basic_put_get[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testTrustGetRequest` | `tests/test_datastore_file.py::test_trust_get_request[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testDisassembly` | `tests/test_datastore_file.py::test_disassembly[trash-...]` (6 former subtests) |
| `tests/test_datastore.py::TrashDatastoreTestCase::testRemove` | `tests/test_datastore_file.py::test_remove[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testForget` | `tests/test_datastore_file.py::test_forget[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testTransfer` | `tests/test_datastore_file.py::test_transfer[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testBasicTransaction` | `tests/test_datastore_file.py::test_basic_transaction[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testNestedTransaction` | `tests/test_datastore_file.py::test_nested_transaction[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testIngestNoTransfer` | `tests/test_datastore_file.py::test_ingest_no_transfer[trash-...]` (2 former subtests) |
| `tests/test_datastore.py::TrashDatastoreTestCase::testIngestTransfer` | `tests/test_datastore_file.py::test_ingest_transfer[trash-...]` (7 former subtests) |
| `tests/test_datastore.py::TrashDatastoreTestCase::testIngestSymlinkOfSymlink` | `tests/test_datastore_file.py::test_ingest_symlink_of_symlink[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testExportImportRecords` | `tests/test_datastore_file.py::test_export_import_records[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testExportImportTable` | `tests/test_datastore_file.py::test_export_import_table[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testExportPredictedRecords` | `tests/test_datastore_file.py::test_export_predicted_records[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testExport` | `tests/test_datastore_file.py::test_export[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::test_pydantic_dict_storage_class_conversions` | `tests/test_datastore_file.py::test_pydantic_dict_storage_class_conversions[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::test_simple_class_put_get` | `tests/test_datastore_file.py::test_simple_class_put_get[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::test_dataclass_put_get` | `tests/test_datastore_file.py::test_dataclass_put_get[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::test_pydantic_put_get` | `tests/test_datastore_file.py::test_pydantic_put_get[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::test_tuple_put_get` | `tests/test_datastore_file.py::test_tuple_put_get[trash]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testConfigRoot` | `tests/test_datastore_file.py::test_config_root[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testConstructor` | `tests/test_datastore_file.py::test_constructor[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testConfigurationValidation` | `tests/test_datastore_file.py::test_configuration_validation[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testParameterValidation` | `tests/test_datastore_file.py::test_parameter_validation[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testBasicPutGet` | `tests/test_datastore_file.py::test_basic_put_get[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testTrustGetRequest` | `tests/test_datastore_file.py::test_trust_get_request[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testDisassembly` | `tests/test_datastore_file.py::test_disassembly[in-memory-...]` (6 former subtests) |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testRemove` | `tests/test_datastore_file.py::test_remove[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testForget` | `tests/test_datastore_file.py::test_forget[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testTransfer` | `tests/test_datastore_file.py::test_transfer[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testBasicTransaction` | `tests/test_datastore_file.py::test_basic_transaction[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testNestedTransaction` | `tests/test_datastore_file.py::test_nested_transaction[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testIngestNoTransfer` | `tests/test_datastore_file.py::test_ingest_no_transfer[in-memory-...]` (2 former subtests) |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testIngestTransfer` | `tests/test_datastore_file.py::test_ingest_transfer[in-memory-...]` (7 former subtests) |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testIngestSymlinkOfSymlink` | `tests/test_datastore_file.py::test_ingest_symlink_of_symlink[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testExportImportRecords` | `tests/test_datastore_file.py::test_export_import_records[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testExportImportTable` | `tests/test_datastore_file.py::test_export_import_table[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testExportPredictedRecords` | `tests/test_datastore_file.py::test_export_predicted_records[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::testExport` | `tests/test_datastore_file.py::test_export[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::test_pydantic_dict_storage_class_conversions` | `tests/test_datastore_file.py::test_pydantic_dict_storage_class_conversions[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::test_simple_class_put_get` | `tests/test_datastore_file.py::test_simple_class_put_get[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::test_dataclass_put_get` | `tests/test_datastore_file.py::test_dataclass_put_get[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::test_pydantic_put_get` | `tests/test_datastore_file.py::test_pydantic_put_get[in-memory]` |
| `tests/test_datastore.py::InMemoryDatastoreTestCase::test_tuple_put_get` | `tests/test_datastore_file.py::test_tuple_put_get[in-memory]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testConfigRoot` | `tests/test_datastore_file.py::test_config_root[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testConstructor` | `tests/test_datastore_file.py::test_constructor[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testConfigurationValidation` | `tests/test_datastore_file.py::test_configuration_validation[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testParameterValidation` | `tests/test_datastore_file.py::test_parameter_validation[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testBasicPutGet` | `tests/test_datastore_file.py::test_basic_put_get[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testTrustGetRequest` | `tests/test_datastore_file.py::test_trust_get_request[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testDisassembly` | `tests/test_datastore_file.py::test_disassembly[chained-...]` (6 former subtests) |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testRemove` | `tests/test_datastore_file.py::test_remove[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testForget` | `tests/test_datastore_file.py::test_forget[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testTransfer` | `tests/test_datastore_file.py::test_transfer[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testBasicTransaction` | `tests/test_datastore_file.py::test_basic_transaction[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testNestedTransaction` | `tests/test_datastore_file.py::test_nested_transaction[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testIngestNoTransfer` | `tests/test_datastore_file.py::test_ingest_no_transfer[chained-...]` (2 former subtests) |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testIngestTransfer` | `tests/test_datastore_file.py::test_ingest_transfer[chained-...]` (7 former subtests) |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testIngestSymlinkOfSymlink` | `tests/test_datastore_file.py::test_ingest_symlink_of_symlink[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testExportImportRecords` | `tests/test_datastore_file.py::test_export_import_records[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testExportImportTable` | `tests/test_datastore_file.py::test_export_import_table[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testExportPredictedRecords` | `tests/test_datastore_file.py::test_export_predicted_records[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testExport` | `tests/test_datastore_file.py::test_export[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::test_pydantic_dict_storage_class_conversions` | `tests/test_datastore_file.py::test_pydantic_dict_storage_class_conversions[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::test_simple_class_put_get` | `tests/test_datastore_file.py::test_simple_class_put_get[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::test_dataclass_put_get` | `tests/test_datastore_file.py::test_dataclass_put_get[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::test_pydantic_put_get` | `tests/test_datastore_file.py::test_pydantic_put_get[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::test_tuple_put_get` | `tests/test_datastore_file.py::test_tuple_put_get[chained]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testConfigRoot` | `tests/test_datastore_file.py::test_config_root[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testConstructor` | `tests/test_datastore_file.py::test_constructor[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testConfigurationValidation` | `tests/test_datastore_file.py::test_configuration_validation[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testParameterValidation` | `tests/test_datastore_file.py::test_parameter_validation[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testBasicPutGet` | `tests/test_datastore_file.py::test_basic_put_get[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testTrustGetRequest` | `tests/test_datastore_file.py::test_trust_get_request[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testDisassembly` | `tests/test_datastore_file.py::test_disassembly[chained-memory-...]` (6 former subtests) |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testRemove` | `tests/test_datastore_file.py::test_remove[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testForget` | `tests/test_datastore_file.py::test_forget[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testTransfer` | `tests/test_datastore_file.py::test_transfer[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testBasicTransaction` | `tests/test_datastore_file.py::test_basic_transaction[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testNestedTransaction` | `tests/test_datastore_file.py::test_nested_transaction[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testIngestNoTransfer` | `tests/test_datastore_file.py::test_ingest_no_transfer[chained-memory-...]` (2 former subtests) |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testIngestTransfer` | `tests/test_datastore_file.py::test_ingest_transfer[chained-memory-...]` (7 former subtests) |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testIngestSymlinkOfSymlink` | `tests/test_datastore_file.py::test_ingest_symlink_of_symlink[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testExportImportRecords` | `tests/test_datastore_file.py::test_export_import_records[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testExportImportTable` | `tests/test_datastore_file.py::test_export_import_table[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testExportPredictedRecords` | `tests/test_datastore_file.py::test_export_predicted_records[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::testExport` | `tests/test_datastore_file.py::test_export[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::test_pydantic_dict_storage_class_conversions` | `tests/test_datastore_file.py::test_pydantic_dict_storage_class_conversions[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::test_simple_class_put_get` | `tests/test_datastore_file.py::test_simple_class_put_get[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::test_dataclass_put_get` | `tests/test_datastore_file.py::test_dataclass_put_get[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::test_pydantic_put_get` | `tests/test_datastore_file.py::test_pydantic_put_get[chained-memory]` |
| `tests/test_datastore.py::ChainedDatastoreMemoryTestCase::test_tuple_put_get` | `tests/test_datastore_file.py::test_tuple_put_get[chained-memory]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testAtomicWrite` | `tests/test_datastore_file.py::test_atomic_write[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::testCanNotDeterminePutFormatterLocation` | `tests/test_datastore_file.py::test_can_not_determine_put_formatter_location[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::test_roots` | `tests/test_datastore_file.py::test_roots[posix]` |
| `tests/test_datastore.py::PosixDatastoreTestCase::test_prepare_get_for_external_client` | `tests/test_datastore_file.py::test_prepare_get_for_external_client[posix]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testAtomicWrite` | `tests/test_datastore_file.py::test_atomic_write[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testCanNotDeterminePutFormatterLocation` | `tests/test_datastore_file.py::test_can_not_determine_put_formatter_location[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::test_roots` | `tests/test_datastore_file.py::test_roots[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::test_prepare_get_for_external_client` | `tests/test_datastore_file.py::test_prepare_get_for_external_client[posix-no-checksums]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testAtomicWrite` | `tests/test_datastore_file.py::test_atomic_write[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testCanNotDeterminePutFormatterLocation` | `tests/test_datastore_file.py::test_can_not_determine_put_formatter_location[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::test_roots` | `tests/test_datastore_file.py::test_roots[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::test_prepare_get_for_external_client` | `tests/test_datastore_file.py::test_prepare_get_for_external_client[trash]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testAtomicWrite` | `tests/test_datastore_file.py::test_atomic_write[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::testCanNotDeterminePutFormatterLocation` | `tests/test_datastore_file.py::test_can_not_determine_put_formatter_location[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::test_roots` | `tests/test_datastore_file.py::test_roots[chained]` |
| `tests/test_datastore.py::ChainedDatastoreTestCase::test_prepare_get_for_external_client` | `tests/test_datastore_file.py::test_prepare_get_for_external_client[chained]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::testChecksum` | `tests/test_datastore_file.py::test_checksum[posix-no-checksums]` |
| `tests/test_datastore.py::PosixDatastoreNoChecksumsTestCase::test_repeat_ingest` | `tests/test_datastore_file.py::test_repeat_ingest[posix-no-checksums]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::testTrash` | `tests/test_datastore_file.py::test_trash[trash]` |
| `tests/test_datastore.py::TrashDatastoreTestCase::test_empty_trash` | `tests/test_datastore_file.py::test_empty_trash[trash]` |
| `tests/test_datastore.py::CleanupPosixDatastoreTestCase::testCleanup` | `tests/test_datastore_file.py::test_cleanup[posix]` |
