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

## Findings for separate tickets

| Finding | Where | Why not fixed here |
| --- | --- | --- |
| `useTempRoot` is a dead class attribute | `tests/test_butler.py:625` sets it `True`, `:2697` sets it `False`, and nothing ever reads it | Dropped rather than ported. Test-only, so no library change and no ticket needed. |
| `assert type(a) != type(b)` trips E721 | `assertButlerTransfers`, from the ruff autofix of `assertNotEqual(type(a), type(b))` | Rewritten `is not`, which is what the comparison meant. |
| `self.id = 0` is dead state | `DatastoreCacheTestCase.setUp` assigned it and nothing read it; `DatasetTestHelper.makeDatasetRef` does not use instance state | Dropped rather than ported. Test-only. |
| `runPutGetTest`'s `args = tuple[DatasetRef] \| tuple[str \| DatasetType, DataCoordinate]` | `tests/test_butler.py:306`, immediately before the loop that rebinds `args` | A type expression assigned as a value, so it did nothing. Written as the annotation it was meant to be. Test-only, so no library change and no ticket needed. |
| `retrieveArtifacts(transfer="move")` raises different messages per client | `FileDatastore` says "Can not move artifacts out of datastore", `RemoteButler` says "Only 'copy' and 'auto' transfer modes are supported" | The original bare `assertRaises(ValueError)` hid this. `PT011` forces a `match`, so the converted assertion names both. Not a bug, but the two implementations could usefully agree; not worth a ticket on its own. |

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
