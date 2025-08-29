# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This software is dual licensed under the GNU General Public License and also
# under a 3-clause BSD license. Recipients may choose which of these licenses
# to use; please see the files gpl-3.0.txt and/or bsd_license.txt,
# respectively.  If you choose the GPL option then the following text applies
# (but note that there is still no warranty even if you opt for BSD instead):
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ("QuantumBackedButler", "QuantumProvenanceData")

import itertools
import logging
import uuid
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any

import pydantic

from lsst.resources import ResourcePath, ResourcePathExpression

from ._butler_config import ButlerConfig
from ._butler_metrics import ButlerMetrics
from ._config import Config
from ._dataset_provenance import DatasetProvenance
from ._dataset_ref import DatasetId, DatasetRef
from ._dataset_type import DatasetType
from ._deferredDatasetHandle import DeferredDatasetHandle
from ._limited_butler import LimitedButler
from ._quantum import Quantum
from ._standalone_datastore import instantiate_standalone_datastore
from ._storage_class import StorageClass, StorageClassFactory
from .datastore import Datastore
from .datastore.record_data import DatastoreRecordData, SerializedDatastoreRecordData
from .datastores.file_datastore.retrieve_artifacts import retrieve_and_zip
from .dimensions import DimensionUniverse
from .registry.interfaces import DatastoreRegistryBridgeManager, OpaqueTableStorageManager

if TYPE_CHECKING:
    from ._butler import Butler

_LOG = logging.getLogger(__name__)


class QuantumBackedButler(LimitedButler):
    """An implementation of `LimitedButler` intended to back execution of a
    single `Quantum`.

    Parameters
    ----------
    predicted_inputs : `~collections.abc.Iterable` [`DatasetId`]
        Dataset IDs for datasets that can can be read from this butler.
    predicted_outputs : `~collections.abc.Iterable` [`DatasetId`]
        Dataset IDs for datasets that can be stored in this butler.
    dimensions : `DimensionUniverse`
        Object managing all dimension definitions.
    datastore : `Datastore`
        Datastore to use for all dataset I/O and existence checks.
    storageClasses : `StorageClassFactory`
        Object managing all storage class definitions.
    dataset_types : `~collections.abc.Mapping` [`str`, `DatasetType`]
        The registry dataset type definitions, indexed by name.
    metrics : `lsst.daf.butler.ButlerMetrics` or `None`, optional
        Metrics object for tracking butler statistics.

    Notes
    -----
    Most callers should use the `initialize` `classmethod` to construct new
    instances instead of calling the constructor directly.

    `QuantumBackedButler` uses a SQLite database internally, in order to reuse
    existing `DatastoreRegistryBridge` and `OpaqueTableStorage`
    implementations that rely SQLAlchemy.  If implementations are added in the
    future that don't rely on SQLAlchemy, it should be possible to swap them
    in by overriding the type arguments to `initialize` (though at present,
    `QuantumBackedButler` would still create at least an in-memory SQLite
    database that would then go unused).`

    We imagine `QuantumBackedButler` being used during (at least) batch
    execution to capture `Datastore` records and save them to per-quantum
    files, which are also a convenient place to store provenance for eventual
    upload to a SQL-backed `Registry` (once `Registry` has tables to store
    provenance, that is).
    These per-quantum files can be written in two ways:

    - The SQLite file used internally by `QuantumBackedButler` can be used
      directly but customizing the ``filename`` argument to ``initialize``, and
      then transferring that file to the object store after execution completes
      (or fails; a ``try/finally`` pattern probably makes sense here).

    - A JSON or YAML file can be written by calling `extract_provenance_data`,
      and using ``pydantic`` methods to write the returned
      `QuantumProvenanceData` to a file.

    Note that at present, the SQLite file only contains datastore records, not
    provenance, but that should be easy to address (if desired) after we
    actually design a `Registry` schema for provenance.  I also suspect that
    we'll want to explicitly close the SQLite file somehow before trying to
    transfer it.  But I'm guessing we'd prefer to write the per-quantum files
    as JSON anyway.
    """

    def __init__(
        self,
        predicted_inputs: Iterable[DatasetId],
        predicted_outputs: Iterable[DatasetId],
        dimensions: DimensionUniverse,
        datastore: Datastore,
        storageClasses: StorageClassFactory,
        dataset_types: Mapping[str, DatasetType] | None = None,
        metrics: ButlerMetrics | None = None,
    ):
        self._dimensions = dimensions
        self._predicted_inputs = set(predicted_inputs)
        self._predicted_outputs = set(predicted_outputs)
        self._available_inputs: set[DatasetId] = set()
        self._unavailable_inputs: set[DatasetId] = set()
        self._actual_inputs: set[DatasetId] = set()
        self._actual_output_refs: set[DatasetRef] = set()
        self._datastore = datastore
        self.storageClasses = storageClasses
        self._dataset_types: Mapping[str, DatasetType] = {}
        self._metrics = metrics if metrics is not None else ButlerMetrics()
        if dataset_types is not None:
            self._dataset_types = dataset_types
        self._datastore.set_retrieve_dataset_type_method(self._retrieve_dataset_type)

    @classmethod
    def initialize(
        cls,
        config: Config | ResourcePathExpression,
        quantum: Quantum,
        dimensions: DimensionUniverse,
        filename: str | None = None,
        OpaqueManagerClass: type[OpaqueTableStorageManager] | None = None,
        BridgeManagerClass: type[DatastoreRegistryBridgeManager] | None = None,
        search_paths: list[str] | None = None,
        dataset_types: Mapping[str, DatasetType] | None = None,
        metrics: ButlerMetrics | None = None,
    ) -> QuantumBackedButler:
        """Construct a new `QuantumBackedButler` from repository configuration
        and helper types.

        Parameters
        ----------
        config : `Config` or `~lsst.resources.ResourcePathExpression`
            A butler repository root, configuration filename, or configuration
            instance.
        quantum : `Quantum`
            Object describing the predicted input and output dataset relevant
            to this butler.  This must have resolved `DatasetRef` instances for
            all inputs and outputs.
        dimensions : `DimensionUniverse`
            Object managing all dimension definitions.
        filename : `str`, optional
            Name for the SQLite database that will back this butler; defaults
            to an in-memory database.
        OpaqueManagerClass : `type`, optional
            A subclass of `OpaqueTableStorageManager` to use for datastore
            opaque records.  Default is a SQL-backed implementation.
        BridgeManagerClass : `type`, optional
            A subclass of `DatastoreRegistryBridgeManager` to use for datastore
            location records.  Default is a SQL-backed implementation.
        search_paths : `list` of `str`, optional
            Additional search paths for butler configuration.
        dataset_types : `~collections.abc.Mapping` [`str`, `DatasetType`], \
                optional
            Mapping of the dataset type name to its registry definition.
        metrics : `lsst.daf.butler.ButlerMetrics` or `None`, optional
            Metrics object for gathering butler statistics.
        """
        predicted_inputs = [ref.id for ref in itertools.chain.from_iterable(quantum.inputs.values())]
        predicted_inputs += [ref.id for ref in quantum.initInputs.values()]
        predicted_outputs = [ref.id for ref in itertools.chain.from_iterable(quantum.outputs.values())]
        return cls._initialize(
            config=config,
            predicted_inputs=predicted_inputs,
            predicted_outputs=predicted_outputs,
            dimensions=dimensions,
            filename=filename,
            datastore_records=quantum.datastore_records,
            OpaqueManagerClass=OpaqueManagerClass,
            BridgeManagerClass=BridgeManagerClass,
            search_paths=search_paths,
            dataset_types=dataset_types,
            metrics=metrics,
        )

    @classmethod
    def from_predicted(
        cls,
        config: Config | ResourcePathExpression,
        predicted_inputs: Iterable[DatasetId],
        predicted_outputs: Iterable[DatasetId],
        dimensions: DimensionUniverse,
        datastore_records: Mapping[str, DatastoreRecordData],
        filename: str | None = None,
        OpaqueManagerClass: type[OpaqueTableStorageManager] | None = None,
        BridgeManagerClass: type[DatastoreRegistryBridgeManager] | None = None,
        search_paths: list[str] | None = None,
        dataset_types: Mapping[str, DatasetType] | None = None,
        metrics: ButlerMetrics | None = None,
    ) -> QuantumBackedButler:
        """Construct a new `QuantumBackedButler` from sets of input and output
        dataset IDs.

        Parameters
        ----------
        config : `Config` or `~lsst.resources.ResourcePathExpression`
            A butler repository root, configuration filename, or configuration
            instance.
        predicted_inputs : `~collections.abc.Iterable` [`DatasetId`]
            Dataset IDs for datasets that can can be read from this butler.
        predicted_outputs : `~collections.abc.Iterable` [`DatasetId`]
            Dataset IDs for datasets that can be stored in this butler, must be
            fully resolved.
        dimensions : `DimensionUniverse`
            Object managing all dimension definitions.
        datastore_records : `dict` [`str`, `DatastoreRecordData`] or `None`
            Datastore records to import into a datastore.
        filename : `str`, optional
            Name for the SQLite database that will back this butler; defaults
            to an in-memory database.
        OpaqueManagerClass : `type`, optional
            A subclass of `OpaqueTableStorageManager` to use for datastore
            opaque records.  Default is a SQL-backed implementation.
        BridgeManagerClass : `type`, optional
            A subclass of `DatastoreRegistryBridgeManager` to use for datastore
            location records.  Default is a SQL-backed implementation.
        search_paths : `list` of `str`, optional
            Additional search paths for butler configuration.
        dataset_types : `~collections.abc.Mapping` [`str`, `DatasetType`], \
                optional
            Mapping of the dataset type name to its registry definition.
        metrics : `lsst.daf.butler.ButlerMetrics` or `None`, optional
            Metrics object for gathering butler statistics.
        """
        return cls._initialize(
            config=config,
            predicted_inputs=predicted_inputs,
            predicted_outputs=predicted_outputs,
            dimensions=dimensions,
            filename=filename,
            datastore_records=datastore_records,
            OpaqueManagerClass=OpaqueManagerClass,
            BridgeManagerClass=BridgeManagerClass,
            search_paths=search_paths,
            dataset_types=dataset_types,
            metrics=metrics,
        )

    @classmethod
    def _initialize(
        cls,
        *,
        config: Config | ResourcePathExpression,
        predicted_inputs: Iterable[DatasetId],
        predicted_outputs: Iterable[DatasetId],
        dimensions: DimensionUniverse,
        filename: str | None = None,
        datastore_records: Mapping[str, DatastoreRecordData] | None = None,
        OpaqueManagerClass: type[OpaqueTableStorageManager] | None = None,
        BridgeManagerClass: type[DatastoreRegistryBridgeManager] | None = None,
        search_paths: list[str] | None = None,
        dataset_types: Mapping[str, DatasetType] | None = None,
        metrics: ButlerMetrics | None = None,
    ) -> QuantumBackedButler:
        """Initialize quantum-backed butler.

        Internal method with common implementation used by `initialize` and
        `for_output`.

        Parameters
        ----------
        config : `Config` or `~lsst.resources.ResourcePathExpression`
            A butler repository root, configuration filename, or configuration
            instance.
        predicted_inputs : `~collections.abc.Iterable` [`DatasetId`]
            Dataset IDs for datasets that can can be read from this butler.
        predicted_outputs : `~collections.abc.Iterable` [`DatasetId`]
            Dataset IDs for datasets that can be stored in this butler.
        dimensions : `DimensionUniverse`
            Object managing all dimension definitions.
        filename : `str`, optional
            Name for the SQLite database that will back this butler; defaults
            to an in-memory database.
        datastore_records : `dict` [`str`, `DatastoreRecordData`] or `None`
            Datastore records to import into a datastore.
        OpaqueManagerClass : `type`, optional
            A subclass of `OpaqueTableStorageManager` to use for datastore
            opaque records.  Default is a SQL-backed implementation.
        BridgeManagerClass : `type`, optional
            A subclass of `DatastoreRegistryBridgeManager` to use for datastore
            location records.  Default is a SQL-backed implementation.
        search_paths : `list` of `str`, optional
            Additional search paths for butler configuration.
        dataset_types : `~collections.abc.Mapping` [`str`, `DatasetType`]
            Mapping of the dataset type name to its registry definition.
        metrics : `lsst.daf.butler.ButlerMetrics` or `None`, optional
            Metrics object for gathering butler statistics.
        """
        butler_config = ButlerConfig(config, searchPaths=search_paths)
        datastore, _ = instantiate_standalone_datastore(
            butler_config, dimensions, filename, OpaqueManagerClass, BridgeManagerClass
        )

        # TODO: We need to inform `Datastore` here that it needs to support
        # predictive reads; This only really works for file datastore but
        # we need to try everything in case there is a chained datastore.
        datastore._set_trust_mode(True)

        if datastore_records is not None:
            datastore.import_records(datastore_records)
        storageClasses = StorageClassFactory()
        storageClasses.addFromConfig(butler_config)
        return cls(
            predicted_inputs,
            predicted_outputs,
            dimensions,
            datastore,
            storageClasses=storageClasses,
            dataset_types=dataset_types,
            metrics=metrics,
        )

    def _retrieve_dataset_type(self, name: str) -> DatasetType | None:
        """Return DatasetType defined in registry given dataset type name."""
        return self._dataset_types.get(name)

    def isWriteable(self) -> bool:
        # Docstring inherited.
        return True

    def get(
        self,
        ref: DatasetRef,
        /,
        *,
        parameters: dict[str, Any] | None = None,
        storageClass: StorageClass | str | None = None,
    ) -> Any:
        try:
            obj = super().get(
                ref,
                parameters=parameters,
                storageClass=storageClass,
            )
        except (LookupError, FileNotFoundError, OSError):
            self._unavailable_inputs.add(ref.id)
            raise
        if ref.id in self._predicted_inputs:
            # do this after delegating to super in case that raises.
            self._actual_inputs.add(ref.id)
            self._available_inputs.add(ref.id)
        return obj

    def getDeferred(
        self,
        ref: DatasetRef,
        /,
        *,
        parameters: dict[str, Any] | None = None,
        storageClass: str | StorageClass | None = None,
    ) -> DeferredDatasetHandle:
        if ref.id in self._predicted_inputs:
            # Unfortunately, we can't do this after the handle succeeds in
            # loading, so it's conceivable here that we're marking an input
            # as "actual" even when it's not even available.
            self._actual_inputs.add(ref.id)
        return super().getDeferred(ref, parameters=parameters, storageClass=storageClass)

    def stored(self, ref: DatasetRef) -> bool:
        # Docstring inherited.
        stored = super().stored(ref)
        if ref.id in self._predicted_inputs:
            if stored:
                self._available_inputs.add(ref.id)
            else:
                self._unavailable_inputs.add(ref.id)
        return stored

    def stored_many(
        self,
        refs: Iterable[DatasetRef],
    ) -> dict[DatasetRef, bool]:
        # Docstring inherited.
        existence = super().stored_many(refs)

        for ref, stored in existence.items():
            if ref.id in self._predicted_inputs:
                if stored:
                    self._available_inputs.add(ref.id)
                else:
                    self._unavailable_inputs.add(ref.id)
        return existence

    def markInputUnused(self, ref: DatasetRef) -> None:
        # Docstring inherited.
        self._actual_inputs.discard(ref.id)

    @property
    def dimensions(self) -> DimensionUniverse:
        # Docstring inherited.
        return self._dimensions

    def put(self, obj: Any, ref: DatasetRef, /, *, provenance: DatasetProvenance | None = None) -> DatasetRef:
        # Docstring inherited.
        if ref.id not in self._predicted_outputs:
            raise RuntimeError("Cannot `put` dataset that was not predicted as an output.")
        with self._metrics.instrument_put(log=_LOG, msg="Put QBB dataset"):
            self._datastore.put(obj, ref, provenance=provenance)
            self._actual_output_refs.add(ref)
        return ref

    def pruneDatasets(
        self,
        refs: Iterable[DatasetRef],
        *,
        disassociate: bool = True,
        unstore: bool = False,
        tags: Iterable[str] = (),
        purge: bool = False,
    ) -> None:
        # docstring inherited from LimitedButler

        if purge:
            if not disassociate:
                raise TypeError("Cannot pass purge=True without disassociate=True.")
            if not unstore:
                raise TypeError("Cannot pass purge=True without unstore=True.")
        elif disassociate:
            # No tagged collections for this butler.
            raise TypeError("Cannot pass disassociate=True without purge=True.")

        refs = list(refs)

        # Pruning a component of a DatasetRef makes no sense.
        for ref in refs:
            if ref.datasetType.component():
                raise ValueError(f"Can not prune a component of a dataset (ref={ref})")

        if unstore:
            self._datastore.trash(refs)
        if purge:
            for ref in refs:
                # We only care about removing them from actual output refs,
                self._actual_output_refs.discard(ref)

        if unstore:
            # Point of no return for removing artifacts. Only try to remove
            # refs associated with this pruning.
            self._datastore.emptyTrash(refs=refs)

    def retrieve_artifacts_zip(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePathExpression,
        overwrite: bool = True,
    ) -> ResourcePath:
        """Retrieve artifacts from the graph and place in ZIP file.

        Parameters
        ----------
        refs : `~collections.abc.Iterable` [ `DatasetRef` ]
            The datasets to be included in the zip file.
        destination : `lsst.resources.ResourcePathExpression`
            Directory to write the new ZIP file. This directory will
            also be used as a staging area for the datasets being downloaded
            from the datastore.
        overwrite : `bool`, optional
            If `False` the output Zip will not be written if a file of the
            same name is already present in ``destination``.

        Returns
        -------
        zip_file : `lsst.resources.ResourcePath`
            The path to the new ZIP file.

        Raises
        ------
        ValueError
            Raised if there are no refs to retrieve.
        """
        return retrieve_and_zip(refs, destination, self._datastore.retrieveArtifacts, overwrite)

    def retrieve_artifacts(
        self,
        refs: Iterable[DatasetRef],
        destination: ResourcePathExpression,
        transfer: str = "auto",
        preserve_path: bool = True,
        overwrite: bool = False,
    ) -> list[ResourcePath]:
        """Retrieve the artifacts associated with the supplied refs.

        Parameters
        ----------
        refs : iterable of `DatasetRef`
            The datasets for which artifacts are to be retrieved.
            A single ref can result in multiple artifacts. The refs must
            be resolved.
        destination : `lsst.resources.ResourcePath` or `str`
            Location to write the artifacts.
        transfer : `str`, optional
            Method to use to transfer the artifacts. Must be one of the options
            supported by `~lsst.resources.ResourcePath.transfer_from()`.
            "move" is not allowed.
        preserve_path : `bool`, optional
            If `True` the full path of the artifact within the datastore
            is preserved. If `False` the final file component of the path
            is used.
        overwrite : `bool`, optional
            If `True` allow transfers to overwrite existing files at the
            destination.

        Returns
        -------
        targets : `list` of `lsst.resources.ResourcePath`
            URIs of file artifacts in destination location. Order is not
            preserved.
        """
        outdir = ResourcePath(destination)
        artifact_map = self._datastore.retrieveArtifacts(
            refs,
            outdir,
            transfer=transfer,
            preserve_path=preserve_path,
            overwrite=overwrite,
            write_index=True,
        )
        return list(artifact_map)

    def extract_provenance_data(self) -> QuantumProvenanceData:
        """Extract provenance information and datastore records from this
        butler.

        Returns
        -------
        provenance : `QuantumProvenanceData`
            A serializable struct containing input/output dataset IDs and
            datastore records.  This assumes all dataset IDs are UUIDs (just to
            make it easier for `pydantic` to reason about the struct's types);
            the rest of this class makes no such assumption, but the approach
            to processing in which it's useful effectively requires UUIDs
            anyway.

        Notes
        -----
        `QuantumBackedButler` records this provenance information when its
        methods are used, which mostly saves `~lsst.pipe.base.PipelineTask`
        authors from having to worry about while still recording very
        detailed information.  But it has two small weaknesses:

        - Calling `getDeferred` or `get` is enough to mark a
          dataset as an "actual input", which may mark some datasets that
          aren't actually used.  We rely on task authors to use
          `markInputUnused` to address this.

        - We assume that the execution system will call ``stored``
          on all predicted inputs prior to execution, in order to populate the
          "available inputs" set.  This is what I envision
          '`~lsst.ctrl.mpexec.SingleQuantumExecutor` doing after we update it
          to use this class, but it feels fragile for this class to make such
          a strong assumption about how it will be used, even if I can't think
          of any other executor behavior that would make sense.
        """
        if not self._actual_inputs.isdisjoint(self._unavailable_inputs):
            _LOG.warning(
                "Inputs %s were marked as actually used (probably because a DeferredDatasetHandle) "
                "was obtained, but did not actually exist.  This task should be be using markInputUnused "
                "directly to clarify its provenance.",
                self._actual_inputs & self._unavailable_inputs,
            )
            self._actual_inputs -= self._unavailable_inputs
        checked_inputs = self._available_inputs | self._unavailable_inputs
        if self._predicted_inputs != checked_inputs:
            _LOG.warning(
                "Execution harness did not check predicted inputs %s for existence; available inputs "
                "recorded in provenance may be incomplete.",
                self._predicted_inputs - checked_inputs,
            )
        datastore_records = self._datastore.export_records(self._actual_output_refs)
        provenance_records = {
            datastore_name: records.to_simple() for datastore_name, records in datastore_records.items()
        }

        return QuantumProvenanceData(
            predicted_inputs=self._predicted_inputs,
            available_inputs=self._available_inputs,
            actual_inputs=self._actual_inputs,
            predicted_outputs=self._predicted_outputs,
            actual_outputs={ref.id for ref in self._actual_output_refs},
            datastore_records=provenance_records,
        )


class QuantumProvenanceData(pydantic.BaseModel):
    """A serializable struct for per-quantum provenance information and
    datastore records.

    Notes
    -----
    This class slightly duplicates information from the `Quantum` class itself
    (the ``predicted_inputs`` and ``predicted_outputs`` sets should have the
    same IDs present in `Quantum.inputs` and `Quantum.outputs`), but overall it
    assumes the original `Quantum` is also available to reconstruct the
    complete provenance (e.g. by associating dataset IDs with data IDs,
    dataset types, and `~CollectionType.RUN` names.

    Note that ``pydantic`` method ``parse_raw()`` is not going to work
    correctly for this class, use `direct` method instead.
    """

    # This class probably should have information about its execution
    # environment (anything not controlled and recorded at the
    # `~CollectionType.RUN` level, such as the compute node ID). but adding it
    # now is out of scope for this prototype.

    predicted_inputs: set[uuid.UUID]
    """Unique IDs of datasets that were predicted as inputs to this quantum
    when the QuantumGraph was built.
    """

    available_inputs: set[uuid.UUID]
    """Unique IDs of input datasets that were actually present in the datastore
    when this quantum was executed.

    This is a subset of ``predicted_inputs``, with the difference generally
    being datasets were ``predicted_outputs`` but not ``actual_outputs`` of
    some upstream task.
    """

    actual_inputs: set[uuid.UUID]
    """Unique IDs of datasets that were actually used as inputs by this task.

    This is a subset of ``available_inputs``.

    Notes
    -----
    The criteria for marking an input as used is that rerunning the quantum
    with only these ``actual_inputs`` available must yield identical outputs.
    This means that (for example) even just using an input to help determine
    an output rejection criteria and then rejecting it as an outlier qualifies
    that input as actually used.
    """

    predicted_outputs: set[uuid.UUID]
    """Unique IDs of datasets that were predicted as outputs of this quantum
    when the QuantumGraph was built.
    """

    actual_outputs: set[uuid.UUID]
    """Unique IDs of datasets that were actually written when this quantum
    was executed.
    """

    datastore_records: dict[str, SerializedDatastoreRecordData]
    """Datastore records indexed by datastore name."""

    @staticmethod
    def collect_and_transfer(
        butler: Butler, quanta: Iterable[Quantum], provenance: Iterable[QuantumProvenanceData]
    ) -> None:
        """Transfer output datasets from multiple quanta to a more permanent
        `Butler` repository.

        Parameters
        ----------
        butler : `Butler`
            Full butler representing the data repository to transfer datasets
            to.
        quanta : `~collections.abc.Iterable` [ `Quantum` ]
            Iterable of `Quantum` objects that carry information about
            predicted outputs.  May be a single-pass iterator.
        provenance : `~collections.abc.Iterable` [ `QuantumProvenanceData` ]
            Provenance and datastore data for each of the given quanta, in the
            same order.  May be a single-pass iterator.

        Notes
        -----
        Input-output provenance data is not actually transferred yet, because
        `Registry` has no place to store it.

        This method probably works most efficiently if run on all quanta for a
        single task label at once, because this will gather all datasets of
        a particular type together into a single vectorized `Registry` import.
        It should still behave correctly if run on smaller groups of quanta
        or even quanta from multiple tasks.

        Currently this method transfers datastore record data unchanged, with
        no possibility of actually moving (e.g.) files.  Datastores that are
        present only in execution or only in the more permanent butler are
        ignored.
        """
        grouped_refs = defaultdict(list)
        summary_records: dict[str, DatastoreRecordData] = {}
        for quantum, provenance_for_quantum in zip(quanta, provenance, strict=True):
            quantum_refs_by_id = {
                ref.id: ref
                for ref in itertools.chain.from_iterable(quantum.outputs.values())
                if ref.id in provenance_for_quantum.actual_outputs
            }
            for ref in quantum_refs_by_id.values():
                grouped_refs[ref.datasetType, ref.run].append(ref)

            # merge datastore records into a summary structure
            for datastore_name, serialized_records in provenance_for_quantum.datastore_records.items():
                quantum_records = DatastoreRecordData.from_simple(serialized_records)
                if (records := summary_records.get(datastore_name)) is not None:
                    records.update(quantum_records)
                else:
                    summary_records[datastore_name] = quantum_records

        for refs in grouped_refs.values():
            butler.registry._importDatasets(refs)
        butler._datastore.import_records(summary_records)

    @classmethod
    def parse_raw(cls, *args: Any, **kwargs: Any) -> QuantumProvenanceData:
        raise NotImplementedError("parse_raw() is not usable for this class, use direct() instead.")

    @classmethod
    def direct(
        cls,
        *,
        predicted_inputs: Iterable[str | uuid.UUID],
        available_inputs: Iterable[str | uuid.UUID],
        actual_inputs: Iterable[str | uuid.UUID],
        predicted_outputs: Iterable[str | uuid.UUID],
        actual_outputs: Iterable[str | uuid.UUID],
        datastore_records: Mapping[str, Mapping],
    ) -> QuantumProvenanceData:
        """Construct an instance directly without validators.

        Parameters
        ----------
        predicted_inputs : `~collections.abc.Iterable` of `str` or `uuid.UUID`
            The predicted inputs.
        available_inputs : `~collections.abc.Iterable` of `str` or `uuid.UUID`
            The available inputs.
        actual_inputs : `~collections.abc.Iterable` of `str` or `uuid.UUID`
            The actual inputs.
        predicted_outputs : `~collections.abc.Iterable` of `str` or `uuid.UUID`
            The predicted outputs.
        actual_outputs : `~collections.abc.Iterable` of `str` or `uuid.UUID`
            The actual outputs.
        datastore_records : `~collections.abc.Mapping` [ `str`, \
                `~collections.abc.Mapping` ]
            The datastore records.

        Returns
        -------
        provenance : `QuantumProvenanceData`
            Serializable model of the quantum provenance.

        Notes
        -----
        This differs from the Pydantic "construct" method in that the
        arguments are explicitly what the model requires, and it will recurse
        through members, constructing them from their corresponding `direct`
        methods.

        This method should only be called when the inputs are trusted.
        """

        def _to_uuid_set(uuids: Iterable[str | uuid.UUID]) -> set[uuid.UUID]:
            """Convert input UUIDs, which could be in string representation to
            a set of `UUID` instances.
            """
            return {uuid.UUID(id) if isinstance(id, str) else id for id in uuids}

        data = cls.model_construct(
            predicted_inputs=_to_uuid_set(predicted_inputs),
            available_inputs=_to_uuid_set(available_inputs),
            actual_inputs=_to_uuid_set(actual_inputs),
            predicted_outputs=_to_uuid_set(predicted_outputs),
            actual_outputs=_to_uuid_set(actual_outputs),
            datastore_records={
                key: SerializedDatastoreRecordData.direct(**records)
                for key, records in datastore_records.items()
            },
        )

        return data
