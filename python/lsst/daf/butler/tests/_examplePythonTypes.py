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

"""
Python classes that can be used to test datastores without requiring
large external dependencies on python classes such as afw or serialization
formats such as FITS or HDF5.
"""

from __future__ import annotations

__all__ = (
    "ListDelegate",
    "MetricsDelegate",
    "MetricsExample",
    "registerMetricsExample",
    "MetricsExampleModel",
    "MetricsExampleDataclass",
)


import copy
import dataclasses
import types
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from lsst.daf.butler import StorageClass, StorageClassDelegate
from pydantic import BaseModel

if TYPE_CHECKING:
    from lsst.daf.butler import Butler, Datastore, FormatterFactory


def registerMetricsExample(butler: Butler) -> None:
    """Modify a repository to support reading and writing
    `MetricsExample` objects.

    This method allows `MetricsExample` to be used with test repositories
    in any package without needing to provide a custom configuration there.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The repository that needs to support `MetricsExample`.

    Notes
    -----
    This method enables the following storage classes:

    ``StructuredData``
        A `MetricsExample` whose ``summary``, ``output``, and ``data`` members
        can be retrieved as dataset components.
    ``StructuredDataNoComponents``
        A monolithic write of a `MetricsExample`.

    These definitions must match the equivalent definitions in the test YAML
    files.
    """
    yamlDict = _addFullStorageClass(
        butler,
        "StructuredDataDictYaml",
        "lsst.daf.butler.formatters.yaml.YamlFormatter",
        pytype=dict,
    )

    yamlList = _addFullStorageClass(
        butler,
        "StructuredDataListYaml",
        "lsst.daf.butler.formatters.yaml.YamlFormatter",
        pytype=list,
        parameters={"slice"},
        delegate="lsst.daf.butler.tests.ListDelegate",
    )

    _addFullStorageClass(
        butler,
        "StructuredDataNoComponents",
        "lsst.daf.butler.formatters.pickle.PickleFormatter",
        pytype=MetricsExample,
        parameters={"slice"},
        delegate="lsst.daf.butler.tests.MetricsDelegate",
        converters={"dict": "lsst.daf.butler.tests.MetricsExample.makeFromDict"},
    )

    _addFullStorageClass(
        butler,
        "StructuredData",
        "lsst.daf.butler.formatters.yaml.YamlFormatter",
        pytype=MetricsExample,
        components={
            "summary": yamlDict,
            "output": yamlDict,
            "data": yamlList,
        },
        delegate="lsst.daf.butler.tests.MetricsDelegate",
    )


def _addFullStorageClass(butler: Butler, name: str, formatter: str, **kwargs: Any) -> StorageClass:
    """Create a storage class-formatter pair in a repository if it does not
    already exist.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        The repository that needs to contain the class.
    name : `str`
        The name to use for the class.
    formatter : `str`
        The formatter to use with the storage class. Ignored if ``butler``
        does not use formatters.
    **kwargs
        Arguments, other than ``name``, to the `~lsst.daf.butler.StorageClass`
        constructor.

    Returns
    -------
    class : `lsst.daf.butler.StorageClass`
        The newly created storage class, or the class of the same name
        previously found in the repository.
    """
    storageRegistry = butler._datastore.storageClassFactory

    # Use the special constructor to allow a subclass of storage class
    # to be created. This allows other test storage classes to inherit from
    # this one.
    storage_type = storageRegistry.makeNewStorageClass(name, None, **kwargs)
    storage = storage_type()
    try:
        storageRegistry.registerStorageClass(storage)
    except ValueError:
        storage = storageRegistry.getStorageClass(name)

    for registry in _getAllFormatterRegistries(butler._datastore):
        registry.registerFormatter(storage, formatter)

    return storage


def _getAllFormatterRegistries(datastore: Datastore) -> list[FormatterFactory]:
    """Return all formatter registries used by a datastore.

    Parameters
    ----------
    datastore : `lsst.daf.butler.Datastore`
        A datastore containing zero or more formatter registries.

    Returns
    -------
    registries : `list` [`lsst.daf.butler.FormatterFactory`]
        A possibly empty list of all formatter registries used
        by ``datastore``.
    """
    try:
        datastores = datastore.datastores  # type: ignore[attr-defined]
    except AttributeError:
        datastores = [datastore]

    registries = []
    for datastore in datastores:
        try:
            # Not all datastores have a formatterFactory
            formatterRegistry = datastore.formatterFactory  # type: ignore[attr-defined]
        except AttributeError:
            pass  # no formatter needed
        else:
            registries.append(formatterRegistry)
    return registries


class MetricsExample:
    """Smorgasboard of information that might be the result of some
    processing.

    Parameters
    ----------
    summary : `dict`
        Simple dictionary mapping key performance metrics to a scalar
        result.
    output : `dict`
        Structured nested data.
    data : `list`, optional
        Arbitrary array data.
    """

    def __init__(
        self,
        summary: dict[str, Any] | None = None,
        output: dict[str, Any] | None = None,
        data: list[Any] | None = None,
    ) -> None:
        self.summary = summary
        self.output = output
        self.data = data

    def __eq__(self, other: Any) -> bool:
        try:
            return self.summary == other.summary and self.output == other.output and self.data == other.data
        except AttributeError:
            pass
        return NotImplemented

    def __str__(self) -> str:
        return str(self.exportAsDict())

    def __repr__(self) -> str:
        return f"MetricsExample({self.exportAsDict()})"

    def exportAsDict(self) -> dict[str, list | dict | None]:
        """Convert object contents to a single python dict."""
        exportDict: dict[str, list | dict | None] = {"summary": self.summary, "output": self.output}
        if self.data is not None:
            exportDict["data"] = list(self.data)
        else:
            exportDict["data"] = None
        return exportDict

    def _asdict(self) -> dict[str, list | dict | None]:
        """Convert object contents to a single Python dict.

        This interface is used for JSON serialization.

        Returns
        -------
        exportDict : `dict`
            Object contents in the form of a dict with keys corresponding
            to object attributes.
        """
        return self.exportAsDict()

    @classmethod
    def makeFromDict(cls, exportDict: dict[str, list | dict | None]) -> MetricsExample:
        """Create a new object from a dict that is compatible with that
        created by `exportAsDict`.

        Parameters
        ----------
        exportDict : `dict`
            `dict` with keys "summary", "output", and (optionally) "data".

        Returns
        -------
        newobject : `MetricsExample`
            New `MetricsExample` object.
        """
        data = exportDict["data"] if "data" in exportDict else None
        assert isinstance(data, list | types.NoneType)
        assert isinstance(exportDict["summary"], dict | types.NoneType)
        assert isinstance(exportDict["output"], dict | types.NoneType)
        return cls(exportDict["summary"], exportDict["output"], data)


class MetricsExampleModel(BaseModel):
    """A variant of `MetricsExample` based on model."""

    summary: dict[str, Any] | None = None
    output: dict[str, Any] | None = None
    data: list[Any] | None = None

    @classmethod
    def from_metrics(cls, metrics: MetricsExample) -> MetricsExampleModel:
        """Create a model based on an example."""
        d = metrics.exportAsDict()
        # Assume pydantic v2 but fallback to v1
        try:
            return cls.model_validate(d)  # type: ignore
        except AttributeError:
            return cls.parse_obj(d)


@dataclasses.dataclass
class MetricsExampleDataclass:
    """A variant of `MetricsExample` based on a dataclass."""

    summary: dict[str, Any] | None
    output: dict[str, Any] | None
    data: list[Any] | None


class ListDelegate(StorageClassDelegate):
    """Parameter handler for list parameters."""

    def handleParameters(self, inMemoryDataset: Any, parameters: Mapping[str, Any] | None = None) -> Any:
        """Modify the in-memory dataset using the supplied parameters,
        returning a possibly new object.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to modify based on the parameters.
        parameters : `dict`
            Parameters to apply. Values are specific to the parameter.
            Supported parameters are defined in the associated
            `StorageClass`.  If no relevant parameters are specified the
            inMemoryDataset will be return unchanged.

        Returns
        -------
        inMemoryDataset : `object`
            Updated form of supplied in-memory dataset, after parameters
            have been used.
        """
        inMemoryDataset = copy.deepcopy(inMemoryDataset)
        use = self.storageClass.filterParameters(parameters, subset={"slice"})
        if use:
            inMemoryDataset = inMemoryDataset[use["slice"]]
        return inMemoryDataset


class MetricsDelegate(StorageClassDelegate):
    """Parameter handler for parameters using Metrics."""

    def handleParameters(self, inMemoryDataset: Any, parameters: Mapping[str, Any] | None = None) -> Any:
        """Modify the in-memory dataset using the supplied parameters,
        returning a possibly new object.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to modify based on the parameters.
        parameters : `dict`
            Parameters to apply. Values are specific to the parameter.
            Supported parameters are defined in the associated
            `StorageClass`.  If no relevant parameters are specified the
            inMemoryDataset will be return unchanged.

        Returns
        -------
        inMemoryDataset : `object`
            Updated form of supplied in-memory dataset, after parameters
            have been used.
        """
        inMemoryDataset = copy.deepcopy(inMemoryDataset)
        use = self.storageClass.filterParameters(parameters, subset={"slice"})
        if use:
            inMemoryDataset.data = inMemoryDataset.data[use["slice"]]
        return inMemoryDataset

    def getComponent(self, composite: Any, componentName: str) -> Any:
        if componentName == "counter":
            return len(composite.data)
        return super().getComponent(composite, componentName)

    @classmethod
    def selectResponsibleComponent(cls, readComponent: str, fromComponents: set[str | None]) -> str:
        forwarderMap = {
            "counter": "data",
        }
        forwarder = forwarderMap.get(readComponent)
        if forwarder is not None and forwarder in fromComponents:
            return forwarder
        raise ValueError(f"Can not calculate read component {readComponent} from {fromComponents}")
