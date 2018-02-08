#
# LSST Data Management System
#
# Copyright 2018  AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <https://www.lsstcorp.org/LegalNotices/>.
#

import builtins
import json

from lsst.daf.butler.core.composites import genericAssembler
from lsst.daf.butler.core.formatter import Formatter


class JsonFormatter(Formatter):
    """Interface for reading and writing Python objects to and from JSON files.
    """

    def _getPath(self, fileDescriptor):
        """Form the path to the file, taking into account URI fragments.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            `FileDescriptor` specifying the path to use and URI.
        """
        filepath = fileDescriptor.location.path
        fragment = fileDescriptor.location.fragment
        if fragment:
            filepath = "{}#{}".format(filepath, fragment)
        return filepath

    def _readJson(self, path):
        """Read a file from the path in JSON format.

        Parameters
        ----------
        path : `str`
            Path to use to open JSON format file.

        Returns
        -------
        data : `object`
            Either data as Python object read from JSON file, or None
            if the file could not be opened.
        """
        try:
            with open(path, "r") as fd:
                data = json.load(fd)
        except FileNotFoundError:
            data = None

        return data

    def read(self, fileDescriptor):
        """Read a `Dataset`.

        Supports read of either a component that was written by `write` or
        a read of a component that was written as part of a single composite
        write, so long as the component name matching a getter in the
        composite.

        Parameters
        ----------
        fileDescriptor : `FileDescriptor`
            Identifies the file to read, type to read it into and parameters
            to be used for reading.

        Returns
        -------
        inMemoryDataset : `InMemoryDataset`
            The requested `Dataset`.
        """

        # Try the file or the component version
        path = self._getPath(fileDescriptor)
        data = self._readJson(path)
        name = fileDescriptor.location.fragment

        if name:
            if data is None:
                # Must be composite written as single file
                data = self._readJson(fileDescriptor.location.path)

                # It will be a dict since that is the only way for JSON
                # to serialize a composite.
                data = data[name]

            else:
                # The component was written standalone
                pass
        else:
            # Not requesting a component, so already read
            pass

        # Attempt to convert the simple python type from JSON to the expected
        # type. Do not do this if the output type is expected to be a simple
        # python type.
        if not hasattr(builtins, fileDescriptor.pytype.__name__):
            data = genericAssembler(fileDescriptor.storageClass, data, pytype=fileDescriptor.pytype)

        return data

    def write(self, inMemoryDataset, fileDescriptor):
        """Write an inMemoryDataset to a JSON file.

        The dataset will either be written directly, or if an `_asdict()`
        method is available, it will be converted to a `dict` before being
        serialized. The `_asdict()` method should be defined for all
        complex Python classes.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to serialize.
        fileDescriptor : `FileDescriptor`
            Information about the file output location and associated
            type information.

        Returns
        -------
        uri : `str`
            URI to primary storage location.
        components : `dict`
            Individual components accessible from this items. The keys are
            the component names matching those defined in the `StorageClass`
            associated with the `fileDescriptor` that are also present in the
            supplied inMemoryDataset,
            and the values are URIs that should be used to retrieve the
            components. Can be an empty `dict` if `StorageClass` defines
            no components.

        Notes
        -----
        `_asdict()` is the approach used by the `simplejson` package.
        """
        with open(self._getPath(fileDescriptor), "w") as fd:
            if hasattr(inMemoryDataset, "_asdict"):
                inMemoryDataset = inMemoryDataset._asdict()
            json.dump(inMemoryDataset, fd)

        # The reference components come from the StorageClass when determining
        # whether this object can be a composite.
        sc = fileDescriptor.storageClass
        comps = []
        if sc is not None and sc.components:
            for c in sc.components:
                if (hasattr(inMemoryDataset, c) or
                        (isinstance(inMemoryDataset, dict) and c in inMemoryDataset)):
                    comps.append(c)

        return (fileDescriptor.location.uri,
                {c: fileDescriptor.location.componentUri(c) for c in comps})
