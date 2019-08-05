# This file is part of daf_butler.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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

__all__ = ("YamlFormatter", )

import builtins
import yaml

from lsst.daf.butler.formatters.fileFormatter import FileFormatter


class YamlFormatter(FileFormatter):
    """Interface for reading and writing Python objects to and from YAML files.
    """
    extension = ".yaml"

    unsupportedParameters = None
    """This formatter does not support any parameters"""

    def _readFile(self, path, pytype=None):
        """Read a file from the path in YAML format.

        Parameters
        ----------
        path : `str`
            Path to use to open YAML format file.

        Returns
        -------
        data : `object`
            Either data as Python object read from YAML file, or None
            if the file could not be opened.

        Notes
        -----
        The `~yaml.UnsafeLoader` is used when parsing the YAML file.
        """
        try:
            with open(path, "r") as fd:
                data = yaml.load(fd, Loader=yaml.UnsafeLoader)
        except FileNotFoundError:
            data = None

        return data

    def _writeFile(self, inMemoryDataset):
        """Write the in memory dataset to file on disk.

        Will look for `_asdict()` method to aid YAML serialization, following
        the approach of the simplejson module.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to serialize.

        Raises
        ------
        Exception
            The file could not be written.
        """
        with open(self.fileDescriptor.location.path, "w") as fd:
            if hasattr(inMemoryDataset, "_asdict"):
                inMemoryDataset = inMemoryDataset._asdict()
            yaml.dump(inMemoryDataset, stream=fd)

    def _coerceType(self, inMemoryDataset, storageClass, pytype=None):
        """Coerce the supplied inMemoryDataset to type `pytype`.

        Parameters
        ----------
        inMemoryDataset : `object`
            Object to coerce to expected type.
        storageClass : `StorageClass`
            StorageClass associated with `inMemoryDataset`.
        pytype : `type`, optional
            Override type to use for conversion.

        Returns
        -------
        inMemoryDataset : `object`
            Object of expected type `pytype`.
        """
        if not hasattr(builtins, pytype.__name__):
            if storageClass.isComposite():
                inMemoryDataset = storageClass.assembler().assemble(inMemoryDataset, pytype=pytype)
            else:
                # Hope that we can pass the arguments in directly
                inMemoryDataset = pytype(inMemoryDataset)
        return inMemoryDataset
