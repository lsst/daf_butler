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

__all__ = ("PackagesFormatter",)

from typing import Any

from lsst.daf.butler import FormatterV2
from lsst.resources import ResourcePath
from lsst.utils.packages import Packages


class PackagesFormatter(FormatterV2):
    """Interface for reading and writing `~lsst.utils.packages.Packages`.

    This formatter supports write parameters:

    * ``format``: The file format to use to write the package data. Allowed
      options are ``yaml``, ``json``, and ``pickle``.
    """

    supported_write_parameters = frozenset({"format"})
    supported_extensions = frozenset({".yaml", ".pickle", ".pkl", ".json"})
    can_read_from_uri = True

    def get_write_extension(self) -> str:
        # Default to YAML but allow configuration via write parameter
        format = self.write_parameters.get("format", "yaml")
        ext = "." + format
        if ext not in self.supported_extensions:
            raise RuntimeError(f"Requested file format '{format}' is not supported for Packages")
        return ext

    def read_from_uri(self, uri: ResourcePath, component: str | None = None, expected_size: int = -1) -> Any:
        # Read the full file using the class associated with the
        # storage class it was originally written with.
        # Read the bytes directly from resource. These are not going to be
        # large.
        pytype = self.file_descriptor.storageClass.pytype
        assert issubclass(pytype, Packages)  # for mypy
        format = uri.getExtension().lstrip(".")  # .yaml -> yaml
        return pytype.fromBytes(uri.read(), format)

    def to_bytes(self, in_memory_dataset: Any) -> bytes:
        """Write the in memory dataset to a bytestring.

        Parameters
        ----------
        in_memory_dataset : `object`
            Object to serialize.

        Returns
        -------
        serializedDataset : `bytes`
            YAML string encoded to bytes.

        Raises
        ------
        Exception
            The object could not be serialized.
        """
        format = self.get_write_extension().lstrip(".")
        return in_memory_dataset.toBytes(format)
