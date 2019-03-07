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

__all__ = ("Extractor",)

import re
from collections import OrderedDict

from .structures import Gen2Dataset, Gen2DatasetType

# Regular expression that matches a single substitution in
# Gen2 CameraMapper template, such as "%(tract)04d".
TEMPLATE_RE = re.compile(r"\%\((?P<name>\w+)\)[^\%]*?(?P<type>[idrs])")


class FilePathParser:
    """A callable object that extracts Gen2Dataset instances from filenames
    corresponding to a particular Gen2 DatasetType.

    External code should use the `fromMapping` method to construct instances.

    Parameters
    ----------
    datasetType : `Gen2DatasetType`
        Information about the DatasetType this parser processes.
    regex : regular expression object
        Regular expression pattern with named groups for all data ID keys.
    """

    @classmethod
    def fromMapping(cls, mapping):
        """Construct a FilePathParser instance from a Gen2
        `lsst.obs.base.Mapping` instance.
        """
        try:
            template = mapping.template
        except RuntimeError:
            return None
        datasetType = Gen2DatasetType(name=mapping.datasetType,
                                      keys={},
                                      persistable=mapping.persistable,
                                      python=mapping.python)
        # The template string is something like
        # "deepCoadd/%(tract)04d-%(patch)s/%(filter)s"; each step of this
        # iterator corresponds to a %-tagged substitution string.
        # Our goal in all of this parsing is to turn the template into a regex
        # we can use to extract the associated values when matching strings
        # generated with the template.
        last = 0
        terms = []
        allKeys = mapping.keys()
        for match in TEMPLATE_RE.finditer(template):
            # Copy the (escaped) regular string between the last substitution
            # and this one to the terms that will form the regex.
            terms.append(re.escape(template[last:match.start()]))
            # Pull out the data ID key from the name used in the
            # subistution string.  Use that and the substition
            # type to come up with the pattern to use in the regex.
            name = match.group("name")
            if name == "patch":
                pattern = r"\d+,\d+"
            elif match.group("type") in "id":  # integers
                pattern = r"0*\d+"
            else:
                pattern = ".+"
            # only use named groups for the first occurence of a key
            if name not in datasetType.keys:
                terms.append(r"(?P<%s>%s)" % (name, pattern))
                datasetType.keys[name] = allKeys[name]
            else:
                terms.append(r"(%s)" % pattern)
            # Remember the end of this match
            last = match.end()
        # Append anything remaining after the last substitution string
        # to the regex.
        terms.append(re.escape(template[last:]))
        return cls(datasetType=datasetType, regex=re.compile("".join(terms)))

    def __init__(self, datasetType, regex):
        self.datasetType = datasetType
        self.regex = regex

    def __call__(self, filePath, root):
        """Extract a Gen2Dataset instance from the given path.

        Parameters
        ----------
        filePath : `str`
            Path and filename relative to `root`.
        root : `str`
            Absolute path to the root of the Gen2 data repository containing
            this file.
        """
        m = self.regex.fullmatch(filePath)
        if m is None:
            return None
        dataId = {k: v(m.group(k)) for k, v in self.datasetType.keys.items()}
        return Gen2Dataset(datasetType=self.datasetType, dataId=dataId,
                           filePath=filePath, root=root)


class Extractor:
    """A callable object that parses Gen2 paths into Gen2Dataset instances for
    a particular Gen2 data repository.

    Parameters
    ----------
    repo : `Gen2Repo`
        Structure describing the repository this Extractor will process.
    """

    def __init__(self, repo):
        self.repo = repo
        self.parsers = OrderedDict()
        for mapping in self.repo.mapper.mappings.values():
            parser = FilePathParser.fromMapping(mapping)
            if parser is not None:
                self.parsers[parser.datasetType.name] = parser

    def __call__(self, filePath):
        """Parse a file path and return a Gen2Dataset that represents it.

        Parameters
        ----------
        filePath : `str`
            A path relative to the root of the data repository.

        Returns
        -------
        dataset : `Gen2Dataset` or None
            A Gen2Dataset instance, or None if the file path is not recognized
            by this mapper.
        """
        for parser in self.parsers.values():
            dataset = parser(filePath, root=self.repo.root)
            if dataset is not None:
                break
        else:
            return None
        # Move the parser we just used to the front of the OrderedDict so we
        # always try them in MRU order.
        self.parsers.move_to_end(dataset.datasetType.name, last=False)
        return dataset

    def getDatasetTypes(self):
        """Return a dict mapping DatasetType name to Gen2DatasetType
        instance."""
        return {parser.datasetType.name: parser.datasetType for parser in self.parsers.values()}
