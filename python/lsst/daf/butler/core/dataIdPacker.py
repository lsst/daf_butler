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

__all__ = ("DataIdPacker", "DataIdPackerDimensions")

from abc import ABCMeta, abstractmethod
from lsst.utils import doImport

from .dimensions import DataId


class DataIdPackerDimensions:
    """A helper class that holds the three categories of dimensions associated
    with `DataIdPacker` objects.

    Parameters
    ----------
    given : `DimensionGraph`
        The dimensions that must be provided to construct a `DataIdPacker`.
        The packed ID values are only unique and reversible with these
        dimensions held fixed.
    required : `DimensionGraph`
        The dimensions that are identified by the `DataId` passed to
        `DataIdPacker.pack` or returned by `DataIdPacker.unpack`.
    """

    @classmethod
    def fromConfig(cls, universe, config):
        """Construct from a `Config` subset.

        Parameters
        ----------
        universe : `DimensionGraph`
            A graph containing all known `Dimension` instances.
        config : `Config`
            A config with "given" and "covered" keys that map
            to lists of `Dimension` names.
        """
        given = universe.extract(config["given"])
        required = universe.extract(config["required"])
        return cls(given=given, required=required)

    def __init__(self, given, required):
        self._given = given
        self._required = required

    @property
    def universe(self):
        """A graph containing all known dimensions (`DimensionGraph`).
        """
        return self._given.universe

    @property
    def given(self):
        """The dimensions that must be provided to construct a `DataIdPacker`
        (`DimensionSet`)

        The packed ID values are only unique and reversible with these
        dimensions held fixed.
        """
        return self._given

    @property
    def required(self):
        """The dimensions that must be present in a `DataId` for it to be
        packed (`DimensionSet`).
        """
        return self._required


class DataIdPacker(metaclass=ABCMeta):
    """An abstract base class for a bidirectional mappings between a `DataId`
    and a packed integer or `bytes` blob.

    Derived class constructors must accept at least the positional argments
    required by the base class contructor, but may accept additional keyword
    subclass-specific arguments as well (see `configure`).

    Parameters
    ----------
    dimensions : `DataIdPackerDimensions`
        Struct containing dimensions related to this `DataIdPacker`.
    kwds
        Additional subclass-specific keyword arguments.  Values for these
        arguments are obtained from the `Registry` database according to the
        how the packer is configured in the `Registry`.

    Notes
    -----
    `DataIdPacker` subclass instances should generally be obtained from a
    `Registry`.  This involves the following steps:

     - One or more packers are configured in the ``dataIdPackers`` section of
       the `Registry` configuration.  In YAML form, that looks something like
       this:

        .. code:: yaml

            dataIdPackers:
              visit_detector_id:
                given: [instrument]
                required: [visit, detector]
                cls: lsst.daf.butler.instrument.ObservationDataIdPacker
                parameters:
                  instrument: instrument.instrument
                  obsMax: instrument.visit_max
                  detectorMax: instrument.detector_max

       See `DataIdPackerDimensions` for a description of the ``given`` and
       ``required`` options.  The ``parameters`` section maps keyword argument
       names for the `DataIdPacker` subclass constructor to dimension metadata
       fields in the `Registry` database that provide the values for these
       arguments.

     - A `DataId` that identifies at least the "given" dimensions of the
       `DataIdPacker` subclass must be expanded to include those metadata
       fields, by calling `Registry.expandDataId`.

     - `Registry.makeDataIdPacker` is called with the name of the packer and
       the expanded `DataId`.  If the `DataId` also identifies all "required"
       dimensions for the packer, `Registry.packDataId` can be called instead
       for convenience (though this does not provide a way to call
       `~DataIdPacker.unpack`).
    """

    # We implement __new__ instead of __init__ because it means things still
    # get initialized here even if derived classes don't call
    # super().__init__, which otherwise would be a very easy mistake to make.
    def __new__(cls, dimensions, **kwds):
        self = object.__new__(cls)
        self._dimensions = dimensions
        return self

    @property
    def dimensions(self):
        """The dimensions associated with the `DataIdPacker`
        (`DataIdPackerDimensions`).
        """
        return self._dimensions

    @property
    @abstractmethod
    def maxBits(self):
        """The maximum number of nonzero bits in the packed ID returned by
        `~DataIdPacker.pack` (`int`).

        Must be implemented by all concrete derived classes.  May return
        `None` to indicate that there is no maximum.
        """
        raise NotImplementedError()

    @abstractmethod
    def _pack(self, dataId):
        """Abstract implementation for `~DataIdPacker.pack`.

        Must be implemented by all concrete derived classes.

        Parameters
        ----------
        dataId : `DataId`
            Dictionary-like object identifying (at least) all required
            dimensions associated with this packer.  Guaranteed to be a true
            `DataId`, not an arbitrary mapping.

        Returns
        -------
        packed : `int`
            Packed integer ID.
        """
        raise NotImplementedError()

    def pack(self, dataId, *, returnMaxBits=False, **kwds):
        """Pack the given data ID into a single integer.

        Parameters
        ----------
        dataId : `dict` or `DataId`
            Dictionary-like object identifying (at least) all required
            dimensions associated with this packer.  Subclasses should in
            general accept an arbitrary mapping and call the `DataId`
            constructor internally to standardize.  Values for any keys also
            present in the data ID passed at construction must be the same
            as the values in the data ID passed at construction.
        returnMaxBits : `bool`
            If `True`, return a tuple of ``(packed, self.maxBits)``.
        kwds
            Additional keyword arguments forwarded to the `DataId` constructor.

        Returns
        -------
        packed : `int`
            Packed integer ID.
        maxBits : `int`, optional
            Maximum number of nonzero bits in ``packed``.  Not returned unless
            ``returnMaxBits`` is `True`.

        Notes
        -----
        Should not be overridden by derived class (`~DataIdPacker._pack`
        should be overridden instead).
        """
        dataId = DataId(dataId, **kwds)
        packed = self._pack(dataId)
        if returnMaxBits:
            return packed, self.maxBits
        else:
            return packed

    @abstractmethod
    def unpack(self, packedId):
        """Unpack an ID produced by `pack` into a full `DataId`.

        Must be implemented by all concrete derived classes.

        Parameters
        ----------
        packedId : `int` or `bytes`
            The result of a call to `~DataIdPacker.pack` on either ``self``
            or an identically-constructed one.

        Returns
        -------
        dataId : `DataId`
            Dictionary-like ID that uniquely identifies all covered
            dimensions.
        """
        raise NotImplementedError()


class DataIdPackerFactory:
    """A factory class for `DataIdPacker` instances that can report what
    metadata fields they need to be constructed.

    This class is intended for internal use by `Registry` only.

    Parameters
    ----------
    dimensions : `DataIdPackerDimensions`
        The dimensions associated with the `DataIdPacker`.
    clsName : `str`
        Fully-qualified name of the `DataIdPacker` subclass.
    parameters : `dict`, optional
        Dictionary mapping constructor keyword argument names to tuples of
        ``(DimensionElement, str)`` that indicate the database tables and
        columns from which keyword argument values should be obtained.
    """

    @classmethod
    def fromConfig(cls, universe, config):
        """Construct a `DataIdPackerFactory` from a piece of `Registry`
        configuration.

        Parameters
        ----------
        universe : `DimensionGraph`
            All dimension objects known to the `Registry`.
        config : `Config`
            A dict-like `Config` node corresponding to a single entry
            in the ``dataIdPackers`` section of a `RegistryConfig`.
        """
        dimensions = DataIdPackerDimensions.fromConfig(universe, config)
        clsName = config["cls"]
        parameters = {}
        for kwarg, field in config.get("parameters", {}).items():
            tbl, col = field.split(".")
            parameters[kwarg] = (dimensions.given.elements[tbl], col)
        return DataIdPackerFactory(dimensions, clsName, parameters)

    def __init__(self, dimensions, clsName, parameters=None):
        if parameters is None:
            parameters = {}
        self._dimensions = dimensions
        self._clsName = clsName
        self._parameters = parameters
        self._cls = None

    @property
    def dimensions(self):
        """The dimensions associated with the `DataIdPacker`
        (`DataIdPackerDimensions`).
        """
        return self._dimensions

    def updateFieldsToGet(self, metadata):
        """Add metadata fields needed to construct the `DataIdPacker` to
        the given dictionary.

        Parameters
        ----------
        metadata: `DimensionKeyDict` with `set` values
            Mapping to update in-place.
        """
        for tbl, col in self._parameters.values():
            metadata[tbl].add(col)

    def makePacker(self, dataId):
        """Construct a `DataIdPacker` instance for the given data ID.

        Parameters
        ----------
        dataId : `DataId`
            `DataId` that identifies the "given" dimensions of the packer
            and contains the metadata entries needed to construct it.
        """
        assert dataId.dimensions().issuperset(self.dimensions.given)
        if self._cls is None:
            self._cls = doImport(self._clsName)
        kwds = {kwarg: dataId.entries[tbl][col] for kwarg, (tbl, col) in self._parameters.items()}
        return self._cls(self.dimensions, **kwds)
