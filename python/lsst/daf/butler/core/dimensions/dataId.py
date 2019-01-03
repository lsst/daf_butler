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

__all__ = ("DataId",)

import itertools
from collections.abc import Mapping
from .graph import DimensionGraph
from .elements import Dimension


class DataId(Mapping):
    r"""A dict-like identifier for data usable across multiple collections
    and `DatasetType`\s.

    Parameters
    ----------
    dataId : `dict` or `DataId`
        A `dict`-like object containing `Dimension` links.  If this is a true
        `DataId` and the set of dimensions identified does not change, this
        object will be updated in-place and returned instead of a new instance.
    dimensions : iterable of `Dimension` or `str`, optional
        The set of dimensions the `DataId` will identify, either as `Dimension`
        instances or string names thereof.
    dimension : `Dimension` or `str`, optional
        The single dimension this `DataId` will identify (along with all of
        its required dependencies).
    universe : `DimensionGraph`, optional
        A graph containing all known dimensions and joins.  Must be provided
        if names are passed instead of `Dimension` instances in ``dimensions``
        or ``dimension``, or when dimensions are inferred from the provided
        link keys.
    region : `lsst.sphgeom.ConvexPolygon`, optional
        Spatial region on the sky associated with this combination of
        dimension entries.
    entries : `dict`, optional
        A nested dictionary of additional metadata column values associated
        with these dimensions, with `DimensionElement` instances or `str`
        names as the outer keys, `str` column names as inner keys, and
        column values as inner dictionary values.
    extra : `dict`, optional
        Additional key-value pairs to update ``dataId`` with.
    kwds : `dict`, optional
        Additional key-value pairs to update ``dataId`` with.

    Notes
    -----
    The keys of a `DataId` correspond to the "link" columns of one or more
    `Dimension`\s, while values identify particular rows in the tables or
    views for those `Dimension`\s.  In addition to implementing the
    (immutable) `collections.abc.Mapping` API, `DataId`\s have additional
    attributes to hold additional metadata, regions, and definitions for those
    `Dimension`\s. They are also hashable, and hence can be used as keys in
    dictionaries.

    The `DataId` class represents a complete ID that has either been obtained
    from or validated with the set of known `Dimension`\s.  Regular `dict`\s
    are typically used to represent conceptual data IDs that have not been
    validated.
    The `DataId` constructor serves as a sort of standardization routine; most
    APIs that conceptually operate on `DataId`\s should accept either true
    `DataId`\s or regular dicts via a single ``dataId`` argument, and pass
    this through the `DataId` construction (usually with additional keyword
    arguments forwarded) to guarantee a true `DataId`.  When convenient, that
    `DataId` should also be returned.

    The set of dimensions a `DataId` identifies can be provided to the
    constructor four ways:

    - Multiple dimensions may be passed via the ``dimensions`` argument.
    - A single dimension may be passed via the ``dimension`` argument.
    - If a true `DataId` is passed, its dimensions will be used if they are
      not overridden by one of the above.
    - If none of the above is provided, the dimensions are inferred from the
      set of keys provided in ``dataId``, ``extra``, and ``kwds``; any
      dimensions in ``universe`` whose links are a subset of those keys is
      included.

    Raises
    ------
    ValueError
        Raised if incomplete or incompatible arguments are provided.
    """

    def __new__(cls, dataId=None, *, dimensions=None, dimension=None, universe=None, region=None,
                entries=None, extra=None, **kwds):

        if isinstance(dataId, DataId):
            if dimensions is None and dimension is None:
                # Shortcut the case where we already have a true DataId and the
                # dimensions are not changing.
                # Note that this still invokes __init__, which may update
                # the region and/or entries.
                return dataId
            if universe is not None and universe != dataId.dimensions().universe:
                raise ValueError("Input DataId has dimensions from a different universe.")
            universe = dataId.dimensions().universe
        elif dataId is None:
            dataId = {}

        # Transform 'dimension' arg into a Dimension object if it isn't already
        if dimension is not None and not isinstance(dimension, Dimension):
            if universe is None:
                raise ValueError(f"Cannot use {type(dimension)} as 'dimension' argument without universe.")
            dimension = universe[dimension]

        # Transform 'dimensions' arg into a DimensionGraph object if it isn't already
        if dimensions is not None and not isinstance(dimensions, DimensionGraph):
            if universe is None:
                raise ValueError(f"Cannot use {type(dimensions)} as 'dimensions' argument without universe.")
            dimensions = universe.extract(dimensions)

        if dimensions is None:
            if dimension is None:
                if universe is None:
                    raise ValueError(f"Cannot infer dimensions without universe.")
                allLinks = dict(dataId)
                if extra is not None:
                    allLinks.update(extra)
                allLinks.update(kwds)
                dimensions = universe.extract(dim for dim in universe
                                              if dim.links(expand=False).issubset(allLinks))
            else:
                # Set DimensionGraph to the full set of dependencies for the
                # single Dimension that was provided.
                dimensions = dimension.graph()
        elif dimension is not None and dimension.graph() != dimensions:
            # Both 'dimensions' and 'dimension' were provided but they
            # disagree.
            raise ValueError(f"Dimension conflict: {dimension.graph()} != {dimensions}")

        assert isinstance(dimensions, DimensionGraph), "should be set by earlier logic"

        # One more attempt to shortcut by returning the original object: if
        # caller provided a true DataId and explicit dimensions, but they
        # already agree. As above, __init__ will still fire.
        if isinstance(dataId, DataId) and dataId.dimensions() == dimensions:
            return dataId

        if extra is None:
            extra = {}

        # Make a new instance with the dimensions and links we've identified.
        self = super().__new__(cls)
        self._requiredDimensions = dimensions
        self._allDimensions = DimensionGraph(dimensions.universe, dimensions=dimensions, implied=True)
        self._linkValues = {
            linkName: linkValue for linkName, linkValue
            in itertools.chain(dataId.items(), extra.items(), kwds.items())
            if linkName in self._requiredDimensions.links()
        }

        # Transfer more stuff if we're starting from a real DataId
        if isinstance(dataId, DataId):
            # Transfer the region if it's the right one.
            if self._requiredDimensions.getRegionHolder() == dataId.dimensions().getRegionHolder():
                self.region = dataId.region
            else:
                self.region = None

            # Transfer entries for the dimensions, making new dicts where
            # necessary.  We let the new DataId and the old share the same
            # second-level dictionaries, because these correspond to the same
            # rows in the Registry and updates to those rows are rare, so it
            # doesn't make sense to worry about conflicts here.
            self._entries = {element: dataId.entries.get(element, {})
                             for element in self._allDimensions.elements}
        else:
            # Create appropriately empty regions and entries if we're not
            # starting from a real DataId.
            self.region = None
            self._entries = {element: {} for element in self._allDimensions.elements}

        # Return the new instance, invoking __init__ to do further updates.
        return self

    def __init__(self, dataId=None, *, dimensions=None, dimension=None, universe=None, region=None,
                 entries=None, extra=None, **kwds):
        if dataId is None:
            dataId = {}

        if dimension is not None:
            # If a single dimension was explicitly provided, it must be the
            # only leaf dimension in the graph; extract that to ensure that
            # the 'dimension' is in fact a `Dimension`, not a `str` name.
            dimension, = self.dimensions().leaves

        if entries is not None:
            for element, subdict in entries.items():
                self.entries[element].update(subdict)

        missing = self.dimensions().links() - self._linkValues.keys()
        for linkName in missing:
            # Didn't get enough key-value pairs to identify all dimensions
            # from the links; look in entries for those.
            for element in self.dimensions().withLink(linkName):
                try:
                    self._linkValues[linkName] = self.entries[element][linkName]
                    break
                except KeyError:
                    pass
            else:
                raise LookupError(f"No value found for link '{linkName}'")

        # If we got an explicit region argument, use it.
        if region is not None:
            self.region = region
            self.entries[self.dimensions().getRegionHolder()]["region"] = region

        # Entries should contain link fields as well, so transfer them from
        # 'extra' + 'kwds'.  Also transfer from 'links' iff it's not a DataId;
        # if it is, we can safely assume the transfer has already been done.

        def addLinksToEntries(items):
            for linkName, linkValue in items:
                try:
                    associated = self.dimensions(implied=True).withLink(linkName)
                except KeyError:
                    # This isn't a link. If an explicit dimension was
                    # provided, assume these fields are metadata for that
                    # dimension.
                    if dimension is not None:
                        self.entries[dimension][linkName] = linkValue
                    else:
                        raise
                for element in associated:
                    if element in self.dimensions(implied=True):
                        self.entries[element][linkName] = linkValue

        if extra is not None:
            addLinksToEntries(extra.items())
        addLinksToEntries(kwds.items())
        if not isinstance(dataId, DataId):
            addLinksToEntries(dataId.items())

        # If we still haven't got a region, look for one in entries.
        if self.region is None:
            holder = self.dimensions().getRegionHolder()
            if holder is not None:
                self.region = self.entries[holder].get("region", None)

    def dimensions(self, implied=False):
        """Return dimensions this `DataId` identifies.

        Parameters
        ----------
        implied : `bool`
            If `True`, include implied dependencies as well.

        Returns
        -------
        graph : `DimensionGraph`
        """
        if implied:
            return self._allDimensions
        else:
            return self._requiredDimensions

    @property
    def entries(self):
        r"""A nested dictionary of additional values associated with the
        identified dimension entries (`dict`).

        The outer dictionary maps `DimensionElement` objects to dictionaries
        of field names and values.

        Entry values are not in general guaranteed to have been validated
        against any actual `Registry` schema.
        """
        return self._entries

    def implied(self):
        """Return a new `DataId` with all implied dimensions of ``self``
        "upgraded" to required.
        """
        return DataId(self, dimensions=self.dimensions(implied=True))

    def __str__(self):
        return "{{{}}}".format(", ".join(f"{k}: {v}" for k, v in self.items()))

    def __repr__(self):
        return f"DataId({self}, dimensions={self.dimensions()})"

    def __iter__(self):
        return iter(self._linkValues)

    def __contains__(self, key):
        return key in self._linkValues

    def __len__(self):
        return len(self._linkValues)

    def __getitem__(self, key):
        return self._linkValues[key]

    def keys(self):
        return self._linkValues.keys()

    def values(self):
        return self._linkValues.values()

    def items(self):
        return self._linkValues.items()

    def __eq__(self, other):
        try:
            return self._linkValues == other._linkValues
        except AttributeError:
            # also compare equal to regular dicts with the same keys and values
            return self._linkValues == other

    def __hash__(self):
        return hash(frozenset(self._linkValues.items()))

    def fields(self, element, region=True, metadata=True):
        """Return the entries for a particular `DimensionElement`.

        Parameters
        ----------
        element : `DimensionElement` or `str`
            The `Dimension` or `DimensionJoin` for which fields should be
            returned.
        region : `bool`
            Whether to include the region in the result.  Ignored if this
            `DataId` has no region or the given `Dimension` is not the
            region holder for it.
        metadata : `bool`
            Whether to include metadata (non-link, non-region columns) in the
            result.  Ignored if this `DataId` has no metadata for the given
            `Dimension`.

        Returns
        -------
        fields : `dict`
            A dictionary of column name-value pairs.
        """
        element = self.dimensions().universe.elements[element]
        entries = self.entries[element]
        if region and metadata:
            return entries
        return {k: v for k, v in entries.items()
                if (metadata or k in self.keys()) and (region or k != "region")}

    def __getnewargs_ex__(self):
        """Support special pickling for DataId.

        Default unpickling code calls `__new__` without arguments which does
        not work for this class, need to provide minimal set of arguments to
        to support logic in `__new__` and to run it without error. Pickle
        still executes its regular logic to save instance attributes and
        restore their state (after creating new instance with `__new__`).

        Returns
        -------
        args : `tuple`
            Positional arguments for `__new__`.
        kwargs : `dict`
            Keyword arguments for `__new__`.
        """
        args = (None,)
        kwargs = dict(dimensions=self.dimensions())
        return (args, kwargs)
