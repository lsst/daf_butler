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

__all__ = ["DatasetExistence"]

from enum import Flag, auto


class DatasetExistence(Flag):
    """Flags representing the different states that a dataset can have
    in a Butler repository.

    If a flag evaluates to `True` that will indicate that a dataset is
    present in the Butler repository. The different states for `KNOWN`
    and `VERIFIED` both evaluate to `True` and differ solely on whether
    the artifact was checked to make sure it exists or not.

    Some flag are combinations of other flags, so in order to determine
    whether a dataset is present in datastore it is necessary to use logical
    ``AND``.

    .. code-block :: py

        exists = DatasetExistence.VERIFIED
        if (DatasetExistence.DATASTORE & exists) == DatasetExistence.DATASTORE:
            # The datastore knows about this dataset.
    """

    UNRECOGNIZED = 0
    """The dataset is not recognized as part of this registry or datastore.

    Evaluates to `False` in Boolean context.
    """

    RECORDED = auto()
    """Known to registry or equivalent.

    Evaluates to `False` in Boolean context.
    """

    DATASTORE = auto()
    """Known to the datastore.

    Evaluates to `False` in Boolean context.
    """

    _ARTIFACT = auto()
    """Internal flag indicating that the datastore artifact has been
    confirmed to exist."""

    _ASSUMED = auto()
    """Internal flag indicating that the existence of the datastore artifact
    was never verified."""

    KNOWN = RECORDED | DATASTORE | _ASSUMED
    """The dataset is known to registry and datastore. The presence of the
    artifact in datastore has not been verified.

    Evaluates to `True` in Boolean context.
    """

    VERIFIED = RECORDED | DATASTORE | _ARTIFACT
    """The dataset is known to registry and datastore and the presence of the
    artifact has been verified.

    Evaluates to `True` in Boolean context.
    """

    def __bool__(self) -> bool:
        """Indicate whether the dataset exists.

        Returns
        -------
        exists : `bool`
            Evaluates to `True` if the flags evaluate to either ``KNOWN``
            or ``VERIFIED``.
        """
        return self.value == self.KNOWN.value or self.value == self.VERIFIED.value
