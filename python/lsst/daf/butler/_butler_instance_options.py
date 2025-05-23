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

__all__ = ("ButlerInstanceOptions",)

import dataclasses
from typing import Any

from ._butler_metrics import ButlerMetrics


@dataclasses.dataclass(frozen=True)
class ButlerInstanceOptions:
    """The parameters passed to `Butler.from_config` or the Butler convenience
    constructor.  These configure defaults and other settings for a Butler
    instance. These settings are common to all Butler subclasses. See `Butler`
    for the documentation of these properties.
    """

    collections: Any = None
    run: str | None = None
    writeable: bool | None = None
    inferDefaults: bool = True
    metrics: ButlerMetrics = dataclasses.field(default_factory=ButlerMetrics)
    kwargs: dict[str, Any] = dataclasses.field(default_factory=dict)
