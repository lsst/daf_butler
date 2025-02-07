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


__all__ = (
    "collection_argument",
    "collections_argument",
    "destination_argument",
    "dimensions_argument",
    "directory_argument",
    "element_argument",
    "glob_argument",
    "locations_argument",
    "repo_argument",
)


from ..utils import MWArgumentDecorator, split_commas

collection_argument = MWArgumentDecorator("collection")

collections_argument = MWArgumentDecorator("collections", callback=split_commas, nargs=-1)

dimensions_argument = MWArgumentDecorator("dimensions", callback=split_commas, nargs=-1)

directory_argument = MWArgumentDecorator(
    "directory", help="DIRECTORY is the folder containing dataset files."
)


element_argument = MWArgumentDecorator("element", help="ELEMENT is the dimension element to obtain.")


glob_argument = MWArgumentDecorator(
    "glob", callback=split_commas, help="GLOB is one or more strings to apply to the search.", nargs=-1
)

locations_argument = MWArgumentDecorator("locations", callback=split_commas, nargs=-1)

repo_argument = MWArgumentDecorator("repo")

destination_argument = MWArgumentDecorator("destination")
