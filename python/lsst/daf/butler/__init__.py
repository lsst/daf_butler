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
Data Access Butler
"""

# Some components are not auto-imported since they can have additional runtime
# dependencies.

from . import logging  # most symbols are helpers only
from . import progress  # most symbols are only used by handler implementors
from . import ddl, time_utils
from ._butler import *
from ._butler_collections import *
from ._butler_config import *
from ._butler_repo_index import *
from ._collection_type import CollectionType
from ._column_categorization import *
from ._column_tags import *
from ._column_type_info import *
from ._config import *
from ._config_support import LookupKey
from ._dataset_association import *
from ._dataset_existence import *
from ._dataset_ref import *
from ._dataset_type import *
from ._deferredDatasetHandle import *
from ._exceptions import *
from ._file_dataset import *
from ._file_descriptor import *
from ._formatter import *
from ._labeled_butler_factory import *

# Do not import 'json' at all by default.
from ._limited_butler import *
from ._location import *
from ._named import *
from ._quantum import *
from ._quantum_backed import *
from ._storage_class import *
from ._storage_class_delegate import *
from ._timespan import *
from ._topology import *

# Only lift 'Datastore' itself to this scope.
from .datastore import Datastore
from .dimensions import *

# Only export 'ButlerLogRecords' from 'logging', import the module as-is for
# other symbols. ButlerLogRecords is the fundamental type stored in datastores.
from .logging import ButlerLogRecords

# Do not import or lift symbols from mapping_factory and persistence_content,
# as those are internal.
# Only lift 'Progress' from 'progress'; the module is imported as-is above
from .progress import Progress

# Do not import or lift symbols from 'server' or 'server_models'.
# Import the registry subpackage directly for other symbols.
from .registry import MissingCollectionError, NoDefaultCollectionError, Registry, RegistryConfig
from .transfers import RepoExportContext, YamlRepoExportBackend, YamlRepoImportBackend
from .version import *

# Do not import or lift symbols from 'queries' until they are public.


# Do not import or lift symbols from 'repo_relocation'.

# Do not export the utility routines from utils.
