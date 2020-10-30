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

from __future__ import annotations

"""S3 datastore."""

__all__ = ("S3Datastore", )

import logging

from botocore.exceptions import ClientError
from http.client import ImproperConnectionState, HTTPException
from urllib3.exceptions import RequestError, HTTPError

from typing import (
    TYPE_CHECKING,
    Any,
    Union,
    Callable
)

from lsst.daf.butler import (
    DatasetRef,
    Location,
    StoredFileInfo,
)

from .remoteFileDatastore import RemoteFileDatastore
from lsst.daf.butler.core._butlerUri.s3utils import getS3Client, bucketExists

if TYPE_CHECKING:
    from .fileLikeDatastore import DatastoreFileGetInformation
    from lsst.daf.butler import DatastoreConfig
    from lsst.daf.butler.registry.interfaces import DatastoreRegistryBridgeManager

log = logging.getLogger(__name__)




class S3Datastore(RemoteFileDatastore):
    """Basic S3 Object Storage backed Datastore.

    Parameters
    ----------
    config : `DatastoreConfig` or `str`
        Configuration. A string should refer to the name of the config file.
    bridgeManager : `DatastoreRegistryBridgeManager`
        Object that manages the interface between `Registry` and datastores.
    butlerRoot : `str`, optional
        New datastore root to use to override the configuration value.

    Raises
    ------
    ValueError
        If root location does not exist and ``create`` is `False` in the
        configuration.

    Notes
    -----
    S3Datastore supports non-link transfer modes for file-based ingest:
    `"move"`, `"copy"`, and `None` (no transfer).
    """

    defaultConfigFile = "datastores/s3Datastore.yaml"
    """Path to configuration defaults. Accessed within the ``configs`` resource
    or relative to a search path. Can be None if no defaults specified.
    """

    def __init__(self, config: Union[DatastoreConfig, str],
                 bridgeManager: DatastoreRegistryBridgeManager, butlerRoot: str = None):
        super().__init__(config, bridgeManager, butlerRoot)

        self.client = getS3Client()
        if not bucketExists(self.locationFactory.netloc):
            # PosixDatastore creates the root directory if one does not exist.
            # Calling s3 client.create_bucket is possible but also requires
            # ACL LocationConstraints, Permissions and other configuration
            # parameters, so for now we do not create a bucket if one is
            # missing. Further discussion can make this happen though.
            raise IOError(f"Bucket {self.locationFactory.netloc} does not exist!")

    @backoff.on_exception(backoff.expo, retryable_client_errors, max_time=max_retry_time)
    def _artifact_exists(self, location: Location) -> bool:
        """Check that an artifact exists in this datastore at the specified
        location.

        Parameters
        ----------
        location : `Location`
            Expected location of the artifact associated with this datastore.

        Returns
        -------
        exists : `bool`
            True if the location can be found, false otherwise.
        """
        # Exists to allow backoff retry
        return super()._artifact_exists(location)

    @backoff.on_exception(backoff.expo, retryable_client_errors, max_time=max_retry_time)
    def _delete_artifact(self, location: Location) -> None:
        """Delete the artifact from the datastore.

        Parameters
        ----------
        location : `Location`
            Location of the artifact associated with this datastore.
        """
        # Exists to allow backoff retry
        return super()._delete_artifact(location)

    @backoff.on_exception(backoff.expo, all_retryable_errors, max_time=max_retry_time)
    def _read_artifact_into_memory(self, getInfo: DatastoreFileGetInformation,
                                   ref: DatasetRef, isComponent: bool = False) -> Any:
        # Exists to allow backoff retry
        return super()._read_artifact_into_memory(getInfo, ref, isComponent)

    @backoff.on_exception(backoff.expo, all_retryable_errors, max_time=max_retry_time)
    def _write_in_memory_to_artifact(self, inMemoryDataset: Any, ref: DatasetRef) -> StoredFileInfo:
        # Exists to allow backoff retry
        return super()._write_in_memory_to_artifact(inMemoryDataset, ref)
