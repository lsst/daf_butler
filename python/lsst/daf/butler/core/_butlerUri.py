# Temporarily alias ResourcePath as ButlerURI.
from typing import Any

from deprecated.sphinx import deprecated
from lsst.resources import ResourcePath, ResourcePathExpression


@deprecated(
    reason="Please use lsst.resources.ResourcePath instead. Will be removed after v24.",
    version="v24.0",
    category=FutureWarning,
)
class ButlerURI(ResourcePath):
    """Class for handling URIs to file resources.

    All users should instead use `lsst.resources.ResourcePath`.
    """

    def __new__(cls, uri: ResourcePathExpression, **kwargs: Any) -> ResourcePath:  # type: ignore
        return ResourcePath(uri, **kwargs)
