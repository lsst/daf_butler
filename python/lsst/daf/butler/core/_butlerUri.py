# Temporarily alias ResourcePath as ButlerURI.
__all__ = ["ButlerURI"]

from typing import Any, Type

from deprecated.sphinx import deprecated
from lsst.resources import ResourcePath, ResourcePathExpression
from lsst.resources.file import FileResourcePath


def _add_base(cls: Type) -> None:
    """Update the class to use ButlerURI as the base.

    Notes
    -----
    Care must be taken with SchemelessResourcePath since that class inherits
    from FileResourcePath.
    """
    if cls.__bases__ == (ResourcePath,):
        cls.__bases__ = (ButlerURI,)
    elif cls.__bases__ == (FileResourcePath,) and FileResourcePath.__bases__ == (ResourcePath,):
        FileResourcePath.__bases__ = (ButlerURI,)


def _reset_base(cls: Type) -> None:
    """Reset the base class to be ResourcePath."""
    if cls.__bases__ == (ButlerURI,):
        cls.__bases__ = (ResourcePath,)
    elif issubclass(cls, FileResourcePath) and FileResourcePath.__bases__ == (ButlerURI,):
        FileResourcePath.__bases__ = (ResourcePath,)


@deprecated(
    reason="Please use lsst.resources.ResourcePath instead. Will be removed after v24.",
    version="v24.0",
    category=FutureWarning,
)
class ButlerURI(ResourcePath):
    """Class for handling URIs to file resources.

    All users should instead use `lsst.resources.ResourcePath`.
    """

    def __new__(cls, uri: ResourcePathExpression, **kwargs: Any) -> ResourcePath:
        if cls is not ButlerURI:
            # This is a subclass trying to create an updated version of
            # itself without wanting to change the class. The ButlerURI
            # __new__ forces ResourcePath() to be used every time and so
            # the knowledge of the subclass is lost. This can result in a
            # Schemeless becoming a File URI. The simplest approach is to
            # remove ButlerURI from the class hierarchy and add it back again.
            # This seems inefficient but without it the subclass will
            # always try to call ButlerURI.__new__.
            _reset_base(cls)
            new = cls(uri, **kwargs)
            _add_base(cls)
            return new

        new_uri = ResourcePath(uri, **kwargs)
        _add_base(type(new_uri))

        return new_uri
