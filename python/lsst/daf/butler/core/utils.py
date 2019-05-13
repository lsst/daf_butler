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

__all__ = ("iterable", "allSlots", "slotValuesAreEqual", "slotValuesToHash",
           "getFullTypeName", "getInstanceOf", "Singleton", "transactional",
           "getObjectSize", "stripIfNotNone", "PrivateConstructorMeta",
           "NamedKeyDict", "getClassOf"
           "checkFileExists", "s3CheckFileExists", "parsePathToUriElements", "bucketExists")

import builtins
import sys
import functools
import urllib
from urllib.parse import urlparse

try:
    import boto3
except:
    boto3 = None

from lsst.utils import doImport


def s3CheckFileExists(client, bucket, filepath):
    """Returns (True, filesize) if file exists in the bucket
    and (False, -1) if the file is not found.

    You are getting charged for a Bucket GET request. The request
    returns the list of all files matching the given filepath.
    Multiple matches are considered a non-match.

    Parameters
    ----------
    client : 'boto3.client'
        S3 Client object to query.
    bucket : 'str'
        Name of the bucket in which to look.
    filepath : 'str'
        Path to file.

    Returns
    -------
    (`bool`, `int`) : `tuple`
       Tuple (exists, size). If file exists (True, filesize)
       and (False, -1) when the file is not found.
    """
    # this has maxkeys kwarg, limited to 1000 by default
    # apparently this is the fastest way do look-ups as it avoids having
    # to create and add a new HTTP connection to pool
    # https://github.com/boto/botocore/issues/1248
    # https://github.com/boto/boto3/issues/1128
    response = client.list_objects_v2(
        Bucket=bucket,
        Prefix=filepath
    )
    # Hopefully multiple identical files will never exist?
    matches = [x for x in response.get('Contents', []) if x["Key"] == filepath]
    if len(matches) == 1:
        return (True, matches[0]['Size'])
    else:
        return (False, -1)


def parsePathToUriElements(path):
    """If the path is a local filesystem path constructs elements of a URI.
    If path is an URI returns the URI elements: (schema, root, relpath).

    Parameters
    ----------
    uri : `str`
        URI or a POSIX-like path to parse.

    Returns
    -------
    scheme : 'str'
        Either 'file://' or 's3://'.
    root : 'str'
        S3 Bucket name or Posix absolute path up to the top of
        the relative path
    relpath : 'str'
        Posix-like path relative to root.
    """
    parsed = urlparse(path)

    # urllib assumes only absolute paths exist in URIs
    # It will not parse rel and abs paths the same way.
    # We want handle POSIX like paths too.
    # 'file://' prefixed URIs and POSIX paths
    if parsed.scheme == 'file' or not parsed.scheme:
        scheme = 'file://'
        # Absolute paths in URI and absolute POSIX paths
        if not parsed.netloc or not os.path.isabs(parsed.path):
            root = '/'
            relpath = parsed.path.lstrip('/')
        # Relative paths in URI and relative POSIX paths
        else:
            relpath = os.path.join(parsed.netloc, parsed.path.lstrip('/'))
            root = os.path.abspath(relpath).split(relpath)[0]
    # S3 URIs are always s3://bucketName/root/subdir/file.ext
    elif parsed.scheme == 's3':
        scheme = 's3://'
        root = parsed.netloc
        relpath = parsed.path.lstrip('/')
    else:
        raise urllib.error.URLError(f'Can not parse path: {path}')

    return scheme, root, relpath


def bucketExists(uri):
    """Check if the S3 bucket at a given URI actually exists.

    Parameters
    ----------
    uri : `str`
        URI of the S3 Bucket

    Returns
    -------
    exists : `bool`
        True if it exists, False if no Bucket with specified parameters is found.
    """
    if boto3 is None:
        raise ModuleNotFoundError(("Could not find boto3. "
                                   "Are you sure it is installed?"))

    session = boto3.Session(profile_name='default')
    client = boto3.client('s3')
    scheme, root, relpath = parsePath2Uri(uri)

    try:
        client.get_bucket_location(Bucket=root)
        # bucket exists, all is well
        return True
    except client.exceptions.NoSuchBucket:
        return False


def iterable(a):
    """Make input iterable.

    There are three cases, when the input is:

    - iterable, but not a `str` -> iterate over elements
      (e.g. ``[i for i in a]``)
    - a `str` -> return single element iterable (e.g. ``[a]``)
    - not iterable -> return single elment iterable (e.g. ``[a]``).

    Parameters
    ----------
    a : iterable or `str` or not iterable
        Argument to be converted to an iterable.

    Returns
    -------
    i : `generator`
        Iterable version of the input value.
    """
    if isinstance(a, str):
        yield a
        return
    try:
        yield from a
    except Exception:
        yield a


def allSlots(self):
    """
    Return combined ``__slots__`` for all classes in objects mro.

    Parameters
    ----------
    self : `object`
        Instance to be inspected.

    Returns
    -------
    slots : `itertools.chain`
        All the slots as an iterable.
    """
    from itertools import chain
    return chain.from_iterable(getattr(cls, "__slots__", []) for cls in self.__class__.__mro__)


def slotValuesAreEqual(self, other):
    """
    Test for equality by the contents of all slots, including those of its
    parents.

    Parameters
    ----------
    self : `object`
        Reference instance.
    other : `object`
        Comparison instance.

    Returns
    -------
    equal : `bool`
        Returns True if all the slots are equal in both arguments.
    """
    return all((getattr(self, slot) == getattr(other, slot) for slot in allSlots(self)))


def slotValuesToHash(self):
    """
    Generate a hash from slot values.

    Parameters
    ----------
    self : `object`
        Instance to be hashed.

    Returns
    -------
    h : `int`
        Hashed value generated from the slot values.
    """
    return hash(tuple(getattr(self, slot) for slot in allSlots(self)))


def getFullTypeName(cls):
    """Return full type name of the supplied entity.

    Parameters
    ----------
    cls : `type` or `object`
        Entity from which to obtain the full name. Can be an instance
        or a `type`.

    Returns
    -------
    name : `str`
        Full name of type.

    Notes
    -----
    Builtins are returned without the ``builtins`` specifier included.  This
    allows `str` to be returned as "str" rather than "builtins.str".
    """
    # If we have an instance we need to convert to a type
    if not hasattr(cls, "__qualname__"):
        cls = type(cls)
    if hasattr(builtins, cls.__qualname__):
        # Special case builtins such as str and dict
        return cls.__qualname__
    return cls.__module__ + "." + cls.__qualname__


def getClassOf(typeOrName):
    """Given the type name or a type, return the python type.

    If a type name is given, an attempt will be made to import the type.

    Parameters
    ----------
    typeOrName : `str` or Python class
        A string describing the Python class to load or a Python type.

    Returns
    -------
    type_ : `type`
        Directly returns the Python type if a type was provided, else
        tries to import the given string and returns the resulting type.

    Notes
    -----
    This is a thin wrapper around `~lsst.utils.doImport`.
    """
    if isinstance(typeOrName, str):
        cls = doImport(typeOrName)
    else:
        cls = typeOrName
    return cls


def getInstanceOf(typeOrName, *args, **kwargs):
    """Given the type name or a type, instantiate an object of that type.

    If a type name is given, an attempt will be made to import the type.

    Parameters
    ----------
    typeOrName : `str` or Python class
        A string describing the Python class to load or a Python type.
    args : `tuple`
        Positional arguments to use pass to the object constructor.
    kwargs : `dict`
        Keyword arguments to pass to object constructor.

    Returns
    -------
    instance : `object`
        Instance of the requested type, instantiated with the provided
        parameters.
    """
    cls = getClassOf(typeOrName)
    return cls(*args, **kwargs)


class Singleton(type):
    """Metaclass to convert a class to a Singleton.

    If this metaclass is used the constructor for the singleton class must
    take no arguments. This is because a singleton class will only accept
    the arguments the first time an instance is instantiated.
    Therefore since you do not know if the constructor has been called yet it
    is safer to always call it with no arguments and then call a method to
    adjust state of the singleton.
    """

    _instances = {}

    def __call__(cls):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__()
        return cls._instances[cls]


def transactional(func):
    """Decorator that wraps a method and makes it transactional.

    This depends on the class also defining a `transaction` method
    that takes no arguments and acts as a context manager.
    """
    @functools.wraps(func)
    def inner(self, *args, **kwargs):
        with self.transaction():
            return func(self, *args, **kwargs)
    return inner


def getObjectSize(obj, seen=None):
    """Recursively finds size of objects.

    Only works well for pure python objects. For example it does not work for
    ``Exposure`` objects where all the content is behind getter methods.

    Parameters
    ----------
    obj : `object`
       Instance for which size is to be calculated.
    seen : `set`, optional
       Used internally to keep track of objects already sized during
       recursion.

    Returns
    -------
    size : `int`
       Size in bytes.

    See Also
    --------
    sys.getsizeof

    Notes
    -----
    See https://goshippo.com/blog/measure-real-size-any-python-object/
    """
    size = sys.getsizeof(obj)
    if seen is None:
        seen = set()
    obj_id = id(obj)
    if obj_id in seen:
        return 0
    # Important mark as seen *before* entering recursion to gracefully handle
    # self-referential objects
    seen.add(obj_id)
    if isinstance(obj, dict):
        size += sum([getObjectSize(v, seen) for v in obj.values()])
        size += sum([getObjectSize(k, seen) for k in obj.keys()])
    elif hasattr(obj, "__dict__"):
        size += getObjectSize(obj.__dict__, seen)
    elif hasattr(obj, "__iter__") and not isinstance(obj, (str, bytes, bytearray)):
        size += sum([getObjectSize(i, seen) for i in obj])

    return size


def stripIfNotNone(s):
    """Strip leading and trailing whitespace if the given object is not None.

    Parameters
    ----------
    s : `str`, optional
        Input string.

    Returns
    -------
    r : `str` or `None`
        A string with leading and trailing whitespace stripped if `s` is not
        `None`, or `None` if `s` is `None`.
    """
    if s is not None:
        s = s.strip()
    return s


class PrivateConstructorMeta(type):
    """A metaclass that disables regular construction syntax.

    A class that uses PrivateConstructorMeta may have an ``__init__`` and/or
    ``__new__`` method, but these can't be invoked by "calling" the class
    (that will always raise `TypeError`).  Instead, such classes can be called
    by calling the metaclass-provided `_construct` class method with the same
    arguments.

    As is usual in Python, there are no actual prohibitions on what code can
    call `_construct`; the purpose of this metaclass is just to prevent
    instances from being created normally when that can't do what users would
    expect.

    ..note::

        Classes that inherit from PrivateConstructorMeta also inherit
        the hidden-constructor behavior.  If you just want to disable
        construction of the base class, `abc.ABCMeta` may be a better
        option.

    Examples
    --------
    Given this class definition::
        class Hidden(metaclass=PrivateConstructorMeta):

            def __init__(self, a, b):
                self.a = a
                self.b = b

    This doesn't work:

        >>> instance = Hidden(a=1, b="two")
        TypeError: Hidden objects cannot be constructed directly.

    But this does:

        >>> instance = Hidden._construct(a=1, b="two")

    """

    def __call__(cls, *args, **kwds):
        """Disabled class construction interface; always raises `TypeError.`
        """
        raise TypeError(f"{cls.__name__} objects cannot be constructed directly.")

    def _construct(cls, *args, **kwds):
        """Private class construction interface.

        All arguments are forwarded to ``__init__`` and/or ``__new__``
        in the usual way.
        """
        return type.__call__(cls, *args, **kwds)


class NamedKeyDict(MutableMapping):
    """A dictionary wrapper that require keys to have a ``.name`` attribute,
    and permits lookups using either key objects or their names.

    Names can be used in place of keys when updating existing items, but not
    when adding new items.

    It is assumed (but asserted) that all name equality is equivalent to key
    equality, either because the key objects define equality this way, or
    because different objects with the same name are never included in the same
    dictionary.

    Parameters
    ----------
    args
        All positional constructor arguments are forwarded directly to `dict`.
        Keyword arguments are not accepted, because plain strings are not valid
        keys for `NamedKeyDict`.

    Raises
    ------
    AttributeError
        Raised when an attempt is made to add an object with no ``.name``
        attribute to the dictionary.
    AssertionError
        Raised when multiple keys have the same name.
    """

    __slots__ = ("_dict", "_names",)

    def __init__(self, *args):
        self._dict = dict(*args)
        self._names = {key.name: key for key in self._dict}
        assert len(self._names) == len(self._dict), "Duplicate names in keys."

    @property
    def names(self):
        """The set of names associated with the keys, in the same order
        (`~collections.abc.KeysView`).
        """
        return self._names.keys()

    def byName(self):
        """Return a `dict` with names as keys and the same values as ``self``.
        """
        return dict(zip(self._names.keys(), self._dict.values()))

    def __len__(self):
        return len(self._dict)

    def __iter__(self):
        return iter(self._dict)

    def __getitem__(self, key):
        if hasattr(key, "name"):
            return self._dict[key]
        else:
            return self._dict[self._names[key]]

    def __setitem__(self, key, value):
        if hasattr(key, "name"):
            assert self._names.get(key.name, key) == key, "Name is already associated with a different key."
            self._dict[key] = value
            self._names[key.name] = key
        else:
            self._dict[self._names[key]] = value

    def __delitem__(self, key):
        if hasattr(key, "name"):
            del self._dict[key]
            del self._names[key.name]
        else:
            del self._dict[self._names[key]]
            del self._names[key]

    def keys(self):
        return self._dict.keys()

    def values(self):
        return self._dict.values()

    def items(self):
        return self._dict.items()
