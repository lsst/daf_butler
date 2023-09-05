# This file is part of pipe_base.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
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
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Code to support backwards compatibility."""

from __future__ import annotations

__all__ = ["PYDANTIC_V2", "_BaseModelCompat"]

import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel
from pydantic.fields import FieldInfo
from pydantic.version import VERSION as PYDANTIC_VERSION

if sys.version_info >= (3, 11, 0):
    from typing import Self
else:
    from typing import TypeVar

    Self = TypeVar("Self", bound="_BaseModelCompat")  # type: ignore


PYDANTIC_V2 = PYDANTIC_VERSION.startswith("2.")

# This matches the pydantic v2 internal definition.
IncEx = set[int] | set[str] | dict[int, Any] | dict[str, Any] | None

if PYDANTIC_V2:

    class _BaseModelCompat(BaseModel):
        """Methods from pydantic v1 that we want to emulate in v2.

        Some of these methods are provided by v2 but issue deprecation
        warnings.  We need to decide whether we are also okay with deprecating
        them or want to support them without the deprecation message.
        """

        def json(
            self,
            *,
            include: IncEx = None,  # type: ignore
            exclude: IncEx = None,  # type: ignore
            by_alias: bool = False,
            skip_defaults: bool | None = None,
            exclude_unset: bool = False,
            exclude_defaults: bool = False,
            exclude_none: bool = False,
            encoder: Callable[[Any], Any] | None = None,
            models_as_dict: bool = True,
            **dumps_kwargs: Any,
        ) -> str:
            if dumps_kwargs:
                raise TypeError("dumps_kwargs no longer supported.")
            if encoder is not None:
                raise TypeError("json encoder is no longer supported.")
            # Can catch warnings and call BaseModel.json() directly.
            return self.model_dump_json(
                include=include,
                exclude=exclude,
                by_alias=by_alias,
                exclude_defaults=exclude_defaults,
                exclude_none=exclude_none,
                exclude_unset=exclude_unset,
            )

        @classmethod
        def parse_obj(cls, obj: Any) -> Self:
            # Catch warnings and call BaseModel.parse_obj directly?
            return cls.model_validate(obj)

        if TYPE_CHECKING and not PYDANTIC_V2:
            # mypy sees the first definition of a class and ignores any
            # redefinition. This means that if mypy is run with pydantic v1
            # it will not see the classes defined in the else block below.

            @classmethod
            def model_construct(cls, _fields_set: set[str] | None = None, **values: Any) -> Self:
                return cls()

            @classmethod
            def model_validate(
                cls,
                obj: Any,
                *,
                strict: bool | None = None,
                from_attributes: bool | None = None,
                context: dict[str, Any] | None = None,
            ) -> Self:
                return cls()

            def model_dump_json(
                self,
                *,
                indent: int | None = None,
                include: IncEx = None,
                exclude: IncEx = None,
                by_alias: bool = False,
                exclude_unset: bool = False,
                exclude_defaults: bool = False,
                exclude_none: bool = False,
                round_trip: bool = False,
                warnings: bool = True,
            ) -> str:
                return ""

            @property
            def model_fields(self) -> dict[str, FieldInfo]:  # type: ignore
                return {}

            @classmethod
            def model_rebuild(
                cls,
                *,
                force: bool = False,
                raise_errors: bool = True,
                _parent_namespace_depth: int = 2,
                _types_namespace: dict[str, Any] | None = None,
            ) -> bool | None:
                return None

            def model_dump(
                self,
                *,
                mode: Literal["json", "python"] | str = "python",
                include: IncEx = None,
                exclude: IncEx = None,
                by_alias: bool = False,
                exclude_unset: bool = False,
                exclude_defaults: bool = False,
                exclude_none: bool = False,
                round_trip: bool = False,
                warnings: bool = True,
            ) -> dict[str, Any]:
                return {}

            @classmethod
            def model_validate_json(
                cls,
                json_data: str | bytes | bytearray,
                *,
                strict: bool | None = None,
                context: dict[str, Any] | None = None,
            ) -> Self:
                return cls()

else:
    from astropy.utils.decorators import classproperty

    class _BaseModelCompat(BaseModel):  # type:ignore[no-redef]
        """Methods from pydantic v2 that can be used in pydantic v1."""

        @classmethod
        def model_validate(
            cls,
            obj: Any,
            *,
            strict: bool | None = None,
            from_attributes: bool | None = None,
            context: dict[str, Any] | None = None,
        ) -> Self:
            return cls.parse_obj(obj)

        def model_dump_json(
            self,
            *,
            indent: int | None = None,
            include: IncEx = None,
            exclude: IncEx = None,
            by_alias: bool = False,
            exclude_unset: bool = False,
            exclude_defaults: bool = False,
            exclude_none: bool = False,
            round_trip: bool = False,
            warnings: bool = True,
        ) -> str:
            return self.json(
                include=include,  # type: ignore
                exclude=exclude,  # type: ignore
                by_alias=by_alias,
                exclude_unset=exclude_unset,
                exclude_defaults=exclude_defaults,
                exclude_none=exclude_none,
            )

        @classmethod  # type: ignore
        def model_construct(cls, _fields_set: set[str] | None = None, **values: Any) -> Self:
            # BaseModel.construct() is very close to what we previously
            # implemented manually in each direct() method but does have one
            # extra loop in it to fill in defaults and handle aliases.
            return cls.construct(_fields_set=_fields_set, **values)

        @classmethod
        @classproperty
        def model_fields(cls) -> dict[str, FieldInfo]:  # type: ignore
            return cls.__fields__  # type: ignore

        @classmethod
        def model_rebuild(
            cls,
            *,
            force: bool = False,
            raise_errors: bool = True,
            _parent_namespace_depth: int = 2,
            _types_namespace: dict[str, Any] | None = None,
        ) -> bool | None:
            return cls.update_forward_refs()

        def model_dump(
            self,
            *,
            mode: Literal["json", "python"] | str = "python",
            include: IncEx = None,
            exclude: IncEx = None,
            by_alias: bool = False,
            exclude_unset: bool = False,
            exclude_defaults: bool = False,
            exclude_none: bool = False,
            round_trip: bool = False,
            warnings: bool = True,
        ) -> dict[str, Any]:
            # Need to decide whether to warn if the mode parameter is given.
            return self.dict(
                include=include,  # type: ignore
                exclude=exclude,  # type: ignore
                by_alias=by_alias,
                exclude_unset=exclude_unset,
                exclude_defaults=exclude_defaults,
                exclude_none=exclude_none,
            )

        @classmethod
        def model_validate_json(
            cls,
            json_data: str | bytes | bytearray,
            *,
            strict: bool | None = None,
            context: dict[str, Any] | None = None,
        ) -> Self:
            return cls.parse_raw(json_data)
