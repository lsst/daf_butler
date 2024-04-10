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

import unittest

import pydantic
from astropy.time import Time
from lsst.daf.butler.pydantic_utils import DeferredValidation, SerializableRegion, SerializableTime
from lsst.sphgeom import ConvexPolygon, Mq3cPixelization


class Inner(pydantic.BaseModel):
    """Test model that will be wrapped with DeferredValidation."""

    value: int

    @pydantic.model_validator(mode="after")
    def _validate(self, info: pydantic.ValidationInfo) -> Inner:
        if info.context and "override" in info.context:
            self.value = info.context["override"]
        return self


class SerializedInner(DeferredValidation[Inner]):
    """Concrete test subclass of DeferredValidation."""

    pass


class OuterWithWrapper(pydantic.BaseModel):
    """Test model that holds an `Inner` via `DeferredValidation`."""

    inner: SerializedInner


class OuterWithoutWrapper(pydantic.BaseModel):
    """Test model that holds an `Inner` directly."""

    inner: Inner


class DeferredValidationTestCase(unittest.TestCase):
    """Tests for `lsst.daf.butler.pydanic_utils.DeferredValidation`."""

    def test_json_schema(self) -> None:
        self.assertEqual(
            OuterWithWrapper.model_json_schema()["properties"]["inner"], Inner.model_json_schema()
        )

    def test_dump_and_validate(self) -> None:
        outer1a = OuterWithWrapper(inner=Inner(value=5))
        json_str = outer1a.model_dump_json()
        python_data = outer1a.model_dump()
        outer1b = OuterWithoutWrapper(inner=Inner(value=5))
        outer1c = OuterWithWrapper(inner=SerializedInner.from_validated(Inner.model_construct(value=5)))
        self.assertEqual(json_str, outer1b.model_dump_json())
        self.assertEqual(python_data, outer1b.model_dump())
        self.assertEqual(json_str, outer1c.model_dump_json())
        self.assertEqual(python_data, outer1c.model_dump())
        self.assertEqual(OuterWithoutWrapper.model_validate_json(json_str).inner.value, 5)
        self.assertEqual(OuterWithoutWrapper.model_validate(python_data).inner.value, 5)
        outer2a = OuterWithWrapper.model_validate_json(json_str)
        outer2b = OuterWithWrapper.model_validate(python_data)
        self.assertEqual(outer2a.inner.validated().value, 5)
        self.assertEqual(outer2b.inner.validated().value, 5)
        self.assertIs(outer2a.inner.validated(), outer2a.inner.validated())  # caching
        self.assertIs(outer2b.inner.validated(), outer2b.inner.validated())  # caching
        outer2c = OuterWithWrapper.model_validate_json(json_str)
        outer2d = OuterWithWrapper.model_validate(python_data)
        self.assertEqual(outer2c.inner.validated(override=4).value, 4)
        self.assertEqual(outer2d.inner.validated(override=4).value, 4)
        self.assertIs(outer2c.inner.validated(override=4), outer2c.inner.validated(override=4))  # caching
        self.assertIs(outer2d.inner.validated(override=4), outer2d.inner.validated(override=4))  # caching


class SerializableExtensionsTestCase(unittest.TestCase):
    """Tests for third-party types we add serializable annotations for."""

    def test_region(self) -> None:
        pixelization = Mq3cPixelization(10)
        region = pixelization.pixel(12058823)
        adapter = pydantic.TypeAdapter(SerializableRegion)
        self.assertEqual(adapter.json_schema()["media"]["binaryEncoding"], "base16")
        json_roundtripped = adapter.validate_json(adapter.dump_json(region))
        self.assertIsInstance(json_roundtripped, ConvexPolygon)
        self.assertEqual(json_roundtripped.getVertices(), region.getVertices())
        python_roundtripped = adapter.validate_python(adapter.dump_python(region))
        self.assertIsInstance(json_roundtripped, ConvexPolygon)
        self.assertEqual(python_roundtripped.getVertices(), region.getVertices())
        with self.assertRaises(ValueError):
            adapter.validate_python(12)
        with self.assertRaises(ValueError):
            adapter.validate_json({})
        with self.assertRaises(ValueError):
            adapter.validate_json((b"this is not a region").hex())
        with self.assertRaises(ValueError):
            adapter.validate_json("this is not a hex string")

    def test_time(self) -> None:
        time = Time("2021-09-09T03:00:00", format="isot", scale="tai")
        adapter = pydantic.TypeAdapter(SerializableTime)
        self.assertIn("integer nanoseconds", adapter.json_schema()["description"])
        json_roundtripped = adapter.validate_json(adapter.dump_json(time))
        self.assertIsInstance(json_roundtripped, Time)
        self.assertEqual(json_roundtripped, time)
        python_roundtripped = adapter.validate_python(adapter.dump_python(time))
        self.assertIsInstance(json_roundtripped, Time)
        self.assertEqual(python_roundtripped, time)
        with self.assertRaises(ValueError):
            adapter.validate_python("one")
        with self.assertRaises(ValueError):
            adapter.validate_json({})


if __name__ == "__main__":
    unittest.main()
