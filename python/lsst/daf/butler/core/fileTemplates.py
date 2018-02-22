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

"""Support for file template string expansion."""

import os.path
import string


class FileTemplate:
    """Format a path template into a fully expanded path.

    Parameters
    ----------
    template : `str`
        Template string.

    Notes
    -----
    The templates use the standard Format Specification Mini-Language
    with the caveat that only named fields can be used. The field names
    are taken from the DataUnits along with two additional fields:
    "datasettype" will be replaced with the DataSetType and "component"
    will be replaced with the component name of a composite.

    The mini-language is extended to understand a "?" in the format
    specification. This indicates that a field is optional. If that
    DataUnit is missing the field, along with the text before the field,
    unless it is a path separator, will be removed from the output path.
    """

    def __init__(self, template):
        self.template = template

    def format(self, dataunits, datasettype=None, component=None):
        """Format a template string into a full path.

        Parameters
        ----------
        dataunits : `DataUnits`
            DataUnits and the corresponding values.
        datasettype : `str`, optional.
            DataSetType name to use if needed. If it contains a "." separator
            the type name will be split up into the main DataSetType and a
            component.
        component : `str`, optional
            Component of a composite. If `datasettype` defines a component
            this parameter will be ignored.

        Returns
        -------
        path : `str`
            Expanded path.

        Raises
        ------
        KeyError
            Requested field is not defined and the field is not optional.
            Or, `component` is specified but "component" was not part of
            the template.
        """
        units = dataunits.definedUnits()

        if datasettype is not None:
            # calexp.wcs means wcs component of a calexp
            if "." in datasettype:
                datasettype, component = datasettype.split(".", maxsplit=1)
            units["datasettype"] = datasettype

        usedComponent = False
        if component is not None:
            units["component"] = component

        fmt = string.Formatter()
        parts = fmt.parse(self.template)
        output = ""

        for literal, field_name, format_spec, conversion in parts:

            if field_name == "component":
                usedComponent = True

            if "?" in format_spec:
                optional = True
                # Remove the non-standard character from the spec
                format_spec = format_spec.replace("?", "")
            else:
                optional = False

            if field_name in units:
                value = units[field_name]
            elif optional:
                # If this is optional ignore the format spec
                # and do not include the literal text prior to the optional
                # field unless it contains a "/" path separator
                format_spec = ""
                value = ""
                if "/" not in literal:
                    literal = ""
            else:
                raise KeyError("{} requested in template but not defined and not optional".format(field_name))

            # Now use standard formatting
            output = output + literal + format(value, format_spec)

        # Complain if we were meant to use a component
        if component is not None and not usedComponent:
            raise KeyError("Component {} specified but template {} did not use it".format(component,
                                                                                          self.template))

        # Since this is known to be a path, normalize it in case some double
        # slashes have crept in
        path = os.path.normpath(output)

        # It should not be an absolute path (may happen with optionals)
        if os.path.isabs(path):
            path = os.path.relpath(path, start="/")

        return path
