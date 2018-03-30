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

from .config import Config


class FileTemplatesConfig(Config):
    pass


class FileTemplates:
    """Collection of `FileTemplate` templates.

    Parameters
    ----------
    config : `FileTemplatesConfig` or `str`
        Load configuration.
    """

    def __init__(self, config, default=None):
        self.config = FileTemplatesConfig(config)
        self.templates = {}
        for name, info in self.config.items():
            self.templates[name] = FileTemplate(info)

    def getTemplate(self, datasetType):
        """Retrieve the `FileTemplate` associated with the dataset type.

        Parameters
        ----------
        datasetType : `str`
            Dataset type.

        Returns
        -------
        template : `FileTemplate`
            Template instance to use with that dataset type.
        """
        # Get a location from the templates
        template = None
        component = None
        if datasetType is not None:
            if datasetType in self.templates:
                template = self.templates[datasetType]
            elif "." in datasetType:
                baseType, component = datasetType.split(".", maxsplit=1)
                if baseType in self.templates:
                    template = self.templates[baseType]

        if template is None:
            if "default" in self.templates:
                template = self.templates["default"]

        # if still not template give up for now.
        if template is None:
            raise TypeError("Unable to determine file template from supplied type [{}]".format(datasetType))

        return template


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
    "datasetType" will be replaced with the DatasetType and "component"
    will be replaced with the component name of a composite.

    The mini-language is extended to understand a "?" in the format
    specification. This indicates that a field is optional. If that
    DataUnit is missing the field, along with the text before the field,
    unless it is a path separator, will be removed from the output path.
    """

    def __init__(self, template):
        self.template = template

    def format(self, ref):
        """Format a template string into a full path.

        Parameters
        ----------
        ref : `DatasetRef`
            The dataset to be formatted.

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
        # Extract defined non-None units from the dataId
        fields = {k: v for k, v in ref.dataId.items() if v is not None}

        datasetType = ref.datasetType
        fields["datasetType"] = datasetType.name
        component = datasetType.component()

        usedComponent = False
        if component is not None:
            fields["component"] = component

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

            if field_name in fields:
                value = fields[field_name]
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
            raise KeyError("Component '{}' specified but template {} did not use it".format(component,
                                                                                            self.template))

        # Since this is known to be a path, normalize it in case some double
        # slashes have crept in
        path = os.path.normpath(output)

        # It should not be an absolute path (may happen with optionals)
        if os.path.isabs(path):
            path = os.path.relpath(path, start="/")

        return path
