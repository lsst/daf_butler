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

__all__ = ("FileTemplates", "FileTemplate", "FileTemplatesConfig")

import os.path
import string
from collections.abc import Mapping

from .config import Config
from .datasets import DatasetType


class FileTemplatesConfig(Config):
    """Configuration information for `FileTemplates`"""
    pass


class FileTemplates:
    """Collection of `FileTemplate` templates.

    Parameters
    ----------
    config : `FileTemplatesConfig` or `str`
        Load configuration.
    default : `str`, optional
        If not `None`, a default template to use if no template has
        been specified explicitly in the configuration.

    Notes
    -----
    The configuration can include one level of hierarchy where an
    instrument-specific section can be defined to override more general
    template specifications.  This is represented in YAML using a
    key of form ``instrument<name>`` which can then define templates
    that will be returned if a `DatasetRef` contains a matching instrument
    name in the data ID.

    A default fallback template can be specified using the key ``default``.
    Defaulting can be disabled in a child configuration by defining the
    value to be an empty string or a boolean `False`.
    """

    def __init__(self, config, default=None):
        self.config = FileTemplatesConfig(config)
        self.templates = {}
        self.default = FileTemplate(default) if default is not None else None
        for name, templateStr in self.config.items():
            # We can disable defaulting with an empty string in a config
            # or by using a boolean
            if name == "default":
                if not templateStr:
                    self.default = None
                else:
                    self.default = FileTemplate(templateStr)
            else:
                # Possible to have a second level hierarchy but store as
                # full names without separator
                if isinstance(templateStr, Mapping):
                    for subKey, subStr in templateStr.items():
                        self.templates[f"{name}{subKey}"] = FileTemplate(subStr)
                else:
                    self.templates[name] = FileTemplate(templateStr)

    def getTemplate(self, entity):
        """Retrieve the `FileTemplate` associated with the dataset type.

        If the lookup name corresponds to a component the base name for
        the component will be examined if the full component name does
        not match.

        Parameters
        ----------
        entity : `DatasetType`, `DatasetRef`, or `StorageClass`
            Instance to use to look for a corresponding template.
            A `DatasetType` name or a `StorageClass` name will be used
            depending on the supplied entity. Priority is given to a
            `DatasetType` name. Supports instrument override if a
            `DatasetRef` is provided configured with an ``instrument``
            value for the data ID.

        Returns
        -------
        template : `FileTemplate`
            Template instance to use with that dataset type.

        Raises
        ------
        KeyError
            No template could be located for this Dataset type.
        """

        # Get the names to use for lookup
        names = entity._lookupNames()

        # Get a location from the templates
        template = self.default
        for name in names:
            if name in self.templates:
                template = self.templates[name]
                break

            baseType, component = DatasetType.splitDatasetTypeName(name)
            if component is not None and baseType in self.templates:
                template = self.templates[baseType]
                break

        # if still not template give up for now.
        if template is None:
            raise KeyError(f"Unable to determine file template from supplied argument [{entity}]")

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
    are taken from the Dimensions along with several additional fields:

     - datasetType: `str`, `DatasetType.name`
     - component: `str`, name of the StorageClass component
     - collection: `str`, `Run.collection`
     - run: `int`, `Run.id`

    At least one or both of `run` or `collection` must be provided to ensure
    unique filenames.

    The mini-language is extended to understand a "?" in the format
    specification. This indicates that a field is optional. If that
    Dimension is missing the field, along with the text before the field,
    unless it is a path separator, will be removed from the output path.
    """

    def __init__(self, template):
        if not isinstance(template, str) or "{" not in template:
            raise ValueError(f"Template ({template}) does not contain any format specifiers")
        self.template = template

    def __eq__(self, other):
        if not isinstance(other, FileTemplate):
            return False

        return self.template == other.template

    def __str__(self):
        return self.template

    def __repr__(self):
        return f"{self.__class__.__name__}(\"{self.template}\")"

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
        fields["datasetType"], component = datasetType.nameAndComponent()

        usedComponent = False
        if component is not None:
            fields["component"] = component

        usedRunOrCollection = False
        fields["collection"] = ref.run.collection
        fields["run"] = ref.run.id

        fmt = string.Formatter()
        parts = fmt.parse(self.template)
        output = ""

        for literal, field_name, format_spec, conversion in parts:

            if field_name == "component":
                usedComponent = True

            if format_spec is None:
                output = output + literal
                continue

            if "?" in format_spec:
                optional = True
                # Remove the non-standard character from the spec
                format_spec = format_spec.replace("?", "")
            else:
                optional = False

            if field_name in ("run", "collection"):
                usedRunOrCollection = True

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

        # Complain if there's no run or collection
        if not usedRunOrCollection:
            raise KeyError("Template does not include 'run' or 'collection'.")

        # Since this is known to be a path, normalize it in case some double
        # slashes have crept in
        path = os.path.normpath(output)

        # It should not be an absolute path (may happen with optionals)
        if os.path.isabs(path):
            path = os.path.relpath(path, start="/")

        return path
