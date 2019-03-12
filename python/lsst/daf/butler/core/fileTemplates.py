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

__all__ = ("FileTemplates", "FileTemplate", "FileTemplatesConfig", "FileTemplateValidationError")

import os.path
import string
import logging

from .config import Config
from .configSupport import processLookupConfigs, LookupKey, normalizeLookupKeys
from .exceptions import ValidationError

log = logging.getLogger(__name__)


class FileTemplateValidationError(ValidationError):
    """Exception thrown when a file template is not consistent with the
    associated `DatasetType`."""
    pass


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

    The config is parsed using the function
    `~lsst.daf.butler.configSubset.processLookupConfigs`.
    """

    defaultKey = LookupKey("default")
    """Configuration key associated with the default template."""

    def __init__(self, config, default=None):
        self.config = FileTemplatesConfig(config)
        self.templates = {}
        self.normalized = False
        self.default = FileTemplate(default) if default is not None else None
        contents = processLookupConfigs(self.config)

        # Convert all the values to FileTemplate, handling defaults
        for key, templateStr in contents.items():
            if key == self.defaultKey:
                if not templateStr:
                    self.default = None
                else:
                    self.default = FileTemplate(templateStr)
            else:
                self.templates[key] = FileTemplate(templateStr)

    def __contains__(self, key):
        """Indicates whether the supplied key is present in the templates.

        Parameters
        ----------
        key : `LookupKey`
            Key to use to determine if a corresponding value is present
            in the templates.

        Returns
        -------
        in : `bool`
            `True` if the supplied key is present in the templates.
        """
        return key in self.templates

    def __getitem__(self, key):
        return self.templates[key]

    def validateTemplates(self, entities, logFailures=False):
        """Retrieve the template associated with each dataset type and
        validate the dimensions against the template.

        Parameters
        ----------
        entities : `DatasetType`, `DatasetRef`, or `StorageClass`
            Entities to validate against the matching templates.  Can be
            differing types.
        logFailures : `bool`, optional
            If `True`, output a log message for every validation error
            detected.

        Raises
        ------
        FileTemplateValidationError
            Raised if an entity failed validation.

        Notes
        -----
        See `FileTemplate.validateTemplate()` for details on the validation.
        """
        failed = []
        for entity in entities:
            try:
                matchKey, template = self.getTemplateWithMatch(entity)
            except KeyError as e:
                failed.append(e)
                if logFailures:
                    log.fatal("%s", str(e))
                continue
            try:
                template.validateTemplate(entity)
            except FileTemplateValidationError as e:
                failed.append(f"{e} (via key '{matchKey}')")
                if logFailures:
                    log.fatal("Template failure with key '%s': %s", str(matchKey), str(e))

        if failed:
            if len(failed) == 1:
                msg = str(failed[0])
            else:
                failMsg = ";\n".join(str(f) for f in failed)
                msg = f"{len(failed)} template validation failures: {failMsg}"
            raise FileTemplateValidationError(msg)

    def getLookupKeys(self):
        """Retrieve the look up keys for all the template entries.

        Returns
        -------
        keys : `set` of `LookupKey`
            The keys available for matching a template.
        """
        return set(self.templates)

    def getTemplateWithMatch(self, entity):
        """Retrieve the `FileTemplate` associated with the dataset type along
        with the lookup key that was a match for this template.

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
        matchKey : `LookupKey`
            The key that resulted in the successful match.
        template : `FileTemplate`
            Template instance to use with that dataset type.

        Raises
        ------
        KeyError
            Raised if no template could be located for this Dataset type.
        """

        # normalize the registry if not already done and we have access
        # to a universe
        if not self.normalized:
            try:
                universe = entity.dimensions.universe
            except AttributeError:
                pass
            else:
                self.normalizeDimensions(universe)

        # Get the names to use for lookup
        names = entity._lookupNames()

        # Get a location from the templates
        template = self.default
        source = self.defaultKey
        for name in names:
            if name in self.templates:
                template = self.templates[name]
                source = name
                break

        if template is None:
            raise KeyError(f"Unable to determine file template from supplied argument [{entity}]")

        log.debug("Got file %s from %s via %s", template, entity, source)

        return source, template

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
            Raised if no template could be located for this Dataset type.
        """
        _, template = self.getTemplateWithMatch(entity)
        return template

    def normalizeDimensions(self, universe):
        """Normalize template lookups that use dimensions.

        Parameters
        ----------
        universe : `DimensionUniverse`
            The set of all known dimensions. If `None`, returns without
            action.

        Notes
        -----
        Goes through all registered templates, and for keys that include
        dimensions, rewrites those keys to use a verified set of
        dimensions.

        Returns without action if the template keys have already been
        normalized.

        Raises
        ------
        ValueError
            Raised if a key exists where a dimension is not part of
            the ``universe``.
        """
        if self.normalized:
            return

        normalizeLookupKeys(self.templates, universe)

        self.normalized = True


class FileTemplate:
    """Format a path template into a fully expanded path.

    Parameters
    ----------
    template : `str`
        Template string.

    Raises
    ------
    FileTemplateValidationError
        Raised if the template fails basic validation.

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

    mandatoryFields = {"collection", "run"}
    """A set of fields, one of which must be present in a template."""

    datasetFields = {"datasetType", "component"}
    """Fields related to the supplied dataset, not a dimension."""

    specialFields = mandatoryFields | datasetFields
    """Set of special fields that are available independently of the defined
    Dimensions."""

    def __init__(self, template):
        if not isinstance(template, str):
            raise FileTemplateValidationError(f"Template ('{template}') does "
                                              "not contain any format specifiers")
        self.template = template

        # Do basic validation without access to dimensions
        self.validateTemplate(None)

    def __eq__(self, other):
        if not isinstance(other, FileTemplate):
            return False

        return self.template == other.template

    def __str__(self):
        return self.template

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.template}")'

    def fields(self, optionals=False, specials=False):
        """Return the field names used in this template.

        Parameters
        ----------
        optionals : `bool`
            If `True`, optional fields are included in the returned set.
        specials : `bool`
            If `True`, non-dimension fields are included.

        Returns
        -------
        names : `set`
            Names of fields used in this template

        Notes
        -----
        The returned set will include the special values such as `datasetType`
        and `component`.
        """
        fmt = string.Formatter()
        parts = fmt.parse(self.template)

        names = set()
        for literal, field_name, format_spec, conversion in parts:
            if field_name is not None:
                if "?" in format_spec and not optionals:
                    continue

                if not specials and field_name in self.specialFields:
                    continue

                names.add(field_name)

        return names

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
            Raised if the requested field is not defined and the field is
            not optional.  Or, `component` is specified but "component" was
            not part of the template.
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
                raise KeyError(f"'{field_name}' requested in template via '{self.template}' "
                               "but not defined and not optional")

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

    def validateTemplate(self, entity):
        """Compare the template against a representative entity that would
        like to use template.

        Parameters
        ----------
        entity : `DatasetType`, `DatasetRef`, or `StorageClass`
            Entity to compare against template.

        Raises
        ------
        FileTemplateValidationError
            Raised if the template is inconsistent with the supplied entity.

        Notes
        -----
        Validation will always include a check that mandatory fields
        are present and that at least one field refers to a dimension.
        If the supplied entity includes a `DimensionGraph` then it will be
        used to compare the available dimensions with those specified in the
        template.
        """

        # Check that the template has run or collection
        withSpecials = self.fields(specials=True, optionals=True)
        if not withSpecials & self.mandatoryFields:
            raise FileTemplateValidationError(f"Template '{self}' is missing a mandatory field"
                                              f" from {self.mandatoryFields}")

        # Check that there are some dimension fields in the template
        allfields = self.fields(optionals=True)
        if not allfields:
            raise FileTemplateValidationError(f"Template '{self}' does not seem to have any fields"
                                              " corresponding to dimensions.")

        # If we do not have dimensions available then all we can do is shrug
        if not hasattr(entity, "dimensions"):
            return

        # if this entity represents a component then insist that component
        # is present in the template. If the entity is not a component
        # make sure that component is not mandatory.
        try:
            if entity.isComponent():
                if "component" not in withSpecials:
                    raise FileTemplateValidationError(f"Template '{self}' has no component but "
                                                      f"{entity} refers to a component.")
            else:
                mandatorySpecials = self.fields(specials=True)
                if "component" in mandatorySpecials:
                    raise FileTemplateValidationError(f"Template '{self}' has mandatory component but "
                                                      f"{entity} does not refer to a component.")
        except AttributeError:
            pass

        # Get the dimension links to get the full set of available field names
        # Fall back to dataId keys if we have them but no links.
        # dataId keys must still be present in the template
        try:
            links = entity.dimensions.links()
        except AttributeError:
            try:
                links = set(entity.dataId.keys())
            except AttributeError:
                return

        required = self.fields(optionals=False)

        # Calculate any field usage that does not match a dimension
        if not required.issubset(links):
            raise FileTemplateValidationError(f"Template '{self}' is inconsistent with {entity}:"
                                              f" {required} is not a subset of {links}.")

        if not allfields.issuperset(links):
            raise FileTemplateValidationError(f"Template '{self}' is inconsistent with {entity}:"
                                              f" {allfields} is not a superset of {links}.")

        return
