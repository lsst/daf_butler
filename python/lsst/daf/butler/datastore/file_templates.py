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

"""Support for file template string expansion."""

from __future__ import annotations

__all__ = ("FileTemplates", "FileTemplate", "FileTemplatesConfig", "FileTemplateValidationError")

import logging
import os.path
import string
from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from .._config import Config
from .._config_support import LookupKey, processLookupConfigs
from .._dataset_ref import DatasetRef
from .._exceptions import ValidationError
from .._storage_class import StorageClass
from ..dimensions import DataCoordinate

if TYPE_CHECKING:
    from .._dataset_type import DatasetType
    from ..dimensions import DimensionUniverse

log = logging.getLogger(__name__)


class FileTemplateValidationError(ValidationError):
    """Exception for file template inconsistent with associated DatasetType."""

    pass


class FileTemplatesConfig(Config):
    """Configuration information for `FileTemplates`."""

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
    universe : `DimensionUniverse`
        The set of all known dimensions, used to normalize any lookup keys
        involving dimensions.

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

    def __init__(
        self,
        config: FileTemplatesConfig | str,
        default: str | None = None,
        *,
        universe: DimensionUniverse,
    ):
        self.config = FileTemplatesConfig(config)
        self._templates = {}

        contents = processLookupConfigs(self.config, universe=universe)

        # Determine default to use -- defaults can be disabled if
        # we get a False or None
        defaultValue = contents.get(self.defaultKey, default)
        if defaultValue and not isinstance(defaultValue, str):
            raise RuntimeError(
                f"Default template value should be str or False, or None. Got '{defaultValue}'"
            )
        self.default = FileTemplate(defaultValue) if isinstance(defaultValue, str) and defaultValue else None

        # Convert all the values to FileTemplate, handling defaults
        for key, templateStr in contents.items():
            if key == self.defaultKey:
                continue
            if not isinstance(templateStr, str):
                raise RuntimeError(f"Unexpected value in file template key {key}: {templateStr}")
            self._templates[key] = FileTemplate(templateStr)

    @property
    def templates(self) -> Mapping[LookupKey, FileTemplate]:
        """Return collection of templates indexed by lookup key (`dict`)."""
        return MappingProxyType(self._templates)

    def __contains__(self, key: LookupKey) -> bool:
        """Indicate whether the supplied key is present in the templates.

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

    def __getitem__(self, key: LookupKey) -> FileTemplate:
        return self.templates[key]

    def validateTemplates(
        self, entities: Iterable[DatasetType | DatasetRef | StorageClass], logFailures: bool = False
    ) -> None:
        """Validate the templates.

        Retrieves the template associated with each dataset type and
        validates the dimensions against the template.

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
        unmatchedKeys = set(self.templates)
        failed = []
        for entity in entities:
            try:
                matchKey, template = self.getTemplateWithMatch(entity)
            except KeyError as e:
                # KeyError always quotes on stringification so strip here
                errMsg = str(e).strip("\"'")
                failed.append(errMsg)
                if logFailures:
                    log.critical("%s", errMsg)
                continue

            if matchKey in unmatchedKeys:
                unmatchedKeys.remove(matchKey)

            try:
                template.validateTemplate(entity)
            except FileTemplateValidationError as e:
                failed.append(f"{e} (via key '{matchKey}')")
                if logFailures:
                    log.critical("Template failure with key '%s': %s", matchKey, e)

        if logFailures and unmatchedKeys:
            log.warning("Unchecked keys: '%s'", ", ".join([str(k) for k in unmatchedKeys]))

        if failed:
            if len(failed) == 1:
                msg = str(failed[0])
            else:
                failMsg = ";\n".join(failed)
                msg = f"{len(failed)} template validation failures: {failMsg}"
            raise FileTemplateValidationError(msg)

    def getLookupKeys(self) -> set[LookupKey]:
        """Retrieve the look up keys for all the template entries.

        Returns
        -------
        keys : `set` of `LookupKey`
            The keys available for matching a template.
        """
        return set(self.templates)

    def getTemplateWithMatch(
        self, entity: DatasetRef | DatasetType | StorageClass
    ) -> tuple[LookupKey, FileTemplate]:
        """Retrieve the `FileTemplate` associated with the dataset type.

        Also retrieves the lookup key that was a match for this template.

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

    def getTemplate(self, entity: DatasetType | DatasetRef | StorageClass) -> FileTemplate:
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
     - run: `str`, name of the run this dataset was added with

    `run` must always be provided to ensure unique paths.

    More detailed information can be requested from dimensions by using a dot
    notation, so ``visit.name`` would use the name of the visit and
    ``detector.name_in_raft`` would use the name of the detector within the
    raft.

    The mini-language is extended to understand a "?" in the format
    specification. This indicates that a field is optional. If that
    Dimension is missing the field, along with the text before the field,
    unless it is a path separator, will be removed from the output path.

    By default any "/" in a dataId value will be replaced by "_" to prevent
    unexpected directories being created in the path. If the "/" should be
    retained then a special "/" format specifier can be included in the
    template.
    """

    mandatoryFields = {"run", "id"}
    """A set of fields, one of which must be present in a template."""

    datasetFields = {"datasetType", "component"}
    """Fields related to the supplied dataset, not a dimension."""

    specialFields = mandatoryFields | datasetFields
    """Set of special fields that are available independently of the defined
    Dimensions."""

    def __init__(self, template: str):
        if not isinstance(template, str):
            raise FileTemplateValidationError(
                f"Template ('{template}') does not contain any format specifiers"
            )
        self.template = template

        # Do basic validation without access to dimensions
        self.validateTemplate(None)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, FileTemplate):
            return False

        return self.template == other.template

    def __str__(self) -> str:
        return self.template

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}("{self.template}")'

    def fields(self, optionals: bool = False, specials: bool = False, subfields: bool = False) -> set[str]:
        """Return the field names used in this template.

        Parameters
        ----------
        optionals : `bool`
            If `True`, optional fields are included in the returned set.
        specials : `bool`
            If `True`, non-dimension fields are included.
        subfields : `bool`, optional
            If `True`, fields with syntax ``a.b`` are included. If `False`,
            the default, only ``a`` would be returned.

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
        for _, field_name, format_spec, _ in parts:
            if field_name is not None and format_spec is not None:
                if "?" in format_spec and not optionals:
                    continue

                if not specials and field_name in self.specialFields:
                    continue

                if "." in field_name and not subfields:
                    field_name, _ = field_name.split(".")

                names.add(field_name)

        return names

    def format(self, ref: DatasetRef) -> str:
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
        RuntimeError
            Raised if a template uses dimension record metadata but no
            records are attached to the `DatasetRef`.
        """
        # Extract defined non-None dimensions from the dataId.
        # This guards against Nones being explicitly present in the data ID
        # (which can happen if, say, an exposure has no filter), as well as
        # the case where only required dimensions are present (which in this
        # context should only happen in unit tests; in general we need all
        # dimensions to fill out templates).
        fields: dict[str, object] = {
            k: ref.dataId.get(k) for k in ref.datasetType.dimensions.names if ref.dataId.get(k) is not None
        }
        # Extra information that can be included using . syntax
        extras = {}
        if isinstance(ref.dataId, DataCoordinate):
            if ref.dataId.hasRecords():
                extras = {k: ref.dataId.records[k] for k in ref.dataId.dimensions.elements}
            skypix_alias = self._determine_skypix_alias(ref)
            if skypix_alias is not None:
                fields["skypix"] = fields[skypix_alias]
                if extras:
                    extras["skypix"] = extras[skypix_alias]

        datasetType = ref.datasetType
        fields["datasetType"], component = datasetType.nameAndComponent()

        usedComponent = False
        if component is not None:
            fields["component"] = component

        fields["run"] = ref.run
        fields["id"] = ref.id

        fmt = string.Formatter()
        parts = fmt.parse(self.template)
        output = ""

        for literal, field_name, format_spec, _ in parts:
            if field_name == "component":
                usedComponent = True

            if format_spec is None:
                output = output + literal
                continue

            # Should only happen if format_spec is None
            if field_name is None:
                raise RuntimeError(f"Unexpected blank field_name encountered in {self.template} [{literal}]")

            if "?" in format_spec:
                optional = True
                # Remove the non-standard character from the spec
                format_spec = format_spec.replace("?", "")
            else:
                optional = False

            # Check for request for additional information from the dataId
            if "." in field_name:
                primary, secondary = field_name.split(".")
                if primary in extras:
                    record = extras[primary]
                    # Only fill in the fields if we have a value, the
                    # KeyError will trigger below if the attribute is missing,
                    # but only if it is not optional. This is most likely
                    # a typo in the metadata field and so should be reported
                    # even if optional.
                    if hasattr(record, secondary):
                        fields[field_name] = getattr(record, secondary)
                    else:
                        # Is a log message sufficient?
                        log.info(
                            "Template field %s could not be resolved because metadata field %s"
                            " is not understood for dimension %s. Template entry will be ignored",
                            field_name,
                            secondary,
                            primary,
                        )
                elif primary in fields:
                    # We do have an entry for the primary but do not have any
                    # secondary entries. This is likely a problem with the
                    # code failing to attach a record to the DatasetRef.
                    raise RuntimeError(
                        f"No metadata records attached to dataset {ref}"
                        f" when attempting to expand field {field_name}."
                        " Either expand the DatasetRef or change the template."
                    )

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
                raise KeyError(
                    f"'{field_name}' requested in template via '{self.template}' "
                    "but not defined and not optional"
                )

            # Handle "/" in values since we do not want to be surprised by
            # unexpected directories turning up
            replace_slash = True
            if "/" in format_spec:
                # Remove the non-standard character from the spec
                format_spec = format_spec.replace("/", "")
                replace_slash = False

            if isinstance(value, str):
                # Replace spaces with underscores for more friendly file paths
                value = value.replace(" ", "_")
                if replace_slash:
                    value = value.replace("/", "_")

            # Now use standard formatting
            output = output + literal + format(value, format_spec)

        # Replace periods with underscores in the non-directory part to
        # prevent file extension confusion. Also replace # in the non-dir
        # part to avoid confusion with URI fragments
        head, tail = os.path.split(output)
        tail = tail.replace(".", "_")
        tail = tail.replace("#", "HASH")
        output = os.path.join(head, tail)

        # Complain if we were meant to use a component
        if component is not None and not usedComponent:
            raise KeyError(f"Component '{component}' specified but template {self.template} did not use it")

        # Since this is known to be a path, normalize it in case some double
        # slashes have crept in
        path = os.path.normpath(output)

        # It should not be an absolute path (may happen with optionals)
        if os.path.isabs(path):
            path = os.path.relpath(path, start="/")

        return path

    def validateTemplate(self, entity: DatasetRef | DatasetType | StorageClass | None) -> None:
        """Compare the template against supplied entity that wants to use it.

        Parameters
        ----------
        entity : `DatasetType`, `DatasetRef`, or `StorageClass`
            Entity to compare against template. If `None` is given only
            very basic validation of templates will be performed.

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
        # Check that the template has run
        withSpecials = self.fields(specials=True, optionals=True)

        if "collection" in withSpecials:
            raise FileTemplateValidationError(
                "'collection' is no longer supported as a file template placeholder; use 'run' instead."
            )

        if not withSpecials & self.mandatoryFields:
            raise FileTemplateValidationError(
                f"Template '{self}' is missing a mandatory field from {self.mandatoryFields}"
            )

        # Check that there are some dimension fields in the template
        # The id is allowed instead if present since that also uniquely
        # identifies the file in the datastore.
        allfields = self.fields(optionals=True)
        if not allfields and "id" not in withSpecials:
            raise FileTemplateValidationError(
                f"Template '{self}' does not seem to have any fields corresponding to dimensions."
            )

        # Require that if "id" is in the template then it must exist in the
        # file part -- this avoids templates like "{id}/fixed" where the file
        # name is fixed but the directory has the ID.
        if "id" in withSpecials:
            file_part = os.path.split(self.template)[-1]
            if "{id}" not in file_part:
                raise FileTemplateValidationError(
                    f"Template '{self}' includes the 'id' but that ID is not part of the file name."
                )

        # If we do not have dimensions available then all we can do is shrug
        if not hasattr(entity, "dimensions"):
            return

        # Mypy does not know about hasattr so help it out
        if entity is None:
            return

        # if this entity represents a component then insist that component
        # is present in the template. If the entity is not a component
        # make sure that component is not mandatory.
        try:
            # mypy does not see the except block so complains about
            # StorageClass not supporting isComponent
            if entity.isComponent():  # type: ignore
                if "component" not in withSpecials:
                    raise FileTemplateValidationError(
                        f"Template '{self}' has no component but {entity} refers to a component."
                    )
            else:
                mandatorySpecials = self.fields(specials=True)
                if "component" in mandatorySpecials:
                    raise FileTemplateValidationError(
                        f"Template '{self}' has mandatory component but "
                        f"{entity} does not refer to a component."
                    )
        except AttributeError:
            pass

        # From here on we need at least a DatasetType
        # Mypy doesn't understand the AttributeError clause below
        if isinstance(entity, StorageClass):
            return

        # Get the dimension links to get the full set of available field names
        # Fall back to dataId keys if we have them but no links.
        # dataId keys must still be present in the template
        try:
            minimal = set(entity.dimensions.required.names)
            maximal = set(entity.dimensions.names)
        except AttributeError:
            try:
                minimal = set(entity.dataId.keys().names)  # type: ignore
                maximal = minimal
            except AttributeError:
                return

        # Replace specific skypix dimensions with generic one
        skypix_alias = self._determine_skypix_alias(entity)
        if skypix_alias is not None:
            minimal.add("skypix")
            maximal.add("skypix")
            minimal.remove(skypix_alias)
            maximal.remove(skypix_alias)

        required = self.fields(optionals=False)

        # Calculate any field usage that does not match a dimension
        if not required.issubset(maximal):
            raise FileTemplateValidationError(
                f"Template '{self}' is inconsistent with {entity}: {required} is not a subset of {maximal}."
            )

        if not allfields.issuperset(minimal):
            raise FileTemplateValidationError(
                f"Template '{self}' is inconsistent with {entity}:"
                f" {allfields} is not a superset of {minimal}."
            )

        return

    def _determine_skypix_alias(self, entity: DatasetRef | DatasetType) -> str | None:
        """Return the dimension name that refers to a sky pixel.

        Parameters
        ----------
        ref : `DatasetRef` or `DatasetType`
            The entity to examine.

        Returns
        -------
        alias : `str`
            If there is a sky pixelization in the supplied dataId, return
            its name, else returns `None`.  Will return `None` also if there
            is more than one sky pix dimension in the data ID or if the
            dataID is not a `DataCoordinate`
        """
        alias = None

        if isinstance(entity, DatasetRef):
            entity = entity.datasetType

        # If there is exactly one SkyPixDimension in the data ID, alias its
        # value with the key "skypix", so we can use that to match any
        # skypix dimension.
        # We restrict this behavior to the (real-world) case where the
        # data ID is a DataCoordinate, not just a dict.  That should only
        # not be true in some test code, but that test code is a pain to
        # update to be more like the real world while still providing our
        # only tests of important behavior.
        if len(entity.dimensions.skypix) == 1:
            (alias,) = entity.dimensions.skypix.names
        return alias
