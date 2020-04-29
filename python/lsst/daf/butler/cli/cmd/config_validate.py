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

import click

from ... import Butler, ValidationError
from ..opt import dataset_type_option, repo_option
from ..utils import split_commas


@click.command()
@click.pass_context
@repo_option(required=True)
@click.option("--quiet", "-q", is_flag=True, help="Do not report individual failures.")
@dataset_type_option(help="Specific DatasetType(s) to validate.")
@click.option("--ignore", "-i", type=str, multiple=True, callback=split_commas,
              help="DatasetType(s) to ignore for validation.")
def config_validate(ctx, repo, quiet, dataset_type, ignore):
    """Validate the configuration files for a Gen3 Butler repository."""
    logFailures = not quiet
    butler = Butler(config=repo)
    try:
        butler.validateConfiguration(logFailures=logFailures, datasetTypeNames=dataset_type, ignore=ignore)
    except ValidationError:
        ctx.exit(1)
    click.echo("No problems encountered with configuration.")
