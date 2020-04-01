#!/usr/bin/env python

import click
import logging

from lsst.daf.butler import Butler, ButlerConfig, Config, ValidationError


class Verbosity(object):
    def __init__(self):
        self.verbose = False


pass_verbosity = click.make_pass_decorator(Verbosity, ensure=True)


def verbose_option(f):
    def callback(ctx, param, value):
        verbose = ctx.ensure_object(Verbosity)
        verbose.verbose = value
        return value
    return click.option('-v', '--verbose', is_flag=True,
                        expose_value=False,
                        help='Turn on debug reporting.',
                        callback=callback)(f)


@click.group()
def cli():
    pass


@click.command(name='dump-config')
@click.argument('repo')
@click.option('--subset', '-s', type=str, help='Subset of a configuration to report. This can be any key in '
              "the hierarchy such as '.datastore.root' where the leading '.' specified the delimiter for "
              'the hierarchy.')
@click.option('--searchpath', '-p', type=str, multiple=True,
              help='Additional search paths to use for configuration overrides')
@click.option('--file', 'outfile', type=click.File('w'), default='-',
              help='Print the (possibly-expanded) configuration for a repository to a file, or to stdout '
              'by default.')
@verbose_option
@pass_verbosity
def dump_config(verbosity, repo, subset, searchpath, outfile):
    '''Dump either a subset or full Butler configuration to standard output.

    REPO is the filesystem path for an existing Butler repository or path to
    config file.
    '''
    if verbosity.verbose:
        logging.basicConfig(level=logging.DEBUG)

    config = ButlerConfig(repo, searchPaths=searchpath)

    if subset is not None:
        config = config[subset]

    try:
        config.dump(outfile)
    except AttributeError:
        print(config, file=outfile)


@click.command()
@click.argument('repo')
@click.option('--config', '-c', help='Path to an existing YAML config file to apply (on top of defaults).')
@click.option('--standalone', is_flag=True, help='Include all defaults in the config file in the repo, '
              'insulating the repo from changes in package defaults.')
@click.option('--override', '-o', is_flag=True, help='Allow values in the supplied config to override any '
              'repo settings.')
@click.option('--outfile', '-f', default=None, type=str, help='Name of output file to receive repository '
              'configuration. Default is to write butler.yaml into the specified repo.')
@verbose_option
@pass_verbosity
def create(verbosity, repo, config, standalone, override, outfile):
    '''Create an empty Gen3 Butler repository.

    REPO is the filesystem path for the new repository. Will be created if it
    does not exist.'''
    if verbosity.verbose:
        logging.basicConfig(level=logging.DEBUG)
    config = Config(config) if config is not None else None
    Butler.makeRepo(repo, config=config, standalone=standalone, forceConfigRoot=not override,
                    outfile=outfile)


@click.command(name='validate-config')
@click.argument('repo')
@click.option('--quiet', '-q', is_flag=True, help="Do not report individual failures.")
@click.option('--datasettype', '-d', type=str, multiple=True,
              help="Specific DatasetType(s) to validate (can be comma-separated)")
@click.option('--ignore', '-i', type=str, multiple=True,
              help="DatasetType(s) to ignore for validation (can be comma-separated)")
def validate_config(repo, quiet, datasettype, ignore):
    '''Validate the configuration files for a Gen3 Butler repository.

    REPO is the filesystem path for an existing Butler repository.
    '''
    logFailures = not quiet
    butler = Butler(config=repo)
    try:
        butler.validateConfiguration(logFailures=logFailures, datasetTypeNames=datasettype, ignore=ignore)
    except ValidationError:
        return False
    return True


cli.add_command(dump_config)
cli.add_command(create)
cli.add_command(validate_config)


def main():
    return cli()
