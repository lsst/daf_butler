#!/usr/bin/env python

import click
import logging
import sys

from lsst.daf.butler import Butler, ButlerConfig, Config


@click.group()
def cli():
    pass


@click.command(name='dump-config')
@click.argument('root')
@click.option('--subset', '-s', type=str, help='Subset of a configuration to report. This can be any key in '
              "the hierarchy such as '.datastore.root' where the leading '.' specified the delimiter for "
              'the hierarchy.')
@click.option('--searchpath', '-p', type=str, multiple=True,
              help='Additional search paths to use for configuration overrides')
@click.option('--verbose', '-v', is_flag=True, help='Turn on debug reporting.')
def dump_config(root, subset, searchpath, verbose):
    '''Dump either a subset or full Butler configuration to standard output.

    ROOT is the filesystem path for an existing Butler repository or path to
    config file.
    '''
    if verbose:
        logging.basicConfig(level=logging.DEBUG)

    config = ButlerConfig(root, searchPaths=searchpath)

    if subset is not None:
        config = config[subset]

    outfile = sys.stdout
    try:
        config.dump(outfile)
    except AttributeError:
        print(config, file=outfile)


@click.command()
@click.argument('root')
@click.option('--config', '-c', help='Path to an existing YAML config file to apply (on top of defaults).')
@click.option('--standalone', is_flag=True, help='Include all defaults in the config file in the repo, '
              'insulating the repo from changes in package defaults.')
@click.option('--override', '-o', is_flag=True, help='Allow values in the supplied config to override any '
              'root settings.')
@click.option('--outfile', '-f', default=None, type=str, help='Name of output file to receive repository '
              'configuration. Default is to write butler.yaml into the specified root.')
@click.option('--verbose', '-v', is_flag=True, help='Turn on debug reporting.')
def create(root, config, standalone, override, outfile, verbose):
    '''Create an empty Gen3 Butler repository.

    ROOT is the filesystem path for the new repository. Will be created if it
    does not exist.'''
    config = Config(config) if config is not None else None
    Butler.makeRepo(root, config=config, standalone=standalone, forceConfigRoot=not override,
                    outfile=outfile)


cli.add_command(dump_config)
cli.add_command(create)

if __name__ == '__main__':
    cli()
