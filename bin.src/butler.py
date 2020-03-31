#!/usr/bin/env python

import click
import logging
import sys

from lsst.daf.butler import ButlerConfig


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


cli.add_command(dump_config)


if __name__ == '__main__':
    cli()
