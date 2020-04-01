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
