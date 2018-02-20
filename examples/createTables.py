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

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from lsst.daf.butler.schema import metadata


def createDatabase(dbname):
    engine = create_engine(dbname)
    maker = sessionmaker()
    maker.configure(bind=engine)
    metadata.create_all(engine)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create SQL schema.')
    parser.add_argument('--dbname', dest='dbname', help='Name of database to connect to',
                        default='sqlite:///:memory:')

    args = parser.parse_args()

    createDatabase(args.dbname)
