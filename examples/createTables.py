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
