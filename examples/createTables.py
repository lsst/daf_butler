from sqlalchemy import create_engine, LargeBinary
from sqlalchemy.orm import sessionmaker

from lsst.butler.orm.schema import Base

def createDatabase(dbname):
    engine = create_engine(dbname)
    maker = sessionmaker()
    maker.configure(bind=engine)
    Base.metadata.create_all(engine)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Create SQL schema.')
    parser.add_argument('--dbname', dest='dbname', help='Name of database to connect to', default='sqlite:///:memory:')
    
    args = parser.parse_args()

    createDatabase(args.dbname)
