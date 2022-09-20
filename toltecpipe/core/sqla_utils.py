#!/usr/bin/env python

import sqlalchemy as sa
from sqlalchemy.orm import sessionmaker


class SqlaDB(object):
    """A class that provide access to database related entities
    similar to that of `flask_sqlalchemy.SQLAlchemy`.

    """

    def __init__(self, engine, metadata, Session, session=None):
        self._engine = engine
        self._metadata = metadata
        self._Session = Session
        self._session = session

    @property
    def engine(self):
        return self._engine

    @property
    def metadata(self):
        return self._metadata

    @property
    def Session(self):
        return self._Session

    @property
    def session(self):
        return self._Session

    @property
    def tables(self):
        return self.metadata.tables

    @classmethod
    def from_uri(cls, uri, engine_options=None):
        engine = sa.create_engine(uri, **(engine_options or dict()))
        metadata = sa.MetaData(bind=engine)
        Session = sessionmaker(bind=engine)
        return cls(engine=engine, metadata=metadata, Session=Session)

    @classmethod
    def from_flask_sqla(cls, db, bind=None):
        engine = db.get_engine(bind=bind)
        metadata = sa.MetaData()
        Session = sessionmaker(bind=engine)
        session = db.create_scoped_session(options={"bind": engine})
        return cls(engine=engine, metadata=metadata, Session=Session, session=session)

    def reflect_tables(self):
        self.metadata.reflect(bind=self.engine)
        return self.tables

    def session_context(self):
        session = self._session
        if self._session is None:
            return self.Session.begin()
        return self.session.begin()
