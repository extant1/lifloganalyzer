# -*- coding: utf-8 -*-

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker

from compat import basestring
from config import Config

Base = declarative_base()
engine = create_engine(Config.DATABASE)
Session = sessionmaker(bind=engine)

session = Session()


class CRUDMixin(object):
    """Mixin that adds convenience methods for CRUD (create, read, update, delete) operations."""

    @classmethod
    def create(cls, **kwargs):
        """Create a new record and save it the database."""
        instance = cls(**kwargs)
        return instance.save()

    def update(self, commit=True, **kwargs):
        """Update specific fields of a record."""
        for attr, value in kwargs.items():
            setattr(self, attr, value)
        return commit and self.save() or self

    def save(self, commit=True):
        """Save the record."""
        session.add(self)
        if commit:
            session.commit()
        return self

    def delete(self, commit=True):
        """Remove the record from the database."""
        session.delete(self)
        return commit and session.commit()


class Model(CRUDMixin, Base):
    """Base model class that includes CRUD convenience methods."""

    __abstract__ = True


class SurrogatePK(object):
    """A mixin that adds a surrogate integer 'primary key' column named ``id`` to any declarative-mapped class."""

    __table_args__ = {'extend_existing': True}

    id = Column(Integer, primary_key=True)

    @classmethod
    def get_by_id(cls, record_id):
        """Get record by ID."""
        if any(
                (isinstance(record_id, basestring) and record_id.isdigit(),
                 isinstance(record_id, (int, float))),
        ):
            return cls.query.get(int(record_id))
        return None


def reference_col(tablename, nullable=False, pk_name='id', **kwargs):
    """Column that adds primary key foreign key reference.

    Usage: ::

        category_id = reference_col('category')
        category = relationship('Category', backref='categories')
    """
    return Column(
        ForeignKey('{0}.{1}'.format(tablename, pk_name)),
        nullable=nullable, **kwargs)


class User(Model):
    __tablename__ = 'user'

    account = Column(String(64), primary_key=True)
    events = relationship('Event', backref='user')

    def __repr__(self):
        return '{account}'.format(account=self.account)


class Event(Model, SurrogatePK):
    __tablename__ = 'event'

    acctid = Column(Integer, ForeignKey('user.account'))
    state = Column(Integer)
    ipaddr = Column(String(15))
    date = Column(DateTime, nullable=False)

    def __repr__(self):
        return '<{id}, {date}, {state}, {acct}, {ipaddr}>'.format(id=self.id, date=self.date, state=self.state,
                                                                  acct=self.acctid, ipaddr=self.ipaddr)


class Analyzer(Model, SurrogatePK):
    __tablename__ = 'analyzer'

    last_scan = Column(DateTime)
    last_file = Column(String())

    def __repr__(self):
        return '{id}'.format(id=self.id)
