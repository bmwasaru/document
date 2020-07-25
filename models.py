import uuid

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, Text, JSON

engine = create_engine('sqlite:///:memory:', echo=True)
Base = declarative_base()


class Document(Base):
    """Simple document storage.
    API
    ---
    CREATE
        doc = Document(key="britone", type="person", data={"email": "britone@example.com})
        print(doc.key)
        print(doc.type)
        print(doc.data["email"])
    READ
        doc = Document.find("britone")
        people = Document.search(type="person")
        managers = Document.search(type="person", role="manager")
    UPDATE:
        doc.update(email="britone@new-domain.com")
        doc.save()
    DELETE:
        doc.delete()
    """
    id = Column(Integer, primary_key=True)
    key = Column(Text, nullable=False, unique=True)
    type = Column(Text, nullable=False)
    data = Column(JSON)

    def __init__(self, key, data=None):
        """Creates a new Document with specified key and type.
        If key is None, a unique key is automatically generated.
        """
        self.key = key or self._generate_unique_key()
        self.type = type
        self.revision = 0
        self.data = data or {}
        
    def _generate_unique_key(self):
        """Generates a unique random key using UUID.
        """
        return uuid.uuid4().hex

    def update(self, **kwargs):
        self.data.update(**kwargs)

    def save(self):
        db.session.add(self)
        db.session.commit()

    def delete(self):
        db.session.delete(self)
        db.session.commit()

    @staticmethod
    def find(key, type=None):
        """Find the document with the specified key and optionally type.
        """
        q = Document.query.filter_by(key)
        if type:
            q.filter_by(type=type)
        return q.first()

    @staticmethod
    def search(type, **kwargs):
        """Searchs for all documents of specified type matching all the optional constraints
        specified by keyword arguments.
            Document.search("user", email="britone@example.com")
        """
        q = Document.query.filter_by(type=type)
        for name, value in kwargs.items():
            q = q.filter(Document.data[name].astext == value)
        return q.all()

    def __repr__(self):
        return "<Document({!r})>".format(self.key)


Base.metadata.create_all(engine)
