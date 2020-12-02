from peewee import *
from repository.Base import Base

class Polygon(Base):

    _id = AutoField(primary_key=True)
    name = CharField(unique=True)

    
