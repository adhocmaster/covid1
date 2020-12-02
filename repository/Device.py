from peewee import *
from repository.Base import Base
from repository.Polygon import Polygon
class Device(Base):

    _id = AutoField(primary_key=True)
    hId = CharField(unique=True)
    polygon = ForeignKeyField(Polygon, backref="+")
    homeGridId = SmallIntegerField()

    
