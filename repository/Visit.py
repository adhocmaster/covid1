from peewee import *
from repository.Base import Base
from repository.Device import Device

class Visit(Base):
    
    _id = AutoField(primary_key=True)
    device = ForeignKeyField(Device, backref="+")
    lat = FloatField()
    lon = FloatField()
    datetime = DateTimeField()
    td = TimeField()
    dw = FixedCharField(max_length=3)
    tz = SmallIntegerField()
    gridX = SmallIntegerField()
    gridY = SmallIntegerField()
    gridId = SmallIntegerField()


    
