from peewee import Model
from lib.db import DB

class Base(Model):

    class Meta:
        database = DB.getConnection()
    
    