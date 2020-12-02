from peewee import *
from repository.Device import Device
from repository.Visit import Visit
from lib.db import DB


connection = DB.getConnection()
connection.create_tables([Device, Visit])