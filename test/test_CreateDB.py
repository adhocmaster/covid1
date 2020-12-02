import unittest
from peewee import *
from repository.Device import Device
from repository.Visit import Visit
from repository.Polygon import Polygon
from lib.db import DB

class test_CreateDB(unittest.TestCase):

    def test_Create(self):
        
        connection = DB.getConnection()
        connection.create_tables([Device, Visit, Polygon])