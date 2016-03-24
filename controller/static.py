#!/usr/bin/env python
from controller.base import BaseController
from api.error import Error
from frameworks.bottle import Bottle, response, static_file

class StaticController(BaseController):

    def file(self, filename):
        return static_file(filename, root='static/')