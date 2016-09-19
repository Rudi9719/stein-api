#!/usr/bin/env python

from controller.base import BaseController
from frameworks.bottle import view, static_file


class HomeController(BaseController):
    
    
    @view('home/index')
    def index(self):
        pass

