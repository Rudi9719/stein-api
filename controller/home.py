#!/usr/bin/env python

from controller.base import BaseController
from frameworks.bottle import view, static_file
from blog.blogger import Blogger

blog_mgr = Blogger()

class HomeController(BaseController):
    
    
    @view('home/index')
    def index(self):
        pass
    #return dict(title='Home', page_name='Home', latest_post=blog_mgr.last_post())

#@view('home/index')
#def blog(self):
#return dict(title='Blog', page_name='Blog', latest_post=blog_mgr.all_posts())



