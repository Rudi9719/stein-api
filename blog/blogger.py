#!/usr/bin/env python
import json
import glob
from api.error import Error
from frameworks.bottle import Bottle, response, static_file, view, template
from controller.base import BaseController
class Blogger(BaseController):

    @view('home/post')
    def last_post(self):
        data = json.loads(open('blog/posts/latest_post.json').read())
        return dict(post_title=data["post_title"], post_content=data["post_content"], link_text=data["link_text"])

    @view('home/blog')
    def all_posts(self):
        posts = [{}]
        for post in glob.glob('blog/posts/*.json'):
            post_data = json.loads(open(post).read())
            posts.append(dict(post_title=post_data["post_title"], post_content=post_data["post_content"], link_text=post_data["link_text"]))
            print(posts)
        return posts

