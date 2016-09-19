#!/usr/bin/env python

import json
import logging

from uuid import uuid4
from utils import user_utils
from google.appengine.runtime import apiproxy_errors
from google.appengine.api import search, users
from google.appengine.ext import ndb
from google.appengine.ext.db import NotSavedError
from google.net.proto.ProtocolBuffer import ProtocolBufferDecodeError

def pieces(string):
    pieces = []
    baddies = ["http://", "https://", "mailto://", ".", "/", ".com", ".net", ".gov",
               ".org", ".edu", "html", "htm", "asp", "%20", "?", "!", "&", "=", "-",
               "#", "%", "~", "_"]
    for baddie in baddies:
        string = string.replace(baddie, '')

    for word in string.split():
        cursor = 1
        while True:
            pieces.append(word[:cursor])

            for i in range(len(word) - cursor + 1):
                pieces.append(word[i:i + cursor])
            if cursor == len(word):
                break
            cursor += 1

    ret = ','.join(pieces)
    if len(ret) > 1048000:
        return ret[0:1048000]
    else:
        return ret

class BaseModel(ndb.Model):
    created = ndb.DateTimeProperty(auto_now_add=True)
    modified = ndb.DateTimeProperty(auto_now=True)

    def __repr__(self):
        return json.dumps(self.to_external_dict())

    @property
    def external_key(self):
        key = None
        try:
            key = self.key
        except NotSavedError:
            pass

        if key is None:
            return None

        return key.urlsafe()

    @classmethod
    def fetch_all(cls):
        return cls.query().fetch()

    @classmethod
    def fetch_all_in_order(cls, order):
        return cls.query().order(order).fetch()

    @classmethod
    def count_all(cls):
        return cls.query().count(keys_only=True)

    @classmethod
    def delete_all(cls):
        keys = []
        for entity in cls.query().fetch():
            keys.append(entity.key)
        if len(keys) > 0:
            ndb.delete_multi(keys)

    @classmethod
    def get_by_external_key(cls, external_key):
        key = cls.key_from_external_key(external_key)
        if not key:
            return None
        return key.get()

    @classmethod
    def get_by_external_keys(cls, external_keys):

        # Get all the keys
        keys = []
        for external_key in external_keys:
            key = cls.key_from_external_key(external_key)
            if key:
                keys.append(key)

        # Return entities
        return filter(None, ndb.get_multi(keys))

    @classmethod
    def get_by_keys(cls, keys):
        return filter(None, ndb.get_multi(keys))

    @classmethod
    def delete_by_external_key(cls, external_key):
        return cls.key_from_external_key(external_key).delete()

    @classmethod
    def key_from_external_key(cls, external_key):
        key = None
        try:
            key = ndb.Key(urlsafe=external_key)
            if key and key.kind() != cls.__name__:
                key = None
        except ProtocolBufferDecodeError as e:
            logging.exception(
                "An error occurred attempting to get key from external key: {0}. {1}".format(external_key, e.message))
        except TypeError as e:
            logging.exception(
                "An error occurred attempting to get key from external key: {0}. {1}".format(external_key, e.message))
        return key

    def to_external_dict(self):
        return {
            "id": self.external_key,
            "created": self.created.isoformat() if self.created else None,
            "modified": self.modified.isoformat() if self.modified else None
        }

class ConfigData(BaseModel):
    domain = ndb.StringProperty(required=True)
    admin_users = ndb.StringProperty()

    @classmethod
    def get_data(cls):
        return cls.query().get()



class LinkVersion(BaseModel):
    destination = ndb.StringProperty(required=True)
    modified_by = ndb.StringProperty(required=True)
    created_by = ndb.StringProperty(required=True)
    link_id = ndb.StringProperty(required=True)
    owner = ndb.StringProperty(required=True)

    @classmethod
    def fetch_by_created_by(cls, created_by):
        return cls.query(cls.created_by == created_by).order(-cls.created).fetch()

    @classmethod
    def fetch_by_link_id(cls, link_id):
        return cls.query(cls.link_id == link_id).order(-cls.created).fetch()

    @property
    def clicks(self):
        link_stats = LinkStatistics.get_by_link_id(self.link_id)
        if link_stats:
            return link_stats.count
        else:
            return 0

    def to_external_dict(self):
        ext_dict = super(LinkVersion, self).to_external_dict()
        ext_dict.update({
            "clicks": self.clicks,
            "destination": self.destination,
            "modified_by": self.modified_by,
            "created_by": self.created_by,
            "link_id": self.link_id,
            "owner": self.owner
        })
        return ext_dict


class Link(BaseModel):
    name = ndb.StringProperty(required=True)
    created_by = ndb.StringProperty(required=True)
    modified_by = ndb.StringProperty(required=True)
    link_id = ndb.StringProperty(required=True)
    destination = ndb.StringProperty(required=True)
    owner = ndb.StringProperty(required=True)

    def create_shortlink(self, name, created_by, owner, destination):
        if Link.get_by_name(name):
            return False
        else:
            self.name = name
            self.link_id = str(uuid4())
            self.created_by = created_by
            self.owner = owner
            self.destination = destination
            self.modified_by = created_by
            return True


    def _post_put_hook(self, future):
        dest_doc = search.Document(doc_id=str(self.key.id()), fields=[
            search.TextField(name='destination', value=self.destination),
            search.TextField(name='name', value=self.name),
            search.TextField(name='pieces', value=pieces(self.destination)),
            search.TextField(name='id', value=str(self.external_key))
        ])
        name_doc = search.Document(doc_id=str(self.key.id()), fields=[
            search.TextField(name='destination', value=self.destination),
            search.TextField(name='name', value=self.name),
            search.TextField(name='pieces', value=pieces(self.name)),
            search.TextField(name='id', value=str(self.external_key))
        ])
        search.Index('destinations').put(dest_doc)
        search.Index('name').put(name_doc)

    def transfer_ownership(self, new_owner):
        self.update_link(self.destination, self.modified_by)
        self.owner = new_owner
        try:
            self.put()
        except apiproxy_errors.OverQuotaError:
            logging.info("Over quota error.")
            return dict(code=503, message="Link not indexed, over quota.", valid=True)

    def click(self):
        stats = LinkStatistics().get_by_link_id(self.link_id)
        if stats:
            stats.increment()
        else:
            stats = LinkStatistics()
            stats.count = 1
            stats.link_id = self.link_id
            stats.put()
        return self.destination

    def update_link(self, destination, modified_by):
        old_version = LinkVersion()
        old_version.link_id = self.link_id
        old_version.created_by = self.created_by
        old_version.modified_by = self.modified_by
        old_version.destination = self.destination
        old_version.owner = self.owner
        old_version.put()
        self.destination = destination
        self.modified_by = modified_by
        try:
            self.put()
        except apiproxy_errors.OverQuotaError:
            logging.info("Over quota error.")
            return dict(code=503, message="Link not indexed, over quota.", valid=True)

    @classmethod
    def _pre_delete_hook(cls, key):
        id = key.get().link_id
        for version in LinkVersion.fetch_by_link_id(id):
            version.delete_by_external_key(version.external_key)
        stats = LinkStatistics.get_by_link_id(id)
        if stats:
            stats.delete_by_external_key(stats.external_key)


    @classmethod
    def get_by_name(cls, name):
        return cls.query(cls.name == name).get()

    @classmethod
    def fetch_sorted(cls, page, sort, type, user, count=30):
        if "name" in sort:
            if "mine" in type:
                links = cls.query(cls.owner == user.email()).order(cls.name).fetch(count, offset=(count * page))
            else:
                links = cls.query().order(cls.name).fetch(count, offset=(count * page))
        elif "destination" in sort:
            if "mine" in type:
                links = cls.query(cls.owner == user.email()).order(cls.destination).fetch(count, offset=(count * page))
            else:
                links = cls.query().order(cls.destination).fetch(count, offset=(count * page))
        else:
            links = cls.query(cls.owner == user.email()).order(cls.name).fetch(count, offset=(count * page))
        return links

    @classmethod
    def fetch_by_owner(cls, owner_email):
        return cls.query(cls.owner == owner_email).fetch()

    @property
    def clicks(self):
        link_stats = LinkStatistics.get_by_link_id(self.link_id)
        if link_stats:
            return int(link_stats.count)
        else:
            return 0

    @property
    def is_editable(self):
        user = users.get_current_user()
        if user:
            if users.get_current_user().email() == self.owner:
                return 'true'
            elif user_utils.is_current_user_admin():
                return 'true'
            else:
                return 'false'
        else:
            return 'false'

    def to_external_dict(self):
        ext_dict = super(Link, self).to_external_dict()
        ext_dict.update({
            "name": self.name,
            "created_by": self.created_by,
            "modified_by": self.modified_by,
            "link_id": self.link_id,
            "destination": self.destination,
            "clicks": self.clicks,
            "owner": self.owner,
            "editable": self.is_editable

        })
        return ext_dict


class LinkStatistics(BaseModel):
    link_id = ndb.StringProperty(required=True)
    count = ndb.IntegerProperty()

    @classmethod
    def get_by_link_id(cls, link_id):
        existing_stats = cls.query(cls.link_id == link_id).get()
        if existing_stats:
            return existing_stats
        else:
            return None

    def create_stats(self, link_id):
        new_stats = LinkStatistics()
        new_stats.link_id = link_id
        new_stats.count = 0
        return new_stats

    def increment(self):
        self.count = self.count + 1
        self.put()
        return self.count


    def to_external_dict(self):
        ext_dict = super(LinkStatistics, self).to_external_dict()
        ext_dict.update({
            "count": self.count,
            "link_id": self.link_id
        })
        return ext_dict
