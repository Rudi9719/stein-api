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

