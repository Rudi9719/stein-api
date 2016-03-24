#!/usr/bin/env python

import httplib
import json
import logging

from frameworks.bottle import HTTPError

class FordError(HTTPError):

	def __init__(self, status, code, message):
		body = json.dumps({
			"valid": False,
			"code": code,
			"message": message
		})
		super(FordError, self).__init__(status, body)
		self.content_type = "application/json"

	def __str__(self):
		return str(self.status_code) + " " + self.body


class Error:

	@classmethod
	def handle_error(cls, response, error):
		logging.error("error=%s", error)
		if isinstance(error, FordError):
			if error.content_type is not None:
				response.content_type = error.content_type
			return error.body
		else:
			logging.error("Had an unknown error of type %s:\n%s\n%s", error.__class__.__module__ + "." + error.__class__.__name__, error.exception, error.traceback)
			unknown_error = FordError(httplib.INTERNAL_SERVER_ERROR, 0, "An unknown error occurred")
			return cls.handle_error(response, unknown_error)

	@classmethod
	def _raise_error(cls, response, status, code, message):
		raise FordError(status, code, message)

	@classmethod
	def raise_not_found(cls, response, message="Not found"):
		logging.error(message)
		cls._raise_error(response, httplib.NOT_FOUND, 1, message)

	@classmethod
	def raise_forbidden(cls, response, message="Forbidden"):
		logging.error(message)
		cls._raise_error(response, httplib.FORBIDDEN, 2, message)

	@classmethod
	def raise_unauthorized(cls, response, message="Unauthorized"):
		logging.error(message)
		cls._raise_error(response, httplib.UNAUTHORIZED, 3, message)

	@classmethod
	def raise_bad_request(cls, response, message="Bad request"):
		logging.error(message)
		cls._raise_error(response, httplib.BAD_REQUEST, 4, message)

	@classmethod
	def raise_required_field(cls, response, field):
		message = "Missing the required field %s" % (field)
		cls._raise_error(response, httplib.BAD_REQUEST, 5, message)

	@classmethod
	def raise_user_already_exists(cls, response, field):
		message = "User already exists with email %s" % (field)
		cls._raise_error(response, httplib.BAD_REQUEST, 6, message)

	@classmethod
	def raise_invalid_format(cls, response, field, value, format):
		message = "{0} field has invalid format: {1}. Valid format matches {2}.".format(field, value, format)
		cls._raise_error(response, httplib.BAD_REQUEST, 7, message)

	@classmethod
	def assert_field_required(cls, response, field, value):
		if value is None or len(value) == 0:
			Error.raise_required_field(response, field)

