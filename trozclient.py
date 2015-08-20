# -*- coding: utf-8 -*-

from os import environ
import requests, logging, json

class TrozClient(object):

	_filters = []
	_timeframe = ''

	_base_url = 'https://api.troz.io/1.0/projects'
	_base_headers = {'Content-Type': 'application/json'}

	def __init__(self, project_id, authorization):
		self._base_url = '%s/%s' % (self._base_url, project_id)
		self._base_headers['Authorization'] = authorization

	def change_authorization(self, authorization):
		self._base_headers['Authorization'] = authorization

	def add_filter(self, property_name, operator, property_value):

		if isinstance(property_value, bool):
			# little hack to dumps don't convert without quote
			property_value = "true" if property_value else "false"

		self._filters.append({"property_name": property_name,
								"operator": operator,
								"property_value": property_value})

	def remove_filter(self, property_name, operator):
		self._filters = [x for x in self._filters if x['property_name'] != property_name and x['operator'] != operator]

	def remove_all_filters(self):
		self._filters = []

	def timeframe(self, timeframe):
		self._timeframe = timeframe

	def count_unique(self, event_collection, group_by, target_property):

		data = {"event_collection": event_collection,
				"group_by": group_by,
				"target_property": target_property
				}

		return self.api_query('count_unique', data=data)

	def count(self, event_collection, group_by):

		data = {"event_collection": event_collection,
				"group_by": group_by
				}

		return self.api_query('count', data=data)

	def select_unique(self, event_collection, target_property):

		data = {"event_collection": event_collection,
				"target_property": target_property
				}

		return self.api_query('select_unique', data=data)


	def api_query(self, method, data):

		body = {"timeframe": self._timeframe,
				"filters": self._filters
				}

		body.update(data)

		url = '%s/queries/%s' % (self._base_url, method)
		
		r = requests.post(url, data=json.dumps(body), headers=self._base_headers)
		
		return r.json()['result']

	def add_event(event_collection, body):
		url = '%s/events/%s' % (self._base_url, event_collection)

		r = requests.post(url, data=json.dumps(body), headers=self._base_headers)

		if r.status_code != 200:
			logging.error('Troz.io error [%i]!' % r.status_code)
			logging.error(url)
			logging.error(r.content)
			logging.error(body)



