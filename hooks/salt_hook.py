# -*- coding: utf-8 -*-

from builtins import str
import logging
import pprint
import json
import requests

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException


class SaltHook(BaseHook):
	"""
	Interact with Salt servers
	"""

	def __init__(self, salt_conn_id='salt_default'):
		self.salt_conn_id = salt_conn_id
		self.authToken = None
	
	def getAuthedConnection(self):
		"""
		Obtains an authenticated connection
		"""
		conn = self.get_connection(self.salt_conn_id)
		session = requests.Session()
		port = 8000
		
		if conn.port:
			port = conn.port
		
		self.baseUrl = 'https://' + conn.host + ':' + str(port) + '/'
		
		session.headers.update({ 'Content-Type': 'application/json; charset=UTF-8' })
		
		if not self.authToken:
			self.getAuthToken(session, conn.login, conn.password)
		
		session.headers.update({ 'X-Auth-Token': self.authToken })
		return session;
	
	def getAuthToken( self, session, username, password ):
		"""
		Gets auth token from the Salt API
		"""
		self.authToken = None
		
		url = self.baseUrl + 'login'
		data = { 'username': username, 'password': password, 'eauth': 'pam' }
		
		request = requests.Request('POST', url)
		prepped_request = session.prepare_request(request)
		prepped_request.body = json.dumps(data)
		prepped_request.headers.update({ 'Content-Length': len(prepped_request.body) });
		
		response = session.send(prepped_request, stream=False, verify=False, allow_redirects=True)
		resp = response.json()
		
		if 'token' in resp.get('return', [{}])[0]:
			self.authToken = resp['return'][0]['token']
		else:
			raise AirflowException( 'Could not authenticate properly: ' + str(response.status_code) + ' ' + response.reason )
		
		try:
			response.raise_for_status()
		except requests.exceptions.HTTPError:
			raise AirflowException( 'Could not authenticate properly: ' + str(response.status_code) + ' ' + response.reason )
		return self.authToken
	
	def run( self, client='local', tgt=None, fun=None, fun_args=None ):
		"""
		Calls the API
		"""
		session = self.getAuthedConnection()
		url = self.baseUrl
		data = { 'client': client, 'tgt': tgt, 'fun': fun, 'args': fun_args }
		
		try:
			request = requests.Request('POST', url)
			prepped_request = session.prepare_request(request)
			prepped_request.body = json.dumps(data)
			prepped_request.headers.update({ 'Content-Length': len(prepped_request.body) });
			
			response = session.send(prepped_request, stream=False, verify=False, allow_redirects=True)
			response.raise_for_status()
		except requests.exceptions.HTTPError:
			logging.error( 'HTTP error: ' + response.reason )
		
		logging.info( 'DEBUG: ' + pprint.pformat( response.__dict__ ) )
		
		return response



