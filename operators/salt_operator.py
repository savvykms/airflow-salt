from airflow.exceptions import AirflowException
from airflow.hooks import SaltHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import logging
import pprint
import json

class SaltOperator(BaseOperator):
	"""
	Calls the Saltstack API for a given connection
	
	:param salt_conn_id: The connection to run the sensor against
	:type salt_conn_id: string
	:param client: The salt API client
	:type client: string
	:param tgt: The target for the API function
	:type tgt: string
	:param fun: The salt API function
	:type fun: string
	:param fun_args: The salt API function args
	:type fun_args: list
	"""
	
	ui_color = '#f4a578'
	
	@apply_defaults
	def __init__(self, salt_conn_id='salt_default', client=None, tgt=None, fun=None, fun_args=None, *args, **kwargs):
		
		super(SaltOperator, self).__init__(*args, **kwargs)
		
		self.salt_conn_id = salt_conn_id
		self.client = client or 'local'
		self.tgt = tgt or None
		self.fun = fun or 'test.ping'
		self.fun_args = fun_args or []
		
	def execute(self, context):
		logging.info("Initializing connection")
		salt = SaltHook(salt_conn_id=self.salt_conn_id)
		logging.info("Calling API")		
		response = salt.run(client = self.client, tgt = self.tgt, fun = self.fun, fun_args = self.fun_args)
