from __future__ import print_function
import vertica_python
import re
import os
from config import *
from texttable import Texttable

def pretty_print_results(cur, rv):
	"""
	        Pretty print returned SQL   
	"""
	t = Texttable()
	colnames = [col[0] for col in cur.description]

	# empty row will store the column names
	rows = [[]] + rv
	t.add_rows(rows)
	t.header(colnames)
	print(t.draw())

def make_dicts(cursor, row):
	"""
		Turn query results into dictionaries keyed by column name
	"""
	colnames = [col[0] for col in cursor.description]

	fmtrow = {}
	for idx, value in enumerate(row):
		fmtrow[colnames[idx]] = value

	return fmtrow

def connect_to_db():
	"""
		Connect to vertica instance. 
		Use Environment variables as much as possible.
	"""
	conn_info = {'host': DB_HOST,
                 'port': 5433,
                 'user': DB_USER,
                 'password': DB_PASSWORD,
                 'database': DB_NAME,
                 # 10 minutes timeout on queries
                 'read_timeout': 600,
                 # default throw error on invalid UTF-8 results
                 'unicode_error': 'strict',
                 # SSL is disabled by default
                 'ssl': False}

	db = vertica_python.connect(**conn_info)
	
	if db:
		db.cursor().execute('set search_path to ' + DB_SCHEMA + ', "$user", public;')
	return db

def query_db(query, args=(), one=False, db = None, pretty_print=False):
	"""
		Set load to try if the query command wants to load data in vertica
	"""
	e = ""
	print("Query string: " + query)
	if not db:
		db = connect_to_db()
	cur = db.cursor()

	try:
		cur.execute(query, args)
		rv = cur.fetchall()
		res = rv

		if rv and pretty_print:
			pretty_print_results(cur, rv)

		# Turn into colname->val dict representation of tuple
		# this isn't very efficient but will suffice for now
		rv = [make_dicts(cur, row) for row in rv]
	except Exception as e:
		print(e)
		rv = [{'error': e}]

	cur.close()
	#return (rv[0] if rv else None) if one else rv
	return res
'''
def load_db(query, path, db = None):
	"""
		Load data in db
		Example: copy foo from '/home/spantela/vertica-hackathon/server/data.csv' delimiter ',' direct REJECTED DATA AS TABLE foo_rejected;`
	"""
	e = ''
	print("Query string: " + query)
	if not db:
		db = connect_to_db()
	cur = db.cursor()

	try:
		#with open(path, "rb") as fs:
    		#	cur.copy(query, fs, buffer_size=65536)
		gf = open(path, "r")
		cur.stdin = gf
		#cur.copy(query, gf)
		cur.execute(query,gf)		
		rv = [{'success': 1}]
	except Exception as e:
		print(e)
		rv = [{'error': e}]

	cur.close()
	return rv
'''
def load_db(query, path, db=None):
	print("Query string: " + query)
	if not db:
		db = connect_to_db()
	cur = db.cursor()

	try:
		with open(path, 'rb') as gf:
			cur.copy(query, gf)		
		rv = [{'success': 1}]
	except Exception as e:
		print(e)
		rv = [{'error': e}]

	cur.close()
	return rv

		 

