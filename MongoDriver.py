from pymongo import MongoClient, ASCENDING, Connection, DESCENDING
from datetime import datetime, timedelta
from twisted.web import xmlrpc, server
from twisted.python import log, failure
from twisted.internet.threads import deferToThread

import csv
import time
import os
log.startLogging(open('mongolog', 'w'), setStdout=False)

class MongoTest(xmlrpc.XMLRPC):
	
	"""
		for client connecting:
		>>>from xmlrpclib import ServerProxy
		>>>s = ServerProxy("http://localhost:7081")
		>>>s.asyncCounte(name_db, name_col, val) # pass arg by dict for finding and matching
		>>>s.asyncRemove(name_db, name_col, val) # pass arg by dict for finding and matching 
		>>>s.asyncfetchOne(name_db, name_col, val) # pass arg by dict for finding and matching
		>>>s.DropDB(name) # Drop your database. name=name of db 
		>>>s.asyncEnsureIndex(name_db, name_col, key, num) # set ensureIndex, name=key & num=(ASCENDING(1) or DESCENDING(-1))
		>>>s.asyncfetchAll(name_db, name_col, val) # pass arg by dict for finding 
		>>>s.asyncUpdate(name_db, name_col, val, VAL) # pass two dict of args. dict1 = find keys, dict2 = set update keys
		>>>s.ImportByMongo(type) # import data csv or json file by mongo import command. type='csv' or 'json'
		>>>s.asyncInsert(name_db, name_col, number) # Single inserting
		>>>s.asyncBulk(name_db, name_col, number) # for bulking insert, number is chunk of data 	
		>>>s.asyncIndexBulking(name_db, name_col, number)
		>>>s.asyncAddRecord(name_db, name_col, val)
		>>>s.stop() # kill server
	"""
	log.msg("Start the module")
	allowNone = True
	useDateTime = True

	def __init__(self):
		xmlrpc.XMLRPC.__init__(self)
		self.dir = '/home/pythonu/Desktop/check.csv'
		self.dir_json = '/home/pythonu/Desktop/check.json'
		self.Dict = {}


	def xmlrpc_getInfo(self):
		return MongoTest.__doc__

	def GetTime(self, secs):
		"""
			this is convert function from secs to strftime format
			pass your secs to this func for converting to strftime("%H:%M:%S")
		"""
		c = ":"
		sec = timedelta(seconds=int(secs))
		d = datetime(1,1,1) + sec
		val = "%s:%s:%s" % (d.hour, d.minute, d.second)
		# This line for add zero and check standard time
		return c.join("{0:02d}".format(int(i)) for i in val.split(c))

	def xmlrpc_DropDB(self, name):
		"""
			Drop DB
			name: name of your db
		"""
		log.msg(">>> Call DropDB")
		start = time.time()
		client = MongoClient()
		all_db = client.database_names()
		for i in all_db:
			if str(name) == str(i):
				c = Connection()
				c.drop_database(str(name))
				now = time.time() - start
				now_time = self.GetTime(now)
				log.msg("Droped your DB at %s time" % now_time)
				return "Droped your DB at %s time" % now_time
		else:
			log.msg("Faild to Drop your db")
			return "Faild to Drop your db"

		# c['mydatabase'].drop_collection('mycollection') ; for collection

	def xmlrpc_BulkToMongo(self, name_db, name_col, number):
		"""
			added records by bulking insert
		"""
		start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		list_bulk = []

		with open(self.dir) as f:
			Dict = csv.DictReader(f)
			i = 0
			for doc in Dict:
				i += 1
				list_bulk.append(doc)
				if float(i % int(number)) == 0:
					db.db_col.insert(list_bulk)
					list_bulk = []
					now = time.time() - start
					now_time = self.GetTime(now)
					print "\r%s records added by Bulking in my db after %s time\n " % (i,now_time)
		now = time.time() - start
		now_time = self.GetTime(now)
		print "\r%s records added by Bulking in my db after %s time\n " % (i,now_time)
		return "bulking %s records at %s time" % (i, now_time)


	def xmlrpc_asyncBulk(self, name_db, name_col, number):
		"""
			this is for async call function
		"""
		log.msg(">>>call Bulking to Mongo")
		d = deferToThread(self.xmlrpc_BulkToMongo, name_db, name_col, number)
		return d

	def xmlrpc_getDict(self):
		"""
			Imprest Dict
		"""
		return self.Dict

	def xmlrpc_InsertToMongo(self, name_db, name_col, number):
		"""
			added records by single inserting
		"""
		start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		with open(self.dir) as f:
			Dict = csv.DictReader(f)
			i = 0
			for doc in Dict:
				i += 1
				db.db_col.insert(doc)
				if float(i % int(number)) == 0:
					now = time.time() - start
					now_time = self.GetTime(now)
					print "\r%s records added by inserting in my db after %s time\n " % (i,now_time)
		print "\r%s records added by inserting in my db after %s time\n " % (i,now_time)
		return "inserting %s records at %s time" % (i, now_time)

	def xmlrpc_asyncInsert(self, name_db, name_col, number):
		"""
			this is for async call function
		"""
		log.msg(">>>call Inserting to Mongo")
		d = deferToThread(self.xmlrpc_InsertToMongo, name_db, name_col, number)
		return d

	def xmlrpc_ImportByMongo(self, type):
		"""
			Import by mongo command
		"""
		start = time.time()
		if type == "csv":
			os.system("mongoimport --host localhost --db ibs --collection ibs_col --type csv --file %s --headerline" % self.dir)
		elif type == "json":
			os.system("mongoimport --host localhost --db ibs --collection ibs_col --type json --file %s --headerline" % self.dir_json)
		now = time.time() - start
		return "all Records add after %s time" % self.GetTime(now)

	def xmlrpc_fetchOne(self, name_db, name_col, val):
		"""
			single fetch from db
			val: {find keys}
		"""
		log.msg(">>> Call fetchOne")
		start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		seek = db.db_col.find_one(val)
		now = time.time() - start
		return str(seek), self.GetTime(now)

	def xmlrpc_asyncfetchOne(self, name_db, name_col, val):
		"""
			this is for async call function
		"""
		log.msg(">>>call fetch one from Mongo")
		d = deferToThread(self.xmlrpc_fetchOne, name_db, name_col, val)
		return d

	def xmlrpc_fetchAll(self, name_db, name_col, val):
		"""
			fetch all records when match keys
			val: {find keys}
		"""
		log.msg(">>> Call fetchAll")
		start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		tp = ()
		i = 0
		for seek in db.db_col.find(val):
			i += 1
			tp += (str(seek), )
		now = time.time() - start
		return tp, i, self.GetTime(now)

	def xmlrpc_asyncfetchAll(self, name_db, name_col, val):
		"""
			this is for async call function
		"""
		log.msg(">>>call fetch All from Mongo")
		d = deferToThread(self.xmlrpc_fetchAll, name_db, name_col, val)
		return d

	def xmlrpc_update(self, name_db, name_col, val, VAL):
		"""
			update records
			Pass two args:
			val: {find keys}
			VAL: {set to update keys}
		"""
		log.msg(">>> Call update")
		start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		db.db_col.update(val, VAL)
		now = time.time() - start
		return self.GetTime(now)

	def xmlrpc_asyncUpdate(self, name_db, name_col, val, VAL):
		"""
			this is for async call function
		"""
		log.msg(">>>call update record from Mongo")
		d = deferToThread(self.xmlrpc_update, name_db, name_col, val, VAL)
		return d

	def xmlrpc_remove(self, name_db, name_col, val):
		"""
			delete records
		"""
		log.msg(">>> Call remove")
		start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		db.db_col.remove(val)
		now = time.time() - start
		return self.GetTime(now)

	def xmlrpc_asyncRemove(self, name_db, name_col, val):
		"""
			this is for async call function
		"""
		log.msg(">>>call remove record from Mongo")
		d = deferToThread(self.xmlrpc_remove, name_db, name_col, val)
		return d

	def xmlrpc_count(self, name_db, name_col, val):
		"""
			count records
		"""
		log.msg(">>> Call count")
		start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]

		key = db.db_col.find(val).count()
		now = time.time() - start
		return key, self.GetTime(now)

	def xmlrpc_asyncCounte(self, name_db, name_col, val):
		"""
			this is for async call function
		"""
		log.msg(">>>call count records from Mongo")
		d = deferToThread(self.xmlrpc_count, name_db, name_col, val)
		return d


	def xmlrpc_ensureIndex(self, name_db, name_col, key, num):
		log.msg(">>> Call ensureIndex")
		start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		db.db_col.ensure_index([(str(key), int(num))])
		now = time.time() - start
		return "set indexed at %s time" % self.GetTime(now)

	def xmlrpc_asyncEnsureIndex(self, name_db, name_col, key, num):
		"""
			this is for async call function
		"""
		log.msg(">>>call ensureIndex to Mongo")
		d = deferToThread(self.xmlrpc_ensureIndex, name_db, name_col, key, num)
		return d


	def xmlrpc_indexBulk(self, name_db, name_col, number):
		"""
			Import Bulking in db by index
		"""
		start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		db.db_col.ensure_index([('username', ASCENDING)])
		db.db_col.ensure_index([('connection_log_id', DESCENDING)])
		db.db_col.ensure_index([('login_time', DESCENDING)])
		db.db_col.ensure_index([('logout_time', ASCENDING)])
		list_bulk = []

		with open(self.dir) as f:
			Dict = csv.DictReader(f)
			i = 0
			for doc in Dict:
				i += 1
				list_bulk.append(doc)
				if float(i % int(number)) == 0:
					db.db_col.insert(list_bulk)
					list_bulk = []
					now = time.time() - start
					now_time = self.GetTime(now)
					print "\r%s records added by Bulking in my db after %s time\n " % (i,now_time)
					
		now = time.time() - start
		now_time = self.GetTime(now)
		print "\r%s records added by Bulking in my db after %s time\n " % (i,now_time)
		return "bulking %s records at %s time" % (i, now_time)

	def xmlrpc_asyncIndexBulking(self, name_db, name_col, number):
		"""
			this is for async call function
		"""
		log.msg(">>>call indexBulk to Mongo")
		d = deferToThread(self.xmlrpc_indexBulk, name_db, name_col, number)
		return d

	def xmlrpc_addRecord(self, name_db, name_col, val):
		"""
			add record
			val: pass dict of keys
		"""
		log.msg(">>> Call AddRecord")
		start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		db.db_col.insert(val)	
		now = time.time() - start
		now_time = self.GetTime(now)
		return "added your record at %s time" % now_time

	def xmlrpc_asyncAddRecord(self, name_db, name_col, val):
		"""
			this is for async call function
		"""
		log.msg(">>>call indexBulk to Mongo")
		d = deferToThread(self.xmlrpc_addRecord, name_db, name_col, number)
		return d

	def xmlrpc_stop(self):
		"""
			stop reactor
		"""
		reactor.stop()

	def xmlrpc_fault(self):
		f = failure.Failure()
		log.err(f, "error")
		raise xmlrpc.Fault(123, "Kharabi!")



if __name__ == "__main__":
	from twisted.internet import reactor
	r = MongoTest()
	reactor.listenTCP(7081, server.Site(r))
	observer = log.PythonLoggingObserver()
	observer.start()
	reactor.run()
