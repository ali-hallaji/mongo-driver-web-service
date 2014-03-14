from pymongo import MongoClient, ASCENDING, Connection, DESCENDING
from datetime import datetime, timedelta
from twisted.web import xmlrpc, server
from twisted.python import log, failure
from twisted.internet import pollreactor
from twisted.internet.defer import Deferred
from twisted.internet.task import deferLater


import csv
import time
import os
log.startLogging(open('mongolog', 'w'), setStdout=False)

class MongoTest(xmlrpc.XMLRPC):
	
	"""
		Note: * is meaning that pass two args to def for name_db and name_col
		for connecting:
		>>>from xmlrpclib import ServerProxy
		>>>s = ServerProxy("http://localhost:7081")
		>>>s.count(val) # pass arg by dict for finding and matching		*
		>>>s.remove(val) # pass arg by dict for finding and matching  	*
		>>>s.fetchOne(val) # pass arg by dict for finding and matching		*
		>>>s.DropDB(name) # Drop your database. name=name of db 	*
		>>>s.ensureIndex(name, num) # set ensureIndex, name=key & num=(ASCENDING(1) or DESCENDING(-1))	*
		>>>s.fetchAll(val) # pass arg by dict for finding 	*
		>>>s.update({},{}) # pass two dict of args. dict1 = find keys, dict2 = set update keys 	*
		>>>s.ImportByMongo() # import data csv or json file by mongo import command
		>>>s.InsertToMongo() # Single insert
		>>>s.BulkToMongo(number) # for bulking insert, number is chunk of data 	*
		>>>s.indexBulk(number) # for bulking insert with indexing, number is chunk of data 	*
		>>>s.addRecord(val) # pass the your dict 	*
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
		reactor.suggestThreadPoolSize(30)


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
		print ">>> Call DropDB"
		client = MongoClient()
		c = Connection()
		c.drop_database(str(name))
		all_db = client.database_names()
		if str(name) in all_db:
			return "Faild to Drop your db"
		log.msg("Droped your DB")
		return "Droped your DB"
		# c['mydatabase'].drop_collection('mycollection') ; for collection

	def xmlrpc_BulkToMongo(self, name_db, name_col, number):
		"""
			added records by bulking insert
		"""
		self.start = time.time()
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
					now = time.time() - self.start
					now_time = self.GetTime(now)
					print "\r%s records added by Bulking in my db after %s time\n " % (i,now_time)
		now = time.time() - self.start
		now_time = self.GetTime(now)
		print "\r%s records added by Bulking in my db after %s time\n " % (i,now_time)
		return "bulking %s records in %s time" % (i, now_time)

	def xmlrpc_getDict(self):
		"""
			Imprest Dict
		"""
		return self.Dict

	def xmlrpc_InsertToMongo(self, name_db, name_col, number):
		"""
			added records by single inserting
		"""
		self.start = time.time()
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
					now = time.time() - self.start
					now_time = self.GetTime(now)
					print "\r%s records added by inserting in my db after %s time\n " % (i,now_time)

	def xmlrpc_ImportByMongo(self, type):
		"""
			Import by mongo command
		"""
		self.start = time.time()
		if type == "csv":
			os.system("mongoimport --host localhost --db ibs --collection ibs_col --type csv --file %s --headerline" % self.dir)
		elif type == "json":
			os.system("mongoimport --host localhost --db ibs --collection ibs_col --type json --file %s --headerline" % self.dir_json)
		now = time.time() - self.start
		return "all Records add after %s time" % self.GetTime(now)

	def xmlrpc_fetchOne(self, name_db, name_col, val):
		"""
			single fetch from db
			val: {find keys}
		"""
		print ">>> Call fetchOne"
		self.start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		seek = db.db_col.find_one(val)
		now = time.time() - self.start
		return str(seek), self.GetTime(now)

	def xmlrpc_fetchAll(self, name_db, name_col, val):
		"""
			fetch all records when match keys
			val: {find keys}
		"""
		print ">>> Call fetchAll"
		self.start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		tp = ()
		i = 0
		for seek in db.db_col.find(val):
			i += 1
			tp += (str(seek), )
		now = time.time() - self.start
		return tp, i, self.GetTime(now)

	def xmlrpc_update(self, name_db, name_col, val, VAL):
		"""
			update records
			Pass two args:
			val: {find keys}
			VAL: {set to update keys}
		"""
		print ">>> Call update"
		self.start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		db.db_col.update(val, VAL)
		now = time.time() - self.start
		return self.GetTime(now)

	def xmlrpc_remove(self, name_db, name_col, val):
		"""
			delete records
		"""
		print ">>> Call remove"
		self.start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		db.db_col.remove(val)
		now = time.time() - self.start
		return self.GetTime(now)

	def xmlrpc_count(self, name_db, name_col, val):
		"""
			count records
		"""
		print ">>> Call count"
		self.start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]

		key = db.db_col.find(val).count()
		now = time.time() - self.start
		return key, self.GetTime(now)

	def xmlrpc_ensureIndex(self, name_db, name_col, name, num):
		print ">>> Call ensureIndex"
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		db.db_col.ensure_index([(str(name), int(num))])
		return "set index"

	def xmlrpc_indexBulk(self, name_db, name_col, number):
		"""
			Import Bulking in db by index
		"""
		self.start = time.time()
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
					now = time.time() - self.start
					now_time = self.GetTime(now)
					print "\r%s records added by Bulking in my db after %s time\n " % (i,now_time)
					
		now = time.time() - self.start
		now_time = self.GetTime(now)
		print "\r%s records added by Bulking in my db after %s time\n " % (i,now_time)
		return "bulking %s records in %s time" % (i, now_time)

	def xmlrpc_addRecord(self, name_db, name_col, val):
		"""
			add record
			val: pass dict of keys
		"""
		print ">>> Call ensureIndex"
		self.start = time.time()
		client = MongoClient()
		db = client[str(name_db)]
		db_col = db[str(name_col)]
		db.db_col.insert(val)	
		now = time.time() - self.start
		now_time = self.GetTime(now)
		return "added your record in %s time" % now_time


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
	#log.err(f, "sdsd")
	observer.start()
	reactor.run()
