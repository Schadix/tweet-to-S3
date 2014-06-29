import boto
import time
import json
import datetime
from random import randint

kinesis = boto.connect_kinesis()

shard_id = 'shardId-000000000000'
stream = 'dev-tweets'
counter = 0


# write data to the stream
while True:
	time.sleep(1)
	counter += 1
	# data1 = json.loads('{{"id":"{0}","testname":"{1}"}}'.format(randint(100000000,9999999999), counter))
	try:
		response = kinesis.put_record(stream, str(counter), str(counter))
		print "Put: {0}".format(counter)
		# print "Kinesis response: {0}".format(response)
	except Exception,e:
		print "failed because of: {0}".format(e)
