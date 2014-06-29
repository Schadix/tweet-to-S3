import boto
import time
import json

kinesis = boto.connect_kinesis()

shard_id = 'shardId-000000000000'
stream = 'dev-tweets'
data1 = json.loads('{"id":"1234","testname":"testvalue"}')

shard_iterator_json = kinesis.get_shard_iterator(stream, shard_id, 'LATEST')
shard_iterator = shard_iterator_json['ShardIterator']

# Wait for the data to show up
while True:
	time.sleep(1)
	# print "ShardIterator: {0}".format(shard_iterator)
	response = kinesis.get_records(shard_iterator)
	shard_iterator = response['NextShardIterator']

	if len(response['Records']):
		for i,v in enumerate(response['Records']):
			# print "{0})".format(v[i]['Data'])
			print "{0}, {1}".format(i, v["Data"])

