import boto
import time
import json

kinesis = boto.connect_kinesis()

shard_id = 'shardId-000000000000'
stream = 'dev-tweets'
counter = 0

# Write some data to the stream

# Wait for the data to show up
while True:
    time.sleep(1)
    counter += 1
    data1 = json.loads('{"id":"1234","{0}":"testvalue"}').format(counter)
    shard_iterator_json = kinesis.get_shard_iterator(stream, shard_id, 'LATEST')
    # shard_iterator = shard_iterator_json['ShardIterator']
    response = kinesis.put_record(stream, json.dumps(data1), data1["id"])
    print "Kinesis response: {0}".format(response)

    print "ShardIterator: {0}".format(shard_iterator)
    response = kinesis.get_records(shard_iterator)
    shard_iterator = response['NextShardIterator']

    if len(response['Records']):
        print "len(response['Records'])): {0}".format(len(response['Records']))
        print "response['Records'][0]['Data']: {0})".format(response['Records'][0]['Data'])

