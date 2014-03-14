# based on http://badhessian.org/2012/10/collecting-real-time-twitter-data-with-the-streaming-api/
# with modifications by http://github.com/marciw
# requires Tweepy https://github.com/tweepy/tweepy

from tweepy import StreamListener
import json, time, sys, os, datetime, subprocess
from os import rename, remove
import twitterparams, logging
import boto
from boto.s3.key import Key
import boto.ec2.cloudwatch
from boto.dynamodb2.table import Table

class TWaiter(StreamListener):
    # see Tweepy for more info

    s3Conn = None
    dynamoTable = None
    env = None
    DEBUG = False
    # KINESIS = 

    def get_filename(self):
        folder = 'tweets/{0}'.format(time.strftime("%Y%m%d"))
        if not os.path.exists(folder):
            os.makedirs(folder)
        filename = ('{0}/tweet.{1}.txt').format(folder, 
            datetime.datetime.now().strftime("%Y%m%d-%H:%M:%S.%f"))
        return filename

    def __init__(self, api = None, label = 'default_collection'):
        self.logger = logging.getLogger('RotatingLogger')
        self.api = api or API()
        self.counter = 0
        self.interval = twitterparams.CW_INTERVAL
        self.counter_max_size = twitterparams.COUNTER_MAX_SIZE
        self.CW_NAMESPACE = twitterparams.CW_NAMESPACE
        region = twitterparams.REGION
        TWaiter.env = twitterparams.ENV
        self.tweet_read_starttime = datetime.datetime.now()
        try:
            current_dir = os.path.dirname(os.path.realpath(__file__))
            os.chdir(current_dir)
            self.output  = open(self.get_filename(), 'w')
            TWaiter.s3Conn = boto.connect_s3()
            TWaiter.dynamoTable = Table(twitterparams.CONFIG_TABLENAME)
            item=TWaiter.dynamoTable.get_item(id=TWaiter.env+'#total_tweets')
            self.number_of_tweeets = int(item.get('value'))
            self.bucket = TWaiter.s3Conn.get_bucket(twitterparams.BUCKET_NAME)
            TWaiter.cwConn = boto.ec2.cloudwatch.connect_to_region(region)
            self.tweet_interval_start_count = self.number_of_tweeets
        except Exception, e:
            self.logger.error("Problem opening output file and connection to s3. Excpetion: {0}".format(e))

    def notify_cloudwatch(self,total_seconds=1):
        count_per_second = (self.number_of_tweeets - self.tweet_interval_start_count) / total_seconds
        try:
            self.logger.info("self.number_of_tweeets: {0}, self.tweet_interval_start_count: {1}, total_seconds: {2}, count_per_second: {3}".format(self.number_of_tweeets, self.tweet_interval_start_count, total_seconds, count_per_second))
            self.cwConn.put_metric_data(namespace=self.CW_NAMESPACE,name="tweetsPerSecond",value=count_per_second
                , timestamp=datetime.datetime.now(), unit="Count/Second")
            self.cwConn.put_metric_data(namespace=self.CW_NAMESPACE,name="tweetsTotal",value=self.number_of_tweeets
                , timestamp=datetime.datetime.now(), unit="Count")
            self.tweet_read_starttime=datetime.datetime.now()
            self.tweet_interval_start_count = self.number_of_tweeets
        except Exception, e:
            self.logger.error("notify_cloudwatch. Exception {0}".format(e))

    def update_dynamoTable(self):
        try:
            item = TWaiter.dynamoTable.get_item(id=TWaiter.env+'#total_tweets')
            old_value = int(item['value'])
            item['value']=int(self.number_of_tweeets)
            item.save()
            self.logger.info("update_dynamoTable - old value: {0}, new value: {1}".format(old_value, item['value']))
        except Exception, e:
            self.logger.error("update_dynamoTable - error: {0}".format(e))

    def file_to_s3(self):
        # TODO: should go into subprocess
        # 1. create new file (leave old file open for writes)
        # 2. switch to new file
        if TWaiter.DEBUG:
            print "file_to_s3 - debug"
            self.counter = 0
        else:
            try:
                self.counter = 0
                old_temp_file = self.output
                self.output.close()
                self.output  = open(self.get_filename(), 'w')
                subprocess.call(["bzip2", old_temp_file.name])
                bz2File = old_temp_file.name+'.bz2'
                k = Key(self.bucket)
                k.key = bz2File
                k.set_contents_from_filename(bz2File)
                self.logger.info('{0} copied to S3 bucket'.format(bz2File))
                remove(bz2File)
            except Exception, e:
                self.logger.error("file_to_s3. Exception {0}".format(e))

    def on_data(self, data):
        # The presence of 'in_reply_to_status' indicates a "normal" tweet.
        # The presence of 'delete' indicates a tweet that was deleted after posting.
        if  'in_reply_to_status' in data:
            self.on_status(data)
        elif 'delete' in data:
            delete = json.loads(data)['delete']['status']
            if self.on_delete(delete['id'], delete['user_id']) is False:
                return False


    def on_status(self, status):
        # Get only the text of the tweet and its ID.
        self.output.write(status)
        # print (status)

        self.counter += 1
        self.number_of_tweeets += 1

        total_seconds = (datetime.datetime.now() - self.tweet_read_starttime).total_seconds()

        if (total_seconds > self.interval):
            self.notify_cloudwatch(total_seconds)
            self.update_dynamoTable()

        if self.counter >= self.counter_max_size:
            self.file_to_s3()

        # When has geo data, send json to Kinesis
        # data = json.loads(status)
        # if data["geo"]:
            

        return

    def on_delete(self, status_id, user_id):
        # self.deleted.write(str(status_id) + "\n")
        return

    def on_error(self, status_code):
        self.logger.error('Error: ' + str(status_code) + "\n")
        return False
    
    def close(self):
        self.logger.info("Twaiter - close")
        total_seconds = (datetime.datetime.now() - self.tweet_read_starttime).total_seconds()
        self.notify_cloudwatch(total_seconds)
        self.file_to_s3()
        self.output.close()
        TWaiter.s3Conn.close()
        TWaiter.cwConn.close()
