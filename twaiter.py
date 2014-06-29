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

    s3Conn = None
    dynamoTable = None
    env = None
    DEBUG = False

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
        self.tweet_interval_start_count = 0
        self.number_of_tweets=0
        try:
            current_dir = os.path.dirname(os.path.realpath(__file__))
            os.chdir(current_dir)
            if not os.path.exists(twitterparams.TWEETS_COLLECTED_FOLDER):
                os.makedirs(twitterparams.TWEETS_COLLECTED_FOLDER)
            self.output  = open(self.get_filename(), 'w')
            TWaiter.cwConn = boto.ec2.cloudwatch.connect_to_region(region)
        except Exception, e:
            self.logger.error("Problem opening output file and connection to s3. Excpetion: {0}".format(e))

    def notify_cloudwatch(self,total_seconds=1):
        count_per_second = (self.number_of_tweets - self.tweet_interval_start_count) / total_seconds
        try:
            self.logger.info("self.number_of_tweets: {0}, self.tweet_interval_start_count: {1}, total_seconds: {2}, count_per_second: {3}".format(self.number_of_tweets, self.tweet_interval_start_count, total_seconds, count_per_second))
            self.cwConn.put_metric_data(namespace=self.CW_NAMESPACE,name="tweetsPerSecond",value=count_per_second
                , timestamp=datetime.datetime.now(), unit="Count/Second")
            self.tweet_read_starttime=datetime.datetime.now()
            self.tweet_interval_start_count = self.number_of_tweets
        except Exception, e:
            self.logger.error("notify_cloudwatch. Exception {0}".format(e))

    def file_collected(self):
        if TWaiter.DEBUG:
            print "file_collected - debug"
            self.counter = 0
        else:
            try:
                self.counter = 0
                old_temp_file = self.output
                self.output.close()
                self.output  = open(self.get_filename(), 'w')
                self.logger.info("old_temp_file.name: {0}".format(old_temp_file.name))
                path, name = os.path.split(os.path.abspath(old_temp_file.name))
                subprocess.call(["mv", old_temp_file.name, twitterparams.TWEETS_COLLECTED_FOLDER+"/"+name])
            except Exception, e:
                self.logger.error("file_collected. Exception {0}".format(e))

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
        # write to file
        self.output.write(status)

        self.counter += 1
        self.number_of_tweets += 1

        total_seconds = (datetime.datetime.now() - self.tweet_read_starttime).total_seconds()

        if (total_seconds > self.interval):
            self.notify_cloudwatch(total_seconds)

        if self.counter >= self.counter_max_size:
            self.file_collected()

        return

    def on_delete(self, status_id, user_id):
        # self.deleted.write(str(status_id) + "\n")
        return

    def on_error(self, status_code):
        self.logger.error('on_error. Error: ' + str(status_code) + "\n")
        # TODO: just call close, zdaemon will restart. Or throw an exception
        # Better would be proper error handling and reconnecting actually
        raise Exception('on_error. Error: ' + str(status_code) + "\n")
        return 
    
    def close(self):
        self.logger.info("Twaiter - close")
        total_seconds = (datetime.datetime.now() - self.tweet_read_starttime).total_seconds()
        self.notify_cloudwatch(total_seconds)
        self.file_collected()
        self.output.close()
        TWaiter.cwConn.close()
